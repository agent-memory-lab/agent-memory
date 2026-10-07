"""Published history, with current permissions and physical-erasure guards.

A complete published census certifies a point and, with atomic write hooks, an
unchanged interval. No gap interpolation or migration backfill is used.
"""

from copy import deepcopy
from datetime import UTC, datetime

from ..domain import canonical_json
from .contracts import HistoricalQuery
from .coverage import PublishedCoverage, semantic_unit
from .model import (
    HISTORY_INTERVAL,
    HISTORY_MODES,
    HISTORY_POINT,
    DerivedError,
    FacetContext,
    digest,
    identity,
    source_ids,
)
from .observation import compose

MODE = HISTORY_POINT


def iso(value):
    return value.astimezone(UTC).isoformat()


class PublishedHistory:
    def __init__(self, service):
        self.service, self.scope = service, service.scope
        self.coverage = PublishedCoverage(service)

    def point_id(self, epoch, facet_id, known_at):
        return "history-point:" + digest(
            [self.scope.partition_key(), epoch, facet_id, iso(known_at)]
        )

    async def archive(self, uow, snapshot, *, definition, known_at):
        if definition["spec"].get("history_mode") not in HISTORY_MODES:
            return None
        binding = self.service._context_binding(definition["spec"])
        if binding is not None:
            binding.current(known_at)
        query = await uow.derived_get(self.scope, "query", definition["spec"]["query_id"])
        archive = dict(
            schema="derived-history-snapshot/1",
            known_at=iso(known_at),
            definition=deepcopy(definition["spec"]),
            definition_generation=definition["generation"],
            policy=deepcopy(self.service.policy),
            query=deepcopy(query),
            records=deepcopy(snapshot["records"]),
        )
        if len(canonical_json(archive).encode()) > 262144:
            raise DerivedError("derived_history_capacity")
        return archive

    async def persist(self, uow, task, snapshot, archive, revision_id):
        if archive is None:
            return
        unit, spec = task.payload["unit"], archive["definition"]
        key = self.point_id(
            unit["epoch"], unit["facet_id"], datetime.fromisoformat(archive["known_at"])
        )
        old = await uow.derived_get(self.scope, "history_point", key)
        if old:
            if old.get("state") != "published" or old["history_sha256"] != digest(archive):
                raise DerivedError("derived_history_point_conflict")
            await self.coverage.publish(uow, old)
            return  # Identical semantic checkpoint; keep the first immutable certificate.
        points = await uow.derived_records(self.scope, "history_point")
        if (
            len(points) >= 4096
            or sum(p["payload"]["facet_id"] == unit["facet_id"] for p in points) >= 128
        ):
            raise DerivedError("derived_history_capacity")
        point = dict(
            id=key,
            facet_id=unit["facet_id"],
            epoch=unit["epoch"],
            state="published",
            known_at=archive["known_at"],
            revision_id=revision_id,
            unit_id=task.id,
            unit=unit,
            slots=archive["query"]["slots"],
            history_sha256=digest(archive),
            readers=spec["readers"],
            purpose=spec["purpose"],
            authority_id=spec["authority_id"],
            sources=sorted(snapshot["manifest"]["sources"]),
        )
        if spec.get("context") is not None:
            point["context"] = FacetContext.from_payload(spec["context"]).history_certificate()
        if self.service.history_mode == HISTORY_INTERVAL:
            point["input_versions"] = {}
            for source_id, source in snapshot["sources"].items():
                retained = source.metadata.get("_retention")
                document_id = (
                    retained.get("document_id", source_id) if isinstance(retained, dict) else None
                )
                point["input_versions"][source_id] = dict(
                    interpretation=digest(
                        await uow.retention_head_get(self.scope, "interpretation", source_id)
                    ),
                    document_id=document_id,
                    document=digest(
                        await uow.retention_head_get(self.scope, "document", document_id)
                        if document_id
                        else None
                    ),
                )
        point["sha256"] = digest(point)
        await uow.derived_put(self.scope, "history_point", key, point)
        await self.coverage.publish(uow, point)

    async def _guard(self, uow, point, definition, actor, purpose):
        if not point or point.get("state") != "published":
            raise DerivedError("derived_history_coverage_unavailable")
        if point["epoch"] != await uow.retention_epoch(self.scope):
            raise DerivedError("derived_history_coverage_unavailable")
        unsigned = {k: v for k, v in point.items() if k != "sha256"}
        if digest(unsigned) != point["sha256"]:
            raise DerivedError("derived_history_integrity_failed")
        known_at = HistoricalQuery.parse(point["known_at"], point["known_at"]).known_at
        if (
            point["id"] != self.point_id(point["epoch"], point["facet_id"], known_at)
            or known_at > self.service.clock()
            or (point.get("context") and not (
                point["context"]["known_from"] <= point["known_at"]
                < point["context"]["known_to"]
            ))
        ):
            raise DerivedError("derived_history_integrity_failed")
        if actor not in definition["spec"]["readers"] or purpose != definition["spec"]["purpose"]:
            raise DerivedError("derived_read_denied")
        if actor not in point["readers"] or purpose != point["purpose"]:
            raise DerivedError("derived_read_denied")
        _, authority = await self.service.registry.bindings(uow, definition["spec"])
        if point["authority_id"] != self.service.authority_id:
            raise DerivedError("derived_authority_mismatch")
        grants = {}
        for key in point["sources"]:
            grant = await uow.derived_get(self.scope, "grant", key)
            self.service._permission(grant, (actor,), purpose, self.service.clock(), authority)
            grants[key] = grant
        from ..operations.facet_refresh import valid_completion

        job = await uow.derived_get(self.scope, "job", point["unit_id"])
        if not job or job.get("unit") != point["unit"] or not valid_completion(self.scope, job):
            raise DerivedError("derived_history_integrity_failed")
        if not job["no_outputs"] and job["revision_id"] != point["revision_id"]:
            raise DerivedError("derived_history_integrity_failed")
        return grants

    async def _definition(self, uow, facet_id):
        if self.service.history_mode not in HISTORY_MODES:
            raise DerivedError("derived_history_unsupported")
        definition = await self.service._definition(uow, identity(facet_id))
        if definition["spec"].get("history_mode") not in HISTORY_MODES:
            raise DerivedError("derived_history_unsupported")
        return definition

    async def points(self, uow, facet_id, actor, purpose):
        definition = await self._definition(uow, facet_id)
        points = []
        for item in await uow.derived_records(self.scope, "history_point"):
            point = item["payload"]
            if point["facet_id"] != facet_id or point.get("state") != "published":
                continue
            await self._guard(uow, point, definition, actor, purpose)
            points.append(dict(known_at=point["known_at"], revision_id=point["revision_id"]))
        if self.service.history_mode == HISTORY_INTERVAL:
            spans = await self.coverage.describe(uow, facet_id)
            for point in points:
                key = self.point_id(
                    definition["epoch"], facet_id, datetime.fromisoformat(point["known_at"])
                )
                if key in spans:
                    point["coverage"] = spans[key]
        return dict(
            schema="derived-history-points/1",
            mode=self.service.history_mode,
            facet_id=facet_id,
            points=sorted(points, key=lambda p: p["known_at"]),
        )

    async def read(self, uow, facet_id, actor, purpose, query: HistoricalQuery):
        definition = await self._definition(uow, facet_id)
        if query.known_at > self.service.clock() or query.valid_at > self.service.clock():
            raise DerivedError("derived_history_future")
        epoch = await uow.retention_epoch(self.scope)
        key = self.point_id(epoch, facet_id, query.known_at)
        point = await uow.derived_get(self.scope, "history_point", key)
        span = None
        coverage = dict(known_at=iso(query.known_at), kind="point", query_complete=True)
        if point is None and self.service.history_mode == HISTORY_INTERVAL:
            span, coverage = await self.coverage.select(uow, facet_id, definition, query.known_at)
            point = await uow.derived_get(self.scope, "history_point", span["point_id"])
        grants = await self._guard(uow, point, definition, actor, purpose)
        if point.get("context") and not (
            point["context"]["known_from"] <= iso(query.known_at)
            < point["context"]["known_to"]
        ):
            raise DerivedError("derived_history_coverage_unavailable")
        if span is not None and (
            point["sha256"] != span["point_sha256"]
            or point["known_at"] != span["known_from"]
            or semantic_unit(point["unit"]) != span["semantic_unit"]
            or digest(point.get("input_versions")) != span["inputs_sha256"]
        ):
            raise DerivedError("derived_history_integrity_failed")
        if span is not None:
            await self.coverage.verify_inputs(uow, span, point)
        # Only now may the archive containing old L1 bodies be loaded.
        revision = await uow.derived_get(self.scope, "revision", point["revision_id"])
        if not revision or revision.get("state") not in {"ready", "empty"}:
            raise DerivedError("derived_history_coverage_unavailable")
        archive, manifest = revision.get("history"), revision.get("manifest")
        if (
            not archive
            or digest(archive) != point["history_sha256"]
            or digest(archive) != revision.get("history_sha256")
            or digest(manifest) != revision.get("manifest_sha256")
            or revision.get("unit") != point["unit"]
            or revision.get("id") != point["revision_id"]
            or manifest["unit"] != point["unit"]
            or archive["known_at"] != point["known_at"]
            or archive["schema"] != "derived-history-snapshot/1"
            or not manifest["query_complete"]
            or manifest["policy_sha256"] != digest(archive["policy"])
            or archive["definition"]["history_mode"] not in HISTORY_MODES
            or archive["definition"]["template_version"]
            not in {"locale-snapshot/1", "locale-context/1"}
            or archive["definition"]["template_version"] != manifest["template"]
            or (
                revision["state"] == "ready"
                and digest(revision.get("body")) != revision.get("body_sha256")
            )
        ):
            raise DerivedError("derived_history_integrity_failed")
        spec, records = archive["definition"], archive["records"]
        if spec["template_version"] == "locale-context/1":
            binding = FacetContext.from_payload(spec["context"])
            if (
                binding.query.scope != self.scope
                or binding.query.subject_id != spec["subject_id"]
                or binding.query.purpose != purpose
                or binding.history_certificate() != point.get("context")
            ):
                raise DerivedError("derived_history_integrity_failed")
            binding.historical(query.known_at, query.valid_at)
        elif spec.get("context") is not None or point.get("context") is not None:
            raise DerivedError("derived_history_integrity_failed")
        if (
            spec["id"] != facet_id
            or spec["readers"] != point["readers"]
            or spec["purpose"] != purpose
            or spec["authority_id"] != point["authority_id"]
            or archive["definition_generation"] != point["unit"]["definition_generation"]
            or digest(dict(definition=spec, policy=archive["policy"]))
            != point["unit"]["definition_sha256"]
            or {r["id"]: r["version"] for r in records} != manifest["atoms"]
            or set(manifest["sources"]) != set(point["sources"])
            or {k for r in records for k in {r["event_id"], *source_ids(r["payload"])}}
            != set(grants)
            or len(records) > 64
            or len(records) + len(grants) > 128
        ):
            raise DerivedError("derived_history_integrity_failed")
        frozen_query = archive["query"]
        if (
            digest(dict(query=frozen_query["spec"], policy=archive["policy"]))
            != frozen_query["fingerprint"]
            or frozen_query["slots"] != point["slots"]
            or frozen_query["generation"] != point["unit"]["bindings"]["query"]["generation"]
            or frozen_query["fingerprint"] != point["unit"]["bindings"]["query"]["sha256"]
        ):
            raise DerivedError("derived_history_integrity_failed")
        sources = {}
        for source_id, data in manifest["sources"].items():
            source = await uow.get_source_event(self.scope, source_id)
            if source is None or source.scope != self.scope or source.content_hash != data["hash"]:
                raise DerivedError("derived_history_input_erased")
            sources[source_id] = source
        for row in records:
            current = await uow.get_admission_record(self.scope, row["id"])
            if current is None or current["payload"].get("deleted"):
                raise DerivedError("derived_history_input_erased")
            if datetime.fromisoformat(row["recorded_at"]) > datetime.fromisoformat(
                point["known_at"]
            ):
                raise DerivedError("derived_history_integrity_failed")
        result = compose(
            spec, records, sources, query.valid_at,
            admission_policy=archive["policy"], known_at=query.known_at,
        )
        from .service import authorization_summary

        return dict(
            facet_id=facet_id,
            state="empty" if result["no_outputs"] else "ready",
            revision_id=point["revision_id"],
            known_at=iso(query.known_at),
            valid_at=iso(query.valid_at),
            history_mode=self.service.history_mode,
            body=None if result["no_outputs"] else result["body"],
            coverage=coverage,
            authorization=authorization_summary(
                dict(readers=[actor], purpose=purpose), sources, grants
            ),
        )
