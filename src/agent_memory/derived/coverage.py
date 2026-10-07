"""Continuous coverage certificates, closed in the original semantic write UoW.

A complete published census stays valid until the first tracked semantic change.
Changes create honest gaps until another complete publication. Permissions never
extend semantic coverage; they are independently rechecked on every delivery.
"""

from copy import deepcopy
from datetime import UTC

from .model import HISTORY_INTERVAL, DerivedError, digest, timestamp


def iso(value):
    return timestamp(value).astimezone(UTC).isoformat()


def semantic_unit(unit):
    """Valid-time transitions and current permission versions are not old meaning."""
    return {
        key: deepcopy(unit[key])
        for key in (
            "facet_id",
            "definition_sha256",
            "definition_generation",
            "epoch",
            "query_generation",
        )
    } | {"query": deepcopy(unit["bindings"]["query"])}


def checked(row, facet_id):
    if row is None or row.get("state") == "erased":
        return None
    if (
        row.get("schema") != "derived-history-intervals/1"
        or row.get("facet_id") != facet_id
        or digest({k: v for k, v in row.items() if k != "sha256"}) != row.get("sha256")
        or len(row["spans"]) > 128
    ):
        raise DerivedError("derived_history_integrity_failed")
    return row


async def save(uow, scope, row):
    row["version"] += 1
    row["sha256"] = digest({k: v for k, v in row.items() if k != "sha256"})
    await uow.derived_put(scope, "history_interval", row["facet_id"], row)


async def close_coverage(uow, scope, facet_id, *, at, reason):
    """Only metadata; the writer's existing scope lock and transaction are required."""
    row = checked(await uow.derived_get(scope, "history_interval", facet_id), facet_id)
    if row is None:
        return
    at = iso(at)
    if at < row["last_at"]:
        # Backdated writer clocks cannot attest that any old interval was unchanged.
        for span in row["spans"]:
            span.update(state="uncertain", closed_by="clock_rollback")
    else:
        for span in row["spans"]:
            if span["state"] == "open":
                end = min(at, span["known_to"]) if span["known_to"] else at
                span.update(
                    known_to=end,
                    state="sealed" if end > span["known_from"] else "uncertain",
                    closed_by=reason,
                )
        row["last_at"] = at
    await save(uow, scope, row)


class PublishedCoverage:
    def __init__(self, service):
        self.service, self.scope = service, service.scope

    async def publish(self, uow, point):
        if self.service.history_mode != HISTORY_INTERVAL:
            return
        row = checked(
            await uow.derived_get(self.scope, "history_interval", point["facet_id"]),
            point["facet_id"],
        )
        if row is None or row["epoch"] != point["epoch"]:
            row = dict(
                schema="derived-history-intervals/1",
                facet_id=point["facet_id"],
                epoch=point["epoch"],
                state="tracked",
                version=0,
                spans=[],
                slots=point["slots"],
                last_at=point["known_at"],
            )
        if point["known_at"] < row["last_at"]:
            raise DerivedError("derived_history_clock_unordered")
        if any(span["point_id"] == point["id"] for span in row["spans"]):
            return  # Never reopen an existing certificate after a semantic change.
        if len(row["spans"]) >= 128:
            raise DerivedError("derived_history_capacity")
        for span in row["spans"]:
            if span["state"] == "open":
                unchanged = span["semantic_unit"] == semantic_unit(point["unit"]) and span[
                    "inputs_sha256"
                ] == digest(point["input_versions"])
                span.update(
                    state="sealed" if unchanged else "uncertain",
                    known_to=min(point["known_at"], span["known_to"])
                    if span["known_to"] else point["known_at"],
                    closed_by="checkpoint" if unchanged else "untracked_change",
                )
        row["spans"].append(
            dict(
                point_id=point["id"],
                point_sha256=point["sha256"],
                known_from=point["known_at"],
                known_to=point.get("context", {}).get("known_to"),
                state="open",
                semantic_unit=semantic_unit(point["unit"]),
                inputs_sha256=digest(point["input_versions"]),
            )
        )
        row["last_at"] = point["known_at"]
        await save(uow, self.scope, row)

    async def select(self, uow, facet_id, definition, known_at):
        row = checked(await uow.derived_get(self.scope, "history_interval", facet_id), facet_id)
        if not row or row["epoch"] != await uow.retention_epoch(self.scope):
            raise DerivedError("derived_history_coverage_unavailable")
        if iso(self.service.clock()) < row["last_at"]:
            raise DerivedError("derived_history_coverage_unavailable")
        at = iso(known_at)
        matches = [
            s
            for s in row["spans"]
            if s["state"] in {"open", "sealed"}
            and s["known_from"] <= at
            and (s["known_to"] is None or at < s["known_to"])
        ]
        if len(matches) != 1:
            raise DerivedError("derived_history_coverage_unavailable")
        span = matches[0]
        if span["state"] == "open":
            current = (await self.service._unit(uow, definition)).payload()
            if semantic_unit(current) != span["semantic_unit"]:
                raise DerivedError("derived_history_coverage_unavailable")
        return span, dict(
            kind="interval",
            known_from=span["known_from"],
            known_to=span["known_to"],
            end_exclusive=True,
            observed_at=iso(self.service.clock()),
            query_complete=True,
        )

    async def verify_inputs(self, uow, span, point):
        if span["state"] != "open":
            return
        versions = point["input_versions"]
        if set(versions) != set(point["sources"]):
            raise DerivedError("derived_history_integrity_failed")
        for source_id, proof in versions.items():
            interpretation = await uow.retention_head_get(self.scope, "interpretation", source_id)
            document = (
                await uow.retention_head_get(self.scope, "document", proof["document_id"])
                if proof["document_id"]
                else None
            )
            if (
                digest(interpretation) != proof["interpretation"]
                or digest(document) != proof["document"]
            ):
                raise DerivedError("derived_history_coverage_unavailable")

    async def describe(self, uow, facet_id):
        row = checked(await uow.derived_get(self.scope, "history_interval", facet_id), facet_id)
        if row is None:
            return {}
        return {
            s["point_id"]: dict(
                known_from=s["known_from"], known_to=s["known_to"], state=s["state"]
            )
            for s in row["spans"]
        }
