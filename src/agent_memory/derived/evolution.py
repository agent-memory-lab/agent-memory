"""Registered evidence selectors and reviewed L2/L3 maintenance.

Native admission writes invalidate complete selectors, including new members and
empty sets. Evolution uses the existing fenced/coalescing scheduler; no model
output, scene summary or persona revision becomes evidence for a later profile.
"""

from copy import deepcopy
from dataclasses import asdict
from datetime import datetime, timedelta

from ..consolidation.admission_runtime import AdmissionEngine
from ..lifecycle import is_memory_context
from ..operations.facet_refresh import checked_job, stale, valid_completion
from ..operations.refresh_demand import RefreshDemandQueue, publish_coverage, record_dirty
from ..operations.refresh_policy import RefreshPolicy
from ..operations.source_revisions import source_is_current
from . import subscriptions
from .model import DerivedError, digest, identity
from .persona import KINDS, PersonaEvidence, PersonaViews
from .persona_policy import (
    CategoricalPersonaPolicy,
    CategoricalPersonaProposer,
    CategoricalPersonaReviewer,
    PersonaDefinition,
    PersonaProposal,
    PersonaReview,
)
from .registry import slots
from .scenario_evolution import ScenarioEvolution as ScenarioEvolution
from .service import open_derived

SCHEMA = "persona-definition/1"
UNIT_SCHEMA = "persona-refresh-unit/1"
DENIED = {
    "derived_processing_denied",
    "derived_processing_grant_expired",
    "derived_processing_expired",
    "derived_permission_denied",
    "derived_grant_authority_changed",
}


class PersonaEvolution:
    """Callable reviewed pipeline; host may add ``processor`` to a shared queue.

    Proposer/reviewer are distinct approved host adapters with immutable revision
    identifiers. A model adapter must use the governed model host itself; this
    pipeline grants neither external-model permissions nor autonomous truth.
    """

    task_type = "memory.facet_refresh"

    def __init__(self, views, *, proposer, reviewer, queue=None):
        if (
            proposer is reviewer
            or not callable(getattr(proposer, "propose", None))
            or not callable(getattr(reviewer, "review", None))
        ):
            raise DerivedError("persona_independent_review_required")
        for adapter in (proposer, reviewer):
            identity(adapter.revision)
        if reviewer.revision != views.reviewer_revision:
            raise DerivedError("persona_reviewer_configuration_changed")
        self.views, self.service = views, views.service
        self.repository, self.scope, self.clock = views.repository, views.scope, views.service.clock
        self.proposer, self.reviewer = proposer, reviewer
        self._proposer_revision, self._reviewer_revision = proposer.revision, reviewer.revision
        self.key = "persona-evolution/1:" + digest(
            [
                self.scope.partition_key(),
                views.policy_sha256,
                proposer.revision,
                reviewer.revision,
                self.service.authority_id,
            ]
        )
        self.processor = self
        self.queue = queue or RefreshDemandQueue((self,), clock=self.clock)
        if queue is not None:
            if queue.repository is not self.repository:
                raise DerivedError("persona_queue_repository_mismatch")
            old = queue.processors.get(self.key)
            if old is not None and old is not self:
                raise DerivedError("persona_processor_configuration_changed")
            if old is None and len(queue.processors) >= 128:
                raise DerivedError("persona_processor_capacity")
            queue.processors[self.key] = self

    def _adapters_current(self):
        if (
            self.proposer.revision != self._proposer_revision
            or self.reviewer.revision != self._reviewer_revision
        ):
            raise DerivedError("persona_adapter_configuration_changed")

    def instance_id(self, definition):
        return "question-instance:" + digest(
            [self.scope.partition_key(), "persona-evolution/1", definition["facet_id"]]
        )

    def target_metadata(self, definition):
        return dict(
            definition_fingerprint=definition["fingerprint"],
            context_fingerprint=digest(definition["spec"]["context"]),
            request_semantics_digest=digest([self.key, definition["spec"]]),
        )

    def definition_id(self, label):
        return "persona-definition:" + digest([self.scope.partition_key(), identity(label)])

    async def register(self, definition, *, expected_generation=0, refresh_policy=None):
        if type(definition) is not PersonaDefinition:
            raise TypeError("trusted PersonaDefinition required")
        if definition.subject_id != self.scope.user_id or set(definition.predicates) - {
            p["predicate"] for p in self.service.policy["predicates"]
        }:
            raise DerivedError("persona_selector_unregistered")
        if type(expected_generation) is not int or expected_generation < 0:
            raise DerivedError("invalid_persona_generation")
        spec = dict(schema=SCHEMA, authority_id=self.service.authority_id, **asdict(definition))
        fingerprint = digest([spec, self.key])
        key = self.definition_id(definition.label)
        async with self.repository.unit_of_work() as uow:
            epoch = await open_derived(uow, self.scope)
            await self.views._permission(uow, (), definition.readers, definition.purpose)
            old = await uow.derived_get(self.scope, "definition", key)
            if old and old.get("fingerprint") == fingerprint and not old.get("disabled"):
                return deepcopy(old)
            if (old.get("generation", 0) if old else 0) != expected_generation:
                raise DerivedError("persona_definition_conflict")
            if not old and len(await uow.derived_records(self.scope, "definition")) >= 128:
                raise DerivedError("persona_definition_capacity")
            row = dict(
                facet_id=key,
                spec=spec,
                fingerprint=fingerprint,
                generation=expected_generation + 1,
                epoch=epoch,
                safety_generation=(old or {}).get("safety_generation", 0),
                time_generation=0,
                slots=slots(self.scope, definition.subject_id, definition.predicates),
                dirty=True,
                disabled=False,
                refresh_managed=True,
                last_persona_revision=(old or {}).get("last_persona_revision"),
            )
            await uow.derived_put(self.scope, "definition", key, row)
            await subscriptions.install(uow, self.scope, row)
        await self.queue.configure(
            key, refresh_policy or RefreshPolicy(mode="on_change"), processor_key=self.key
        )
        return deepcopy(row)

    async def definition(self, uow, facet_id):
        epoch = await open_derived(uow, self.scope)
        row = await uow.derived_get(self.scope, "definition", facet_id)
        if (
            not row
            or row.get("disabled")
            or row.get("spec", {}).get("schema") != SCHEMA
            or digest([row["spec"], self.key]) != row.get("fingerprint")
        ):
            raise DerivedError("persona_definition_unavailable")
        if row["epoch"] != epoch:
            raise DerivedError("persona_definition_epoch_changed")
        return row

    async def authorize(self, uow, definition, actor):
        if actor not in definition["spec"]["readers"]:
            raise DerivedError("derived_read_denied")
        await self.views._permission(uow, (), (actor,), definition["spec"]["purpose"])

    async def required_parents(self, uow, definition):
        return []

    async def mark_dirty(self, label, *, reason="review_requested"):
        if reason not in {"review_requested", "evidence_changed", "policy_changed"}:
            raise DerivedError("invalid_persona_refresh_reason")
        async with self.repository.unit_of_work() as uow:
            definition = await self.definition(uow, self.definition_id(label))
            definition["dirty"] = True
            await uow.derived_put(self.scope, "definition", definition["facet_id"], definition)
            return await record_dirty(uow, self.scope, definition, at=self.clock(), reason=reason)

    async def initialize(self, uow):
        """Backfill changed epochs after erasure; never reset existing live work."""
        epoch = await open_derived(uow, self.scope)
        for item in await uow.derived_records(self.scope, "definition"):
            definition = item["payload"]
            if definition.get("disabled") or definition.get("spec", {}).get("schema") != SCHEMA:
                continue
            if digest([definition["spec"], self.key]) != definition.get("fingerprint"):
                continue
            if definition["epoch"] != epoch:
                definition.update(
                    epoch=epoch, dirty=True, safety_generation=definition["safety_generation"] + 1
                )
                await uow.derived_put(self.scope, "definition", item["identity"], definition)
                policy = await uow.derived_get(self.scope, "refresh_policy", item["identity"])
                if policy and policy.get("state") != "erased":
                    policy["epoch"] = epoch
                    await uow.derived_put(self.scope, "refresh_policy", item["identity"], policy)
                await record_dirty(
                    uow, self.scope, definition, at=self.clock(), reason="erase_epoch"
                )

    async def discover_changes(self):
        """Bounded startup reconciliation. Native semantic writes are immediate triggers."""
        async with self.repository.unit_of_work() as uow:
            await self.initialize(uow)
            rows = await uow.derived_records(self.scope, "definition")
            return tuple(
                row["identity"]
                for row in rows
                if row["payload"].get("spec", {}).get("schema") == SCHEMA
                and row["payload"].get("dirty")
                and not row["payload"].get("disabled")
            )

    async def _inventory(self, uow, definition):
        self._adapters_current()
        spec, now = definition["spec"], self.clock()
        await self.views._permission(uow, (), spec["readers"], spec["purpose"])
        headers = await uow.derived_candidates(self.scope, definition["slots"])
        if len(headers) > 64:
            raise DerivedError("persona_evidence_capacity")
        proof, facts, transitions, complete = {}, [], [], True
        if len({source for header in headers for source in header["source_ids"]}) > 128:
            raise DerivedError("persona_dependency_capacity")
        for header in sorted(headers, key=lambda h: h["id"]):
            source_id = header["event_id"]
            dependencies = sorted({source_id, *header["source_ids"]})
            grants = {key: await uow.derived_get(self.scope, "grant", key) for key in dependencies}
            proof[header["id"]] = dict(
                version=header["version"],
                source_id=source_id,
                grants={key: digest(grant) for key, grant in grants.items()},
                selected=False,
            )
            try:
                await self.views._permission(uow, dependencies, spec["readers"], spec["purpose"])
            except DerivedError as error:
                if error.code in DENIED:
                    complete = False
                    continue
                raise
            row = await uow.get_admission_record(self.scope, header["id"])
            if not row or row["version"] != header["version"]:
                raise DerivedError("derived_snapshot_changed")
            data, draft = row["payload"], row["payload"]["draft"]
            if sorted({source_id, *AdmissionEngine.source_dependencies(data)}) != dependencies:
                raise DerivedError("derived_snapshot_changed")
            proof[header["id"]]["draft_sha256"] = digest(draft)
            if (
                data.get("deleted")
                or data.get("action") not in {"ACCEPT", "CONTESTED", "PENDING_VERIFICATION"}
                or draft.get("conditions")
            ):
                continue
            start = datetime.fromisoformat(data["valid_from"])
            end = datetime.fromisoformat(data["valid_to"]) if data.get("valid_to") else None
            if start > now:
                transitions.append(start)
                continue
            if end and end <= now:
                continue
            source = await uow.get_source_event(self.scope, source_id)
            if (
                not source
                or not isinstance(source.metadata.get("_retention"), dict)
                or is_memory_context(source)
                or not await source_is_current(uow, source)
                or source.metadata.get("lifecycle", {}).get("origin") == "model"
            ):
                continue
            dependency_hashes, available = {source_id: source.content_hash}, True
            for dependency in sorted(set(dependencies) - {source_id}):
                extra = await uow.get_source_event(self.scope, dependency)
                if (
                    extra is None
                    or isinstance(extra.metadata.get("_retention"), dict)
                    and not await source_is_current(uow, extra)
                ):
                    available = False
                    complete = False
                    break
                dependency_hashes[dependency] = extra.content_hash
            if not available:
                continue
            proof[header["id"]]["dependency_hashes"] = dependency_hashes
            quote = draft["source_quote"]
            offset = source.content.find(quote)
            if offset < 0 or not quote:
                raise DerivedError("persona_source_unavailable")
            family = source.metadata.get("_retention", {}).get("document_id", source.id)
            proof[header["id"]].update(selected=True, source_sha256=source.content_hash)
            reports = data.get("extraction", {}).get("reports", ())
            faithfulness = (
                "supported"
                if reports and all(r.get("faithfulness") == "supported" for r in reports)
                else "uncertain"
                if reports
                else "not_reviewed"
            )
            evidence = PersonaEvidence(
                source_id,
                offset,
                offset + len(quote),
                quote,
                family,
                relation="supports" if data["action"] == "ACCEPT" else "counterexample",
                admission_status="admitted"
                if data["action"] == "ACCEPT"
                else "observed_unverified",
                atom_id=row["id"] if data["action"] == "ACCEPT" else None,
                atom_version=row["version"] if data["action"] == "ACCEPT" else None,
            )
            facts.append(
                dict(
                    atom_id=row["id"],
                    admission_action=data["action"],
                    predicate=draft["predicate"],
                    value=deepcopy(draft["value"]),
                    evidence=asdict(evidence),
                    kind=draft.get("kind"),
                    modality=draft.get("modality"),
                    negated=draft.get("negated", False),
                    conditions=deepcopy(draft.get("conditions", [])),
                    exceptions=deepcopy(draft.get("exceptions", [])),
                    faithfulness=faithfulness,
                    admission_reasons=deepcopy(data.get("reasons", [])),
                    observed_at=source.occurred_at.isoformat(),
                    valid_from=start.isoformat(),
                    valid_to=end.isoformat() if end else None,
                )
            )
            if end:
                transitions.append(end)
            for grant in grants.values():
                if grant.get("expires_at"):
                    transitions.append(datetime.fromisoformat(grant["expires_at"]))
        if len(facts) > 32:
            raise DerivedError("persona_evidence_capacity")
        authority = await self.service.registry.authority(uow, self.service.authority_id)
        proof["authority"] = dict(version=authority["version"], sha256=authority["fingerprint"])
        transitions.append(datetime.fromisoformat(authority["spec"]["expires_at"]))
        return dict(
            proof=proof,
            facts=facts,
            complete=complete,
            transitions=sorted({t.isoformat() for t in transitions}),
        )

    def _unit(self, definition, inventory, head):
        return dict(
            schema=UNIT_SCHEMA,
            facet_id=definition["facet_id"],
            epoch=definition["epoch"],
            definition_sha256=definition["fingerprint"],
            generation=definition["generation"],
            safety_generation=definition["safety_generation"],
            time_generation=definition["time_generation"],
            inputs_sha256=digest(inventory),
            previous_head_sha256=(head or {}).get("sha256"),
        )

    async def freeze(self, uow, definition):
        inventory = await self._inventory(uow, definition)
        head = await uow.derived_get(
            self.scope,
            KINDS[0],
            "persona:" + digest([self.scope.partition_key(), definition["spec"]["label"]]),
        )
        unit = self._unit(definition, inventory, head)
        key = "persona-unit:" + digest(unit)
        old = await uow.derived_get(self.scope, "job", key)
        if old:
            return old
        if len(await uow.derived_records(self.scope, "job")) >= 4096:
            raise DerivedError("derived_refresh_backpressure")
        now = self.clock()
        job = dict(
            id=key,
            unit=unit,
            status="pending",
            attempts=0,
            generation=0,
            created_at=now.isoformat(),
            next_attempt_at=now.isoformat(),
            expires_at=(now + timedelta(days=1)).isoformat(),
        )
        await uow.derived_put(self.scope, "job", key, job)
        return job

    async def check_task(self, uow, task, *, completed=False):
        if task.payload.get("adapter_key") != self.key or not task.payload.get("refresh_execution"):
            raise DerivedError("refresh_processor_unsupported")
        return await checked_job(self.service, uow, task, completed=completed)

    async def snapshot(self, task):
        async with self.repository.unit_of_work() as uow:
            await open_derived(uow, self.scope)
            job = await self.check_task(uow, task)
            definition = await self.definition(uow, job["unit"]["facet_id"])
            inventory = await self._inventory(uow, definition)
            head = await uow.derived_get(
                self.scope,
                KINDS[0],
                "persona:" + digest([self.scope.partition_key(), definition["spec"]["label"]]),
            )
            if self._unit(definition, inventory, head) != job["unit"]:
                raise DerivedError("derived_snapshot_changed")
            frozen = dict(
                unit=deepcopy(job["unit"]),
                definition=deepcopy(definition),
                inventory=inventory,
                expected_version=(head or {}).get("version", 0),
            )
            job["snapshot_sha256"] = digest(frozen)
            await uow.derived_put(self.scope, "job", job["id"], job)
            return frozen

    def prepare(self, snapshot):
        return deepcopy(snapshot)

    async def _checked_snapshot(self, uow, task, snapshot):
        await open_derived(uow, self.scope)
        job = await self.check_task(uow, task)
        definition = await self.definition(uow, job["unit"]["facet_id"])
        inventory = await self._inventory(uow, definition)
        head = await uow.derived_get(
            self.scope,
            KINDS[0],
            "persona:" + digest([self.scope.partition_key(), definition["spec"]["label"]]),
        )
        if (
            digest(snapshot) != job.get("snapshot_sha256")
            or inventory != snapshot["inventory"]
            or self._unit(definition, inventory, head) != job["unit"]
        ):
            raise DerivedError("derived_snapshot_changed")
        return job, definition, inventory

    async def publish(self, task, snapshot, prepared):
        if prepared != snapshot:
            raise DerivedError("derived_input_changed")
        snapshot = deepcopy(snapshot)
        # Never send forged, withdrawn or expired inputs to semantic adapters.
        # The same full proof is checked again in the atomic publication below.
        async with self.repository.unit_of_work() as uow:
            await self._checked_snapshot(uow, task, snapshot)
        spec, facts = snapshot["definition"]["spec"], snapshot["inventory"]["facts"]
        families = {f["evidence"]["family"] for f in facts if f["admission_action"] == "ACCEPT"}
        proposal, review = None, None
        reason = "insufficient_support" if snapshot["inventory"]["complete"] else "source_changed"
        observations = [
            datetime.fromisoformat(f["observed_at"])
            for f in facts
            if f["admission_action"] == "ACCEPT"
        ]
        span = (max(observations) - min(observations)).total_seconds() if observations else 0
        if (
            snapshot["inventory"]["complete"]
            and len(families) >= self.views.minimum_families
            and span >= spec["minimum_span_seconds"]
        ):
            proposal = await self.proposer.propose(deepcopy(spec), deepcopy(facts))
            if type(proposal) is not PersonaProposal or {r[0] for r in proposal.relations} != {
                f["atom_id"] for f in facts
            }:
                raise DerivedError("persona_proposal_incomplete_census")
            if any(
                dict(proposal.relations)[f["atom_id"]] == "supports"
                and f["admission_action"] != "ACCEPT"
                for f in facts
            ):
                raise DerivedError("persona_unverified_support_forbidden")
            if any(
                dict(proposal.relations)[f["atom_id"]] == "counterexample"
                and f["admission_action"] != "ACCEPT"
                and (f["faithfulness"] != "supported" or f["modality"] != "asserted")
                for f in facts
            ):
                raise DerivedError("persona_unfaithful_counterexample_forbidden")
            review = await self.reviewer.review(deepcopy(spec), proposal, deepcopy(facts))
            if type(review) is not PersonaReview:
                raise DerivedError("invalid_persona_review")
            reason = "reviewer_withdrawal"
            support = {
                f["evidence"]["family"]
                for f in facts
                if dict(proposal.relations)[f["atom_id"]] == "supports"
            }
            support_observations = [
                datetime.fromisoformat(f["observed_at"])
                for f in facts
                if dict(proposal.relations)[f["atom_id"]] == "supports"
            ]
            support_span = (
                (max(support_observations) - min(support_observations)).total_seconds()
                if support_observations
                else 0
            )
            if (
                len(support) < self.views.minimum_families
                or support_span < spec["minimum_span_seconds"]
            ):
                reason = "insufficient_support"
                review = PersonaReview("withdraw", "unsupported")
        async with self.repository.unit_of_work() as uow:
            job, definition, inventory = await self._checked_snapshot(uow, task, snapshot)
            binding = dict(
                definition_id=definition["facet_id"],
                processor_key=self.key,
                unit_sha256=digest(job["unit"]),
                proposer_revision=self.proposer.revision,
                reviewer_revision=self.reviewer.revision,
                candidate_parents=["atom:" + f["atom_id"] for f in facts],
                processing_source_hashes={
                    source_id: sha
                    for proof in inventory["proof"].values()
                    if isinstance(proof, dict)
                    for source_id, sha in proof.get("dependency_hashes", {}).items()
                },
            )
            now = self.clock()
            future = [
                datetime.fromisoformat(t)
                for t in inventory["transitions"]
                if datetime.fromisoformat(t) > now
            ]
            if review is not None and review.decision == "publish":
                until = min([now + timedelta(seconds=spec["validity_seconds"]), *future])
                evidence = tuple(
                    PersonaEvidence(
                        **{**f["evidence"], "relation": dict(proposal.relations)[f["atom_id"]]}
                    )
                    for f in facts
                    if dict(proposal.relations)[f["atom_id"]] != "not_relevant"
                )
                result = await self.views._publish_in_uow(
                    uow,
                    spec["label"],
                    proposal.text,
                    origin="inferred",
                    evidence=evidence,
                    readers=spec["readers"],
                    purpose=spec["purpose"],
                    context=spec["context"],
                    valid_from=now,
                    valid_to=until,
                    expected_version=snapshot["expected_version"],
                    evolution=binding,
                )
                definition["next_transition_at"] = until.isoformat()
            else:
                result = await self.views._withdraw_in_uow(
                    uow,
                    spec["label"],
                    reason=reason,
                    readers=spec["readers"],
                    purpose=spec["purpose"],
                    context=spec["context"],
                    expected_version=snapshot["expected_version"],
                    evolution=binding,
                    sources=[f["evidence"]["source_id"] for f in facts],
                    parents=["atom:" + f["atom_id"] for f in facts],
                )
                definition["next_transition_at"] = min(future).isoformat() if future else None
            # Recheck native census after the publication's storage awaits.
            if await self._inventory(uow, definition) != inventory:
                raise DerivedError("derived_snapshot_changed")
            await self.check_task(uow, task)
            definition["last_persona_revision"] = result["revision_id"]
            job.update(
                status="completed",
                outcome="applied",
                no_outputs=False,
                revision_id=result["revision_id"],
                completed_at=self.clock().isoformat(),
                commit_token="derived-commit:"
                + digest(
                    [self.scope.partition_key(), job["unit"], result["revision_id"], "applied"]
                ),
            )
            await uow.derived_put(self.scope, "job", job["id"], job)
            await publish_coverage(
                uow,
                self.service,
                job,
                definition,
                manifest=dict(
                    schema="persona-publication/1",
                    unit=job["unit"],
                    query_complete=True,
                    revision_id=result["revision_id"],
                ),
                now=self.clock(),
            )
            await self.check_task(uow, task, completed=True)
            if self.clock() >= datetime.fromisoformat(job["lease_until"]):
                raise stale()
            return result

    async def verify_coverage(self, uow, execution):
        if not execution or execution.get("adapter_key") != self.key:
            return None
        job = await uow.derived_get(self.scope, "job", execution["unit_id"])
        if not job or job.get("unit") != execution["unit"] or not valid_completion(self.scope, job):
            return None
        publication = await uow.derived_get(self.scope, "refresh_publication", execution["id"])
        header = await uow.derived_get(self.scope, KINDS[0], job["revision_id"])
        if (
            not publication
            or publication.get("state") != "committed"
            or not header
            or header.get("revision_id") != job["revision_id"]
            or publication.get("commit_token") != job["commit_token"]
            or publication.get("claimed") != execution["claimed"]
            or publication.get("unit") != execution["unit"]
            or publication.get("compatibility") != execution["compatibility"]
            or publication.get("sha256")
            != digest({k: v for k, v in publication.items() if k != "sha256"})
            or header.get("sha256") != digest({k: v for k, v in header.items() if k != "sha256"})
        ):
            return None
        body = await uow.derived_get(self.scope, KINDS[1], job["revision_id"])
        if not body or digest(body.get("result")) != header.get("body_sha256"):
            return None
        return publication

    async def read(self, label, *, actor, context, purpose="agent_context"):
        self._adapters_current()
        return await self.views.read(label, actor=actor, purpose=purpose, context=context)

    async def status(self, label, *, actor):
        async with self.repository.unit_of_work() as uow:
            definition = await self.definition(uow, self.definition_id(label))
            await self.authorize(uow, definition, actor)
            head = await uow.derived_get(
                self.scope, KINDS[0], "persona:" + digest([self.scope.partition_key(), label])
            )
            return dict(
                label=label,
                state=(head or {}).get("state", "building"),
                version=(head or {}).get("version", 0),
                dirty=definition["dirty"],
                revision_id=(head or {}).get("revision_id"),
            )


def categorical_persona_evolution(service, policy, *, queue=None):
    """Build separate approved categorical adapters; callers still register selectors."""
    if type(policy) is not CategoricalPersonaPolicy:
        raise TypeError("trusted CategoricalPersonaPolicy required")
    proposer, reviewer = CategoricalPersonaProposer(policy), CategoricalPersonaReviewer(policy)
    return PersonaEvolution(
        PersonaViews(
            service, reviewer_revision=reviewer.revision, minimum_families=policy.minimum_families
        ),
        proposer=proposer,
        reviewer=reviewer,
        queue=queue,
    )
