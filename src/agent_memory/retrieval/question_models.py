"""Explicit model explanations of registered, current QuestionViews.

Host setup owns the model, recipient policy, grants, template, validator and
accounts. Transports accept only a registered question ID. All inherited sources
are authorized for provider processing before any QuestionView body is loaded.
There is no free-text prompt, client-supplied lineage, semantic cache or fallback.
"""

from dataclasses import asdict

from .model_answers import GovernedModelAnswers
from .model_authority import SourceModelAuthority
from .model_contracts import ModelCoordinates, ModelError, canonical, digest


class _ProviderGuardedQuestionRead:
    """Enforce provider processing rights at each actual QuestionView body call.

    B3 guards ordinary query/read authority. This finite wrapper adds the distinct
    model-recipient guard after those metadata awaits, without widening B3's
    public API or changing ordinary model-free reads.
    """

    def __init__(self, uow, authority, coordinates, source_ids, expected):
        self.uow, self.authority, self.coordinates = uow, authority, coordinates
        self.source_ids, self.expected = source_ids, expected

    def __getattr__(self, name):
        return getattr(self.uow, name)

    async def derived_get(self, scope, kind, identity):
        protected = kind in {"question_content", "question_certificate"}
        if protected:
            current = await self.authority._processing(
                self.uow, self.coordinates, self.source_ids, self.expected[0]
            )
            if current != self.expected:
                raise ModelError("model_input_changed")
        result = await self.uow.derived_get(scope, kind, identity)
        if protected:
            self.authority._check_expiry(self.expected[1], self.expected[2])
        return result


class QuestionModelAuthority(SourceModelAuthority):
    """Concrete B3/B4 metadata proof, separate from raw-source host callbacks."""

    def __init__(self, questions, *, public_template, configuration):
        from ..derived.question_service import QuestionService

        if type(questions) is not QuestionService:
            raise ModelError("model_question_service_required")
        self.questions = questions
        super().__init__(
            questions.admission,
            public_template=public_template,
            configuration=configuration,
            verify_coordinates=self._verify,
        )

    async def _verify(self, uow, coordinates):
        # Deliberately not a host-supplied boolean shortcut.
        metadata = await self._header(uow, coordinates.question_definition, coordinates.principal)
        return coordinates == self._coordinates(metadata, coordinates.principal)

    async def _header(self, uow, question_id, actor):
        await self.questions._open(uow)
        loader = getattr(self.questions, "model_input_header", None)
        if not callable(loader):
            raise ModelError("model_original_generation_proof_unsupported")
        return await loader(uow, question_id, actor=actor)

    def _coordinates(self, metadata, actor):
        definition, header = metadata["definition"], metadata["header"]
        spec = definition["spec"]
        # Current is reused ONLY inside the head's proven validity interval;
        # this exact origin coordinate is stable while _guard proves that window.
        return ModelCoordinates(
            self.scope.partition_key(),
            actor,
            spec["project_id"],
            spec["purpose"],
            actor,
            spec["question_id"],
            spec["instance"]["definition"]["version"],
            canonical(spec["instance"]["parameters"]),
            "explain_registered_question/1",
            digest(spec["context"]),
            definition["fingerprint"],
            digest({"header": header, "generation": metadata["original_generation_manifest"]}),
            header["validated_at"],
            header["validated_at"],
        )

    async def _assemble(self, uow, coordinates, source_ids=None):
        metadata = await self._header(uow, coordinates.question_definition, coordinates.principal)
        if coordinates != self._coordinates(metadata, coordinates.principal):
            raise ModelError("model_question_proof_changed")
        ids = tuple(sorted(metadata["source_ids"]))
        if len(ids) > 256 or len(set(ids)) != len(ids):
            raise ModelError("invalid_model_sources")
        epoch = await uow.retention_epoch(self.scope)
        _, authority, grants = await self._processing(uow, coordinates, ids, epoch)
        # The first body read occurs only AFTER all original and current inputs
        # pass the independent provider/account/endpoint/purpose grant check.
        guarded = _ProviderGuardedQuestionRead(
            uow, self, coordinates, ids, (epoch, authority, grants)
        )
        result = await self.questions._read_in_uow(
            guarded, coordinates.question_definition, actor=coordinates.principal
        )
        original = metadata["original_generation_manifest"]
        if result["generation_manifest"] != original:
            raise ModelError("model_original_generation_changed")
        # A complete, bounded result retains qualifiers, unknowns, conflicts and
        # source quotes. Runtime refresh activity is not part of model input.
        result = {key: value for key, value in result.items() if key != "refresh_status"}
        sources = {}
        for source_id in ids:
            proof = await uow.derived_project_source_proof(self.scope, source_id)
            if not proof:
                raise ModelError("model_source_unavailable")
            sources[source_id] = dict(proof=proof, **grants[source_id])
        current = await self._processing(uow, coordinates, ids, epoch)
        if current != (epoch, authority, grants):
            raise ModelError("model_input_changed")
        latest = await self._header(uow, coordinates.question_definition, coordinates.principal)
        if latest != metadata:
            raise ModelError("model_question_proof_changed")
        self._check_expiry(authority, grants)
        return self._seal(
            coordinates,
            sources,
            [
                {"role": "system", "content": self.template},
                {"role": "user", "content": canonical(result)},
            ],
            epoch,
            authority,
            question_header_sha256=digest(metadata["header"]),
            inherited_generation_manifest=original,
            question_content_revision=result["content_revision_id"],
            question_certificate_revision=result["certificate_revision_id"],
            question_instance_id=result["instance_id"],
        )

    async def prepare_question(self, question_id, *, actor):
        observed = await self.clock_barrier()
        try:
            async with self.repository.unit_of_work() as uow:
                metadata = await self._header(uow, question_id, actor)
                return await self._assemble(uow, self._coordinates(metadata, actor))
        except BaseException:
            await self.failure_barrier(observed)
            raise


class QuestionModelRuntime:
    """Opt-in host binding; construction performs no network request or download.

    The supplied validator is a mandatory output contract, not an assertion of
    real-model semantic quality. Real acceptance must still be evaluated with a
    frozen model, licensed workload and full costs. Plain answers remain model-free.
    """

    def __init__(
        self,
        questions,
        port,
        *,
        public_template,
        account_keys,
        validate_output,
        maximum_microunits=None,
        upper_bound_evidence=None,
        **options,
    ):
        self.authority = QuestionModelAuthority(
            questions, public_template=public_template, configuration=port.configuration
        )
        self.answers = GovernedModelAnswers(
            self.authority,
            port,
            account_keys=account_keys,
            validate_output=validate_output,
            maximum_microunits=maximum_microunits,
            upper_bound_evidence=upper_bound_evidence,
            **options,
        )
        self.questions = questions
        # Host registration is separate from untrusted MCP/SDK call parameters.
        questions.models = self

    def capabilities(self):
        cfg = self.authority.configuration
        return dict(
            schema="question-model-runtime/1",
            enabled=True,
            experimental=True,
            provider=cfg.provider,
            configuration_sha256=cfg.fingerprint,
            input_budget="exact_utf8_request_bytes",
            strict_input_tokens=False,
            strict_cost_budget=False,
            strict_immutable_server_execution=False,
            buffered=True,
            streaming=False,
            tools=False,
            failover=False,
            exact_cache=True,
            current_only=True,
            original_input_lineage=True,
            real_quality_cost_acceptance="pending_external_evidence",
        )

    async def answer(self, question_id, *, actor):
        sealed = await self.authority.prepare_question(question_id, actor=actor)
        # The governor freezes this entire public envelope before the final
        # authorization transaction. There is no second response assembly later.
        return await self.answers.answer(
            sealed,
            serialize=lambda answer: dict(
                schema="question-model-answer/1",
                question_id=question_id,
                input_contract="question-answer/1",
                experimental=True,
                factual_authority="registered_structured_question_only",
                **asdict(answer),
            ),
        )
