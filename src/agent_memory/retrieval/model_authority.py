"""Concrete source input/dispatch/cache/delivery governance on the existing UoW.

This finite slice accepts raw retained sources only. Derived inputs require a
separate complete lineage adapter; they cannot be supplied as unlabelled text.
QuestionModelAuthority binds this contract to actual registered QuestionViews.
The raw-source variant requires a trusted metadata-only coordinate validator.
"""

import json
from datetime import datetime
from uuid import uuid4

from ..operations.source_revisions import source_is_current
from .model_contracts import ModelError, SealedModelInput, canonical, digest, text
from .source_processing import SourceProcessingAuthority


class SourceModelAuthority(SourceProcessingAuthority):
    def __init__(self, service, *, public_template, configuration, verify_coordinates):
        super().__init__(
            service, configuration=configuration, verify_coordinates=verify_coordinates
        )
        if digest(public_template) != configuration.template_sha256:
            raise ModelError("model_template_mismatch")
        text(public_template, limit=65536)
        self.template = public_template

    async def _assemble(self, uow, coordinates, source_ids):
        epoch, authority, grants = await self._metadata(uow, coordinates, source_ids)
        sources, messages = {}, [{"role": "system", "content": self.template}]
        for source_id in source_ids:
            current = await self._metadata(uow, coordinates, (source_id,))
            if current != (epoch, authority, {source_id: grants[source_id]}):
                raise ModelError("model_input_changed")
            source = await uow.get_source_event(self.scope, source_id)
            if source is None or not await source_is_current(uow, source):
                raise ModelError("model_source_unavailable")
            sources[source_id] = dict(content_sha256=source.content_hash, **grants[source_id])
            # The envelope and IDs are the actual input and counted in the byte budget.
            messages.append(
                {
                    "role": "user",
                    "content": canonical({"source_id": source_id, "content": source.content}),
                }
            )
        if await self._metadata(uow, coordinates, source_ids) != (epoch, authority, grants):
            raise ModelError("model_input_changed")
        return self._seal(coordinates, sources, messages, epoch, authority)

    def _seal(self, coordinates, sources, messages, epoch, authority, **lineage):
        cfg = self.configuration
        if digest(self.template) != cfg.template_sha256:
            raise ModelError("model_template_mismatch")
        payload = dict(
            model=cfg.model,
            messages=messages,
            stream=False,
            truncate=False,
            shift=False,
            options=json.loads(cfg.options_json),
            think=cfg.think,
            keep_alive=cfg.keep_alive,
        )
        if json.loads(cfg.output_schema_json):
            payload["format"] = json.loads(cfg.output_schema_json)
        payload_json = canonical(payload)
        from hashlib import sha256

        manifest = dict(
            schema="model-input-manifest/1",
            sources=sources,
            epoch=epoch,
            authority_sha256=digest(authority),
            recipient=cfg.recipient,
            public_template_sha256=cfg.template_sha256,
            payload_sha256=sha256(payload_json.encode()).hexdigest(),
            configuration_sha256=cfg.fingerprint,
            coordinates_sha256=digest(coordinates.payload()),
            input_bytes=len(payload_json.encode()),
            tokenizer=cfg.tokenizer_revision,
            **lineage,
        )
        return SealedModelInput(cfg, coordinates, payload_json, canonical(manifest))

    async def prepare(self, coordinates, source_ids):
        source_ids = tuple(sorted(source_ids))
        if not 1 <= len(source_ids) <= 256 or len(set(source_ids)) != len(source_ids):
            raise ModelError("invalid_model_sources")
        for source_id in source_ids:
            text(source_id)
        observed = await self.clock_barrier()
        try:
            async with self.repository.unit_of_work() as uow:
                return await self._assemble(uow, coordinates, source_ids)
        except BaseException:
            await self.failure_barrier(observed)
            raise

    async def validate(self, uow, sealed):
        if type(sealed) is not SealedModelInput or sealed.configuration != self.configuration:
            raise ModelError("model_configuration_changed")
        sources = tuple(sorted(json.loads(sealed.manifest_json)["sources"]))
        fresh = await self._assemble(uow, sealed.coordinates, sources)
        if fresh != sealed:
            raise ModelError("model_input_changed")

    @staticmethod
    def parents(sealed):
        manifest = json.loads(sealed.manifest_json)
        parents = {
            "derived:" + manifest[key]
            for key in (
                "question_content_revision",
                "question_certificate_revision",
                "question_instance_id",
            )
            if key in manifest
        }
        for item in manifest.get("inherited_generation_manifest", {}).get("inputs", ()):
            if item["kind"] in {"derived_content", "derived_certificate"}:
                parents.add("derived:" + item["id"])
            elif item["kind"] == "atom":
                parents.add("atom:" + item["id"])
        return sorted(parents)

    async def _authorization_rows(self, uow):
        """Reclaim only unused, expired capacity, never consumed audit evidence."""
        from ..operations.model_authorization_archive import _audit

        rows = await uow.derived_records(self.scope, "model_authorization")
        live = []
        for item in rows:
            row = item["payload"]
            if row.get("state") == "reserved":
                _audit(row, item["identity"])
                if datetime.fromisoformat(row["expires_at"]) <= self.clock():
                    await uow.model_delivery_reservation_delete(
                        self.scope, item["identity"], row["token"]
                    )
                    continue
            live.append(item)
        return live

    async def dispatch_capacity(self, uow, sealed):
        """Avoid new finance rows on a known-full audit; not a reservation."""
        if not callable(getattr(uow, "model_delivery_reservation_delete", None)):
            raise ModelError("model_delivery_reservation_backend_unsupported")
        await self.validate(uow, sealed)
        rows = await self._authorization_rows(uow)
        if len(rows) + 2 > 4096:
            raise ModelError("model_authorization_capacity")

    async def reserve_delivery(self, uow, sealed, *, call_id, token, expires_at):
        """Reserve the first delivery in the SAME transaction as dispatch."""
        # Recheck even if an earlier preflight succeeded. This placeholder counts
        # against the same physical cap as every cache hit and other delivery.
        await self.dispatch_capacity(uow, sealed)
        identity = uuid4().hex
        await uow.derived_put(
            self.scope,
            "model_authorization",
            identity,
            dict(
                schema="model-delivery-reservation/1",
                state="reserved",
                id=identity,
                stage="delivery",
                consumed=False,
                token=token,
                expires_at=expires_at.isoformat(),
                call_id=call_id,
                key=sealed.key,
                sources=list(json.loads(sealed.manifest_json)["sources"]),
                parents=self.parents(sealed),
            ),
        )
        return dict(id=identity, token=token)

    async def delivery_id(self, uow, sealed, *, call_id, reservation=None):
        """Choose the held slot for exactly one caller under the scope lock."""
        await self.validate(uow, sealed)
        if reservation is not None:
            row = await uow.derived_get(self.scope, "model_authorization", reservation["id"])
            if (
                row
                and row.get("state") == "reserved"
                and row.get("consumed") is False
                and row.get("token") == reservation["token"]
                and row.get("key") == sealed.key
                and row.get("call_id") == call_id
                and datetime.fromisoformat(row["expires_at"]) > self.clock()
            ):
                return reservation["id"], reservation["token"]
        return uuid4().hex, None

    async def retain_delivery(self, uow, sealed, *, call_id, reservation, expires_at):
        """Extend a live flight's slot through its atomically published cache TTL."""
        identity, token = await self.delivery_id(
            uow, sealed, call_id=call_id, reservation=reservation
        )
        if token is None:
            raise ModelError("model_delivery_reservation_fenced")
        row = await uow.derived_get(self.scope, "model_authorization", identity)
        row["expires_at"] = expires_at.isoformat()
        await uow.derived_put(self.scope, "model_authorization", identity, row)

    async def release_delivery(self, uow, reservation):
        if reservation is not None:
            await uow.lock_admission_scope(self.scope)
            await uow.model_delivery_reservation_delete(
                self.scope, reservation["id"], reservation["token"]
            )

    async def record(
        self,
        uow,
        sealed,
        stage,
        *,
        payload_sha256,
        call_id,
        authorization_id=None,
        reservation_token=None,
    ):
        if stage not in {"dispatch", "delivery"}:
            raise ModelError("invalid_model_authorization_stage")
        await self.validate(uow, sealed)
        key = authorization_id or uuid4().hex
        rows = await self._authorization_rows(uow)
        old = await uow.derived_get(self.scope, "model_authorization", key)
        if reservation_token is not None:
            if (
                stage != "delivery"
                or not old
                or old.get("state") != "reserved"
                or old.get("consumed") is not False
                or old.get("token") != reservation_token
                or old.get("key") != sealed.key
                or old.get("call_id") != call_id
            ):
                raise ModelError("model_delivery_reservation_fenced")
        elif old is not None:
            raise ModelError("model_authorization_conflict")
        elif len(rows) >= 4096:
            raise ModelError("model_authorization_capacity")
        # Audit identifiers are random; content, coordinates, IDs live only in
        # scrub-able metadata with source reverse associations.
        row = dict(
            id=key,
            stage=stage,
            payload_sha256=payload_sha256,
            call_id=call_id,
            sources=list(json.loads(sealed.manifest_json)["sources"]),
            parents=self.parents(sealed),
            key=sealed.key,
            authorized_at=self.clock().isoformat(),
            consumed=True,
        )
        await uow.derived_put(self.scope, "model_authorization", key, row)
        await self.validate(uow, sealed)
        return key
