"""Pinned local-only Transformers tokenization and actual final-token logits.

Loading never downloads weights or executes repository-provided Python code.
Provision artifacts separately; every load checks the full approved manifest.
"""

import asyncio
import json
import threading
from hashlib import sha256
from pathlib import Path

from agent_memory.retrieval.model_contracts import digest
from agent_memory.retrieval.pair_reranker import FinalTokenLogits, PairModelSpec

QWEN_PREFIX = (
    "<|im_start|>system\nJudge whether the Document meets the requirements based on the Query and "
    'the Instruct provided. Note that the answer can only be "yes" or "no".<|im_end|>\n'
    "<|im_start|>user\n"
)
QWEN_SUFFIX = "<|im_end|>\n<|im_start|>assistant\n<think>\n\n</think>\n\n"
MODEL_FILES = {"config.json", "tokenizer_config.json", "tokenizer.json"}


def artifact_manifest(directory):
    directory = Path(directory).resolve(strict=True)
    files = {}
    for path in sorted(directory.rglob("*")):
        if path.is_file() and path.suffix in {".json", ".safetensors", ".txt", ".jinja"}:
            # HuggingFace snapshots use symlinks into an immutable local cache.
            hasher = sha256()
            with path.open("rb") as stream:
                for block in iter(lambda: stream.read(1024 * 1024), b""):
                    hasher.update(block)
            files[path.relative_to(directory).as_posix()] = hasher.hexdigest()
    if not MODEL_FILES <= set(files) or not any(n.endswith(".safetensors") for n in files):
        raise ValueError("complete local tokenizer/config/weights required")
    return {"schema": "local-transformer-artifacts/1", "files": files}


class LocalQwenReranker:
    """Official Qwen3-Reranker yes/no logits with explicit no-truncation bounds."""

    def __init__(self, directory, *, expected_manifest_sha256, max_tokens=8192, device="cpu"):
        if type(max_tokens) is not int or not 64 <= max_tokens <= 32768:
            raise ValueError("invalid reranker context bound")
        if device not in {"cpu", "mps"}:
            raise ValueError("explicit CPU or MPS device required")
        self.directory = Path(directory).resolve(strict=True)
        self.manifest = artifact_manifest(self.directory)
        if digest(self.manifest) != expected_manifest_sha256:
            raise ValueError("local model artifacts do not match frozen manifest")
        import torch
        from transformers import AutoModelForCausalLM, AutoTokenizer

        self.tokenizer = AutoTokenizer.from_pretrained(
            self.directory, local_files_only=True, trust_remote_code=False, padding_side="left"
        )
        self.model = (
            AutoModelForCausalLM.from_pretrained(
                self.directory,
                local_files_only=True,
                trust_remote_code=False,
                torch_dtype=torch.float32,
            )
            .to(device)
            .eval()
        )
        if self.model.config.model_type != "qwen3":
            raise ValueError("only approved Qwen3 reranker architecture is supported")
        self.device, self.max_tokens, self.torch = device, max_tokens, torch
        yes = self.tokenizer.encode("yes", add_special_tokens=False)
        no = self.tokenizer.encode("no", add_special_tokens=False)
        if len(yes) != 1 or len(no) != 1 or yes == no:
            raise ValueError("exact distinct yes/no tokens required")
        self.spec = PairModelSpec(
            model="Qwen/Qwen3-Reranker-0.6B",
            model_sha256=expected_manifest_sha256,
            tokenizer_sha256=digest(
                {n: h for n, h in self.manifest["files"].items() if "tokenizer" in n}
            ),
            prompt_sha256=digest({"prefix": QWEN_PREFIX, "suffix": QWEN_SUFFIX}),
            runtime_sha256=digest(
                {
                    "transformers": __import__("transformers").__version__,
                    "torch": torch.__version__,
                    "device": device,
                    "max_tokens": max_tokens,
                    "max_batch_tokens": 16384,
                    "use_cache": False,
                    "threads": torch.get_num_threads(),
                }
            ),
            quantization_sha256=digest("float32"),
            yes_token_id=yes[0],
            no_token_id=no[0],
        )
        # This lock stays held by the inference thread even if the caller cancels.
        self._thread_lock = threading.Lock()
        self._lock = asyncio.Lock()
        self._inflight = None

    def _infer(self, query, candidates):
        with self._thread_lock:
            return self._infer_locked(query, candidates)

    def _infer_locked(self, query, candidates):
        if not candidates:
            return ()
        if len(candidates) > 16:
            raise ValueError("reranker_batch_capacity_exceeded")
        prompts = [
            "<Instruct>: Given a web search query, retrieve relevant "
            "passages that answer the query\n<Query>: " + query + "\n<Document>: " + item.text
            for item in candidates
        ]
        prefix = self.tokenizer.encode(QWEN_PREFIX, add_special_tokens=False)
        suffix = self.tokenizer.encode(QWEN_SUFFIX, add_special_tokens=False)
        sequences = [
            prefix + self.tokenizer.encode(prompt, add_special_tokens=False) + suffix
            for prompt in prompts
        ]
        if (
            any(len(ids) > self.max_tokens for ids in sequences)
            or len(sequences) * max(map(len, sequences)) > 16384
        ):
            raise ValueError("reranker_input_token_budget_exceeded")
        values = self.tokenizer.pad({"input_ids": sequences}, padding=True, return_tensors="pt")
        values = {key: value.to(self.device) for key, value in values.items()}
        with self.torch.inference_mode():
            logits = (
                self.model(**values, logits_to_keep=1, use_cache=False)
                .logits[:, -1, :]
                .float()
                .cpu()
            )
        return tuple(
            FinalTokenLogits(
                item.memory_id,
                float(row[self.spec.yes_token_id]),
                float(row[self.spec.no_token_id]),
            )
            for item, row in zip(candidates, logits, strict=True)
        )

    async def infer(self, spec, query, candidates):
        if spec != self.spec:
            raise ValueError("reranker_configuration_changed")
        async with self._lock:
            if self._inflight is not None and not self._inflight.done():
                raise ValueError("reranker_busy_after_cancelled_request")
            flight = asyncio.create_task(asyncio.to_thread(self._infer, query, candidates))
            self._inflight = flight

            def finished(task):
                if not task.cancelled():
                    task.exception()  # Retrieve failures even when its caller cancelled.
                if self._inflight is task:
                    self._inflight = None

            flight.add_done_callback(finished)
            result = await asyncio.shield(flight)
        if spec != self.spec:
            raise ValueError("reranker_configuration_changed")
        return result


class LocalChatTokenCounter:
    """Actual local tokenizer + chat template, requiring separate server approval.

    This measures the supplied template exactly. The host must additionally prove
    that the target server renders that same template before using TokenBudgetPort.
    Schema insertion/hidden server context is rejected rather than guessed.
    """

    def __init__(self, directory, *, expected_manifest_sha256):
        self.directory = Path(directory).resolve(strict=True)
        manifest = artifact_manifest(self.directory)
        if digest(manifest) != expected_manifest_sha256:
            raise ValueError("tokenizer artifacts changed")
        from transformers import AutoTokenizer

        self.tokenizer = AutoTokenizer.from_pretrained(
            self.directory, local_files_only=True, trust_remote_code=False
        )
        self.revision = digest({n: h for n, h in manifest["files"].items() if "tokenizer" in n})
        self.template_revision = digest(self.tokenizer.chat_template)

    def __call__(self, payload_json):
        payload = json.loads(payload_json)
        if payload.get("format"):
            raise ValueError("server schema rendering requires a separately approved renderer")
        ids = self.tokenizer.apply_chat_template(
            payload["messages"],
            tokenize=True,
            add_generation_prompt=True,
            enable_thinking=payload["think"],
        )
        return len(ids)


class LocalTransformerChat:
    """Exact local chat rendering and generation with no opaque server context.

    This optional port supports deterministic generation only. Declared schemas
    are rendered into a fixed instruction and counted, not silently added by a
    server. A schema still requires the governor's normal output validator.
    """

    RENDERER = "local-transformer-whole-chat/1"

    def __init__(self, directory, configuration, *, expected_manifest_sha256, device="cpu"):
        from agent_memory.context.model_input_budget import ModelInputBudget
        from agent_memory.retrieval.model_contracts import ModelError

        if (
            configuration.provider != "transformers-local"
            or configuration.adapter_revision != "local-transformers/1"
        ):
            raise ModelError("local_transformer_configuration_required")
        self.runtime = LocalQwenReranker(
            directory,
            expected_manifest_sha256=expected_manifest_sha256,
            max_tokens=32768,
            device=device,
        )
        self.configuration = configuration
        options = json.loads(configuration.options_json)
        if (
            set(options) != {"num_ctx", "num_predict", "temperature"}
            or options["temperature"] != 0
            or configuration.think is not False
            or configuration.model_revision != expected_manifest_sha256
            or configuration.tokenizer_revision != self.runtime.spec.tokenizer_sha256
            or configuration.runtime_manifest_sha256 != self.runtime.spec.runtime_sha256
        ):
            raise ModelError("local_transformer_binding_mismatch")
        self.budget = ModelInputBudget(
            configuration.model_revision,
            configuration.tokenizer_revision,
            digest([self.RENDERER, self.runtime.tokenizer.chat_template]),
            options["num_ctx"],
            options["num_predict"],
        )
        if (
            self.budget.fingerprint != configuration.overflow_guard_sha256
            or self.budget.context_tokens > self.runtime.model.config.max_position_embeddings
        ):
            raise ModelError("local_transformer_budget_mismatch")
        self._inflight = None
        self._configuration = configuration
        self._model, self._tokenizer = self.runtime.model, self.runtime.tokenizer

    def _tokens(self, payload_json):
        from agent_memory.retrieval.model_contracts import ModelError, canonical

        if (
            self.configuration != self._configuration
            or self.runtime.model is not self._model
            or self.runtime.tokenizer is not self._tokenizer
        ):
            raise ModelError("local_transformer_runtime_changed")
        payload = json.loads(payload_json)
        if (
            payload["model"] != self.configuration.model
            or payload["options"] != json.loads(self.configuration.options_json)
            or payload.get("think") is not False
            or payload.get("stream") is not False
            or payload.get("format", {}) != json.loads(self.configuration.output_schema_json)
        ):
            raise ModelError("local_transformer_payload_changed")
        messages = list(payload["messages"])
        if payload.get("format"):
            messages = [
                dict(
                    role="system",
                    content="Return JSON matching this schema: " + canonical(payload["format"]),
                )
            ] + messages
        return self._tokenizer.apply_chat_template(
            messages, tokenize=True, add_generation_prompt=True, enable_thinking=False
        )

    def count_payload(self, payload_json):
        return len(self._tokens(payload_json))

    def verify_binding(self, budget, configuration):
        return (
            budget == self.budget
            and configuration == self._configuration
            and self.configuration == configuration
            and self.runtime.model is self._model
            and self.runtime.tokenizer is self._tokenizer
        )

    def _generate(self, payload_json):
        from time import perf_counter_ns

        from agent_memory.retrieval.model_contracts import ModelError, ModelResponse

        tokens = self._tokens(payload_json)
        if len(tokens) + self.budget.output_tokens > self.budget.context_tokens:
            raise ModelError("model_input_token_budget_exceeded")
        torch = self.runtime.torch
        values = torch.tensor([tokens], device=self.runtime.device)
        started = perf_counter_ns()
        with torch.inference_mode():
            generated = self._model.generate(
                input_ids=values,
                attention_mask=torch.ones_like(values),
                do_sample=False,
                max_new_tokens=self.budget.output_tokens,
                pad_token_id=self._tokenizer.eos_token_id,
            )
        output = generated[0, len(tokens) :].tolist()
        text = self._tokenizer.decode(output, skip_special_tokens=True)
        return ModelResponse(text, len(tokens), len(output), perf_counter_ns() - started)

    async def generate(self, sealed):
        from agent_memory.retrieval.model_contracts import ModelError

        if sealed.configuration != self.configuration:
            raise ModelError("local_transformer_configuration_changed")
        if self._inflight is not None and not self._inflight.done():
            raise ModelError("local_transformer_busy_after_cancellation")
        flight = asyncio.create_task(asyncio.to_thread(self._generate, sealed.payload_json))
        self._inflight = flight

        def finished(task):
            if not task.cancelled():
                task.exception()
            if self._inflight is task:
                self._inflight = None

        flight.add_done_callback(finished)
        return await asyncio.shield(flight)
