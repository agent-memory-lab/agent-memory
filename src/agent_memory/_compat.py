"""Compatibility for the former flat module layout.

Register aliases to the real module objects, not copied exports: monkeypatches,
class identity and old pickle references must resolve through the same module.
Keep this table frozen; new code imports the owning subsystem directly.
"""
import sys
from importlib import import_module
from types import ModuleType

LEGACY_MODULES = {
    "benchmark_harness": "evaluation.benchmark",
    "bundle_packer": "retrieval.bundle",
    "candidate_diversity": "retrieval.diversity",
    "candidate_fusion": "retrieval.fusion",
    "candidate_guard": "retrieval.guard",
    "capture_api": "capture.api",
    "capture_artifacts": "capture.artifacts",
    "capture_policy": "capture.policy",
    "capture_queue": "capture.queue",
    "capture_sink": "capture.sink",
    "claim_consolidation": "consolidation.claims",
    "comparison_adapters": "evaluation.comparison",
    "compression_feedback": "context.compression_feedback",
    "compression_strategy": "context.compression_strategy",
    "context_compression": "context.compression",
    "deletion_audit": "operations.deletion_audit",
    "entity_retriever": "retrieval.entity",
    "episode_segmentation": "consolidation.episodes",
    "governed_recall": "retrieval.governed",
    "hybrid_candidate_plugin": "retrieval.hybrid",
    "lexical_plugin": "retrieval.lexical_plugin",
    "lexical_retrieval": "retrieval.lexical",
    "memory_doctor": "operations.doctor",
    "memory_evaluation": "evaluation.memory",
    "model_token_counter": "context.model_token_counter",
    "ontology_acceptance": "ontology.acceptance",
    "ontology_api": "ontology.api",
    "ontology_backfill": "ontology.backfill",
    "ontology_checkpoint": "ontology.checkpoint",
    "ontology_config": "ontology.config",
    "ontology_delta": "ontology.delta",
    "ontology_layers": "ontology.layers",
    "ontology_live": "ontology.live",
    "ontology_memory": "ontology.memory",
    "ontology_plugin": "ontology.plugin",
    "ontology_queries": "ontology.queries",
    "ontology_readiness": "ontology.readiness",
    "ontology_registry": "ontology.registry",
    "ontology_rule_candidates": "ontology.rule_candidates",
    "ontology_rules": "ontology.rules",
    "ontology_runtime": "ontology.runtime",
    "ontology_schema": "ontology.schema",
    "ontology_source": "ontology.source",
    "ontology_upgrade": "ontology.upgrade",
    "ontology_workspace": "ontology.workspace",
    "parallel_retrieval": "retrieval.parallel",
    "plugin_loader": "extensions.loader",
    "plugin_protocol": "extensions.protocol",
    "plugin_testing": "extensions.testing",
    "plugins": "extensions.registry",
    "procedure_induction": "consolidation.procedures",
    "recovery": "context.recovery",
    "recovery_operations": "context.operations",
    "recovery_partitions": "context.partitions",
    "recovery_store": "context.store",
    "recovery_transport": "context.transport",
    "reference_plugins": "extensions.reference",
    "release_acceptance": "evaluation.release",
    "resource_evaluation": "evaluation.resources",
    "retrieval_evaluation": "evaluation.retrieval",
    "retriever_plugin": "retrieval.retriever_plugin",
    "scoped_lexical_retrieval": "retrieval.scoped_lexical",
    "semantic_retriever": "retrieval.semantic",
    "snapshot_replay": "evaluation.replay",
    "sqlite_evidence_source": "retrieval.sqlite_source",
    "sqlite_worker_queue": "operations.sqlite_worker_queue",
    "temporal_retriever": "retrieval.temporal",
    "worker_runtime": "operations.worker_runtime",
    "worker_tasks": "operations.worker_tasks",
}


def install_legacy_aliases(package: ModuleType) -> None:
    for old_name, target in LEGACY_MODULES.items():
        module = import_module(f".{target}", package.__name__)
        sys.modules[f"{package.__name__}.{old_name}"] = module
        setattr(package, old_name, module)
