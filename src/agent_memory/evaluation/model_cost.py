"""Project minimal runtime finance evidence into the existing B0 evaluation ledger.

This projection never labels local execution free and never supplies unobserved
phases, quality judgments, dataset licensing, or confidence intervals. Callers
must add measured setup/write/dependency/drain costs and finite pending work.
"""

from .question_cost import CostEntry, CostPhase, PendingResponsibility, TokenBasis


def runtime_model_costs(records, *, request_bindings=None):
    """Return (entries, pending) counted once per call, not once per account.

    ``request_bindings`` is the experiment's frozen attribution manifest, keyed
    by call_id. Runtime finance records deliberately contain no user request IDs.
    Missing bindings use B0's explicit all-requests allocation rule.
    """
    request_bindings = request_bindings or {}
    entries, pending, seen = [], [], set()
    for row in records:
        call_id = row["call_id"]
        if call_id in seen:
            raise ValueError("duplicate runtime model call")
        seen.add(call_id)
        state = row["state"]
        requests = tuple(request_bindings.get(call_id, ()))
        if state == "released":
            continue  # No dispatch, no fabricated model call.
        if state == "reserved":
            pending.append(
                PendingResponsibility(
                    call_id, requests, row["maximum_microunits"], CostPhase(row["phase"])
                )
            )
            continue
        if state not in {"dispatch_intent", "reconciliation_pending", "settled"}:
            raise ValueError("unsupported runtime model cost state")
        measured = row["input_tokens"] is not None and row["output_tokens"] is not None
        configuration = row.get("configuration_sha256")
        entries.append(
            CostEntry(
                operation_id=call_id,
                phase=CostPhase.FAILURE if row["outcome"] == "failed" else CostPhase(row["phase"]),
                request_ids=requests,
                model_call=True,
                provider=row["provider"],
                provider_request_id=row["provider_request_sha256"],
                provider_billed_microunits=row["actual_microunits"],
                billing_reference=(
                    "receipt-sha256:" + row["receipt_sha256"] if row["receipt_sha256"] else None
                ),
                reservation_microunits=row["maximum_microunits"],
                tokens=row["input_tokens"] + row["output_tokens"] if measured else None,
                token_basis=TokenBasis.MEASURED if measured else TokenBasis.UNKNOWN,
                model_role=row["model_role"] if configuration else None,
                model_configuration_sha256=configuration,
            )
        )
    return tuple(entries), tuple(pending)
