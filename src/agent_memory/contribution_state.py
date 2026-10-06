"""Shared contribution lifecycle transforms; no storage or authorization decisions."""

from copy import deepcopy
from datetime import datetime

from .domain import canonical_json


def enroll(rows):
    """Freeze identity-only predecessor barriers before removing any evidence."""
    for row in rows:
        payload = row["payload"]
        previous = payload.get("contribution", {})
        payload["contribution"] = {
            **previous,
            "schema": "contribution/1",
            "blocked_candidate_ids": sorted(
                set(previous.get("blocked_candidate_ids", ()))
                | {
                    other["id"]
                    for other in rows
                    if other["id"] != row["id"]
                    and datetime.fromisoformat(other["payload"]["valid_from"])
                    < datetime.fromisoformat(payload["valid_from"])
                    and canonical_json(other["payload"]["draft"]["value"])
                    != canonical_json(payload["draft"]["value"])
                }
            ),
        }


def erased_payload(payload):
    """Retain no value, quote, authority or source content after physical withdrawal."""
    contribution = payload.get("contribution")
    if not contribution:
        return {"deleted": True}
    return {
        "deleted": True,
        "action": "WITHDRAWN",
        "valid_from": payload["valid_from"],
        "contribution_barrier": deepcopy(contribution["blocked_candidate_ids"]),
    }


def scrub_transitions(payload, source_ids):
    """Loss of end evidence makes the boundary uncertain; it never restores continuity."""
    changed = False
    for transition in payload.get("transitions", []):
        for role in ("end_support", "start_support"):
            support = transition.get(role)
            if support and support.get("source_event_id") in source_ids:
                transition[role] = None
                transition[role + "_status"] = "unavailable"
                changed = True
        correction = transition.get("correction")
        if correction and correction["evidence"]["source_event_id"] in source_ids:
            transition["correction"] = None
            transition["correction_status"] = "unavailable"
            payload["valid_to"] = transition["valid_from"]
            payload["claim"] = {**payload["claim"], "valid_to": transition["valid_from"]}
            changed = True
    return changed


def withdrawal_barrier(payload):
    if "contribution_barrier" in payload:
        return payload["contribution_barrier"]
    if payload.get("action") == "WITHDRAWN" and payload.get("contribution"):
        return payload["contribution"]["blocked_candidate_ids"]
    return ()


def waived_barriers(payload):
    """Only a retained, explicit continuity correction may override a boundary guard."""
    return {
        identity
        for transition in payload.get("transitions", [])
        if transition.get("correction")
        for identity in transition["correction"]["barrier_ids"]
    }
