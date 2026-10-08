CREATE TABLE IF NOT EXISTS agent_memory_model_budget_entries (
    kind TEXT NOT NULL CHECK(kind IN ('account','call','receipt')),
    identity TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    PRIMARY KEY(kind, identity)
);
