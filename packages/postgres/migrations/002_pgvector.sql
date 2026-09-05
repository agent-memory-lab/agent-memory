-- Optional migration executed by PgVectorIndex.initialize(dimensions).
-- The dimension is deployment-specific and is inserted only after strict integer validation.
CREATE EXTENSION IF NOT EXISTS vector;
