CREATE TABLE IF NOT EXISTS environments (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    type TEXT NOT NULL CHECK (type IN ('k8s', 'vm', 'bm')),
    status TEXT NOT NULL CHECK (status IN ('CREATING', 'ACTIVE', 'DELETING', 'DELETED')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_environments_status ON environments(status);
CREATE INDEX IF NOT EXISTS idx_environments_type ON environments(type);
