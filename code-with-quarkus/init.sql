
CREATE UNLOGGED TABLE payments (
    correlationid UUID PRIMARY KEY,
    amount DECIMAL NOT NULL,
    requested_at TIMESTAMPTZ NOT NULL
);

CREATE UNLOGGED TABLE payment_logs (
  correlationId UUID,
  processor    TEXT,
  success      BOOLEAN,
  processed_at TIMESTAMP
);

-- Índice para o JOIN no endpoint /payments-summary (CRÍTICO)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_payment_logs_summary
ON payment_logs(processor, success, processed_at)
INCLUDE (correlationid);

-- Índice para lookup por correlationId (INSERTs e UPDATEs)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_payment_logs_correlation
ON payment_logs(correlationid);

-- Índice para a tabela payments (JOIN)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_payments_correlation
ON payments(correlationid);

-- Atualiza estatísticas para o query planner
ANALYZE payment_logs;
ANALYZE payments;

-- Execute no seu init.sql ou manualmente:
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_payment_logs_for_delete
ON payment_logs(success, processed_at);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_payments_for_delete
ON payments(requested_at);

-- Atualiza estatísticas para o planner
ANALYZE payment_logs;
ANALYZE payments;
