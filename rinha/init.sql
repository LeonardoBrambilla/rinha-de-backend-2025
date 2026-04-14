
CREATE UNLOGGED TABLE payments (
    correlationId UUID PRIMARY KEY,
    amount DECIMAL NOT NULL,
    requested_at TIMESTAMP NOT NULL
);

CREATE INDEX payments_requested_at ON payments (requested_at);

CREATE UNLOGGED TABLE payment_logs (
  correlationId UUID,
  processor    TEXT,
  success      BOOLEAN,
  processed_at TIMESTAMP
);
CREATE INDEX payment_logs_corr ON payment_logs(correlationId);
CREATE INDEX payment_logs_proc ON payment_logs(processor);
