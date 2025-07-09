/*
  Aplicação Node.js com Express, PostgreSQL, integração com dois Payment Processors e Docker
*/

const express = require('express');
const axios = require('axios');
const { Pool } = require('pg');

const app = express();
const PORT = process.env.PORT || 9999;

app.use(express.json());

// Configuração da conexão com o Postgres via env var
const pool = new Pool({ connectionString: process.env.DATABASE_URL });

// URLs dos Processadores
const DEFAULT_URL = process.env.PAYMENT_PROCESSOR_URL_DEFAULT;
const FALLBACK_URL = process.env.PAYMENT_PROCESSOR_URL_FALLBACK;

// Cache simples para health-check (1 chamada a cada 5 segundos)
let healthCache = {
  default: { timestamp: 0, data: null },
  fallback: { timestamp: 0, data: null }
};

async function getServiceHealth(name, url) {
  const now = Date.now();
  const cache = healthCache[name];
  if (cache.data && (now - cache.timestamp) < 5000) {
    return cache.data;
  }
  try {
    const resp = await axios.get(`${url}/payments/service-health`, { timeout: 2000 });
    cache.data = resp.data;
    cache.timestamp = now;
    return resp.data;
  } catch (err) {
    // se 429 ou erro, sinaliza service como failing
    return { failing: true, minResponseTime: Infinity };
  }
}

async function processPaymentExternal(payment) {
  const { correlationId, amount, requestedAt } = payment;

  // decide service
  const defaultHealth = await getServiceHealth('default', DEFAULT_URL);
  let target = DEFAULT_URL;
  if (defaultHealth.failing) {
    target = FALLBACK_URL;
  }

  try {
    const resp = await axios.post(
      `${target}/payments`,
      { correlationId, amount, requestedAt },
      { timeout: defaultHealth.minResponseTime + 1000 }
    );
    return { processor: target === DEFAULT_URL ? 'default' : 'fallback', success: true };
  } catch (err) {
    if (target === DEFAULT_URL) {
      // retry no fallback
      try {
        await axios.post(
          `${FALLBACK_URL}/payments`,
          { correlationId, amount, requestedAt },
          { timeout: 2000 }
        );
        return { processor: 'fallback', success: true };
      } catch (_) {
        return { processor: 'fallback', success: false };
      }
    }
    return { processor: 'fallback', success: false };
  }
}

/**
 * POST /payments
 * Recebe payment, armazena e encaminha para Payment Processor.
 */
app.post('/payments', async (req, res) => {
  const { correlationId, amount } = req.body;
  if (!correlationId || !amount) {
    return res.status(400).json({ error: 'correlationId e amount obrigatórios.' });
  }

  const requestedAt = new Date().toISOString();
  // armazena entrada
  try {
    await pool.query(
      `INSERT INTO payments(correlationId, amount, requested_at) VALUES($1, $2, $3)`,
      [correlationId, amount, requestedAt]
    );
  } catch (err) {
    if (err.code === '23505') return res.status(409).json({ error: 'correlationId duplicado.' });
    console.error(err);
    return res.status(500).json({ error: 'Erro ao salvar pagamento.' });
  }

  // processa externamente (async fire-and-forget)
  processPaymentExternal({ correlationId, amount, requestedAt })
    .then(result => console.log('Processed', correlationId, result))
    .catch(err => console.error('Error processing', correlationId, err));

  return res.status(202).json({ message: 'Pagamento recebido e será processado.' });
});

/**
 * GET /payments-summary
 * Agrega totals por default/fallback
 */
app.get('/payments-summary', async (req, res) => {
  const { from, to } = req.query;
  const params = [];
  const where = [];
  if (from) { where.push(`requested_at >= $${params.push(new Date(from))}`); }
  if (to)   { where.push(`requested_at <= $${params.push(new Date(to))}`); }
  const wc = where.length ? `WHERE ${where.join(' AND ')}` : '';

  try {
    const q = `
      SELECT processor_used, COUNT(*) AS cnt, SUM(amount) AS total_amount
      FROM payments
      ${wc}
      GROUP BY processor_used
    `;
    const { rows } = await pool.query(q, params);
    const summary = { default: { totalRequests:0, totalAmount:0 }, fallback: { totalRequests:0, totalAmount:0 } };
    rows.forEach(r => {
      summary[r.processor_used].totalRequests = parseInt(r.cnt,10);
      summary[r.processor_used].totalAmount = parseFloat(r.total_amount);
    });
    return res.json(summary);
  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: 'Erro ao gerar summary.' });
  }
});

app.listen(PORT, () => console.log(`API rodando na porta ${PORT}`));
