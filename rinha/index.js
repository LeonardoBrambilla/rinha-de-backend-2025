/*
  Aplicação Node.js com Express, PostgreSQL, integração com dois Payment Processors e Docker
  Usa tabela payments sem alterações e tabela auxiliar payment_logs para rastrear processor e status
*/

const express = require('express');
const axios = require('axios');
const { Pool } = require('pg');

const app = express();
const PORT = process.env.PORT || 9999;
app.use(express.json());

// Conexão Postgres via env var
const pool = new Pool({ connectionString: process.env.DATABASE_URL, max: 50 });

// URLs dos Processadores
const DEFAULT_URL = process.env.PAYMENT_PROCESSOR_URL_DEFAULT;
const FALLBACK_URL = process.env.PAYMENT_PROCESSOR_URL_FALLBACK;

// Cache simples para health-check (1 chamada a cada 5 segundos)
let healthCache = {
  default: { ts: 0, data: null },
  fallback: { ts: 0, data: null }
};

async function getServiceHealth(name, url) {
  const now = Date.now();
  const cache = healthCache[name];
  if (cache.data && now - cache.ts < 5000) return cache.data;
  try {
    const { data } = await axios.get(`${url}/payments/service-health`, { timeout: 2000 });
    cache.data = data;
    cache.ts = now;
    return data;
  } catch {
    return { failing: true, minResponseTime: Infinity };
  }
}

/**
 * Processa pagamento externo e registra em payment_logs'
 */
 async function processPaymentExternal({ correlationId, amount, requestedAt, targetUrl, processor }) {
   const health = processor === 'default' ?
     await getServiceHealth('default', DEFAULT_URL) :
     await getServiceHealth('fallback', FALLBACK_URL);

   try {
     await axios.post(
       `${targetUrl}/payments`,
       { correlationId, amount, requestedAt },
       { timeout: health.minResponseTime + 1000 }
     );
     // Atualiza para sucesso
     await pool.query(
       `UPDATE payment_logs SET success=true WHERE correlationid=$1`,
       [correlationId]
     );
   } catch {
     // Tenta fallback se default falhou
     if (processor === 'default') {
       const fallbackHealth = await getServiceHealth('fallback', FALLBACK_URL);
       if (!fallbackHealth.failing) {
         try {
           await axios.post(
             `${FALLBACK_URL}/payments`,
             { correlationId, amount, requestedAt },
             { timeout: fallbackHealth.minResponseTime + 1000 }
           );
           // Atualiza processor e sucesso
           await pool.query(
             `UPDATE payment_logs SET success=true, processor='fallback' WHERE correlationid=$1`,
             [correlationId]
           );
           return;
         } catch {}
       }
     }
     // Falha geral — marca como falhou
     await pool.query(
       `UPDATE payment_logs SET success=false WHERE correlationid=$1`,
       [correlationId]
     );
   }
 }

/**
 * POST /payments
 * Insere em payments e dispara processPaymentExternal
 */
 app.post('/payments', async (req, res) => {
   const { correlationId, amount } = req.body;
   if (!correlationId || !amount) {
     return res.status(400).json({ error: 'correlationId e amount obrigatórios.' });
   }
   const requestedAt = new Date().toISOString();

   // Escolhe processor antes de inserir
   const defaultHealth = await getServiceHealth('default', DEFAULT_URL);
   const fallbackHealth = await getServiceHealth('fallback', FALLBACK_URL);

   let targetUrl, processor;
   if (!defaultHealth.failing) {
     targetUrl = DEFAULT_URL;
     processor = 'default';
   } else if (!fallbackHealth.failing) {
     targetUrl = FALLBACK_URL;
     processor = 'fallback';
   } else {
     processor = 'none';
   }

   try {
     await pool.query(
       `INSERT INTO payments(correlationId, amount, requested_at) VALUES($1,$2,$3)`,
       [correlationId, amount, requestedAt]
     );

     // Insere log IMEDIATAMENTE com success=null (pendente)
     if (processor !== 'none') {
       await pool.query(
         `INSERT INTO payment_logs(correlationid, processor, success, processed_at)
          VALUES($1,$2,NULL,NOW())`,
         [correlationId, processor]
       );
     }
   } catch (err) {
     if (err.code === '23505') {
       return res.status(409).json({ error: 'correlationId duplicado.' });
     }
     console.error(err);
     return res.status(500).json({ error: 'Erro ao inserir pagamento.' });
   }

   // Dispara processamento em background (atualiza success depois)
   if (processor !== 'none') {
     processPaymentExternal({ correlationId, amount, requestedAt, targetUrl, processor })
       .catch(e => console.error('Erro proc ext:', e));
   }

   return res.status(202).json({ message: 'Pagamento recebido e será processado.' });
 });

/**
 * GET /payments-summary
 * Agrupa logs por processor
 */
 app.get('/payments-summary', async (req, res) => {
   const { from, to } = req.query;
   const params = [];
   const conditions = [`l.processor != 'none'`];

   if (from) {
     params.push(new Date(from));
     conditions.push(`l.processed_at >= $${params.length}`);
   }

   if (to) {
     params.push(new Date(to));
     conditions.push(`l.processed_at <= $${params.length}`);
   }

   const whereClause = conditions.length ? `WHERE ${conditions.join(' AND ')}` : '';

   try {
     const { rows } = await pool.query(
       `
       SELECT l.processor,
              COUNT(*) AS totalrequests,
              COALESCE(SUM(p.amount), 0) AS totalamount
       FROM payment_logs l
       JOIN payments p ON p.correlationid = l.correlationid
       ${whereClause}
       GROUP BY l.processor
       `,
       params
     );

     const summary = {
       default: { totalRequests: 0, totalAmount: 0 },
       fallback: { totalRequests: 0, totalAmount: 0 }
     };

     rows.forEach(r => {
       summary[r.processor] = {
         totalRequests: parseInt(r.totalrequests, 10),
         totalAmount: parseFloat(r.totalamount)
       };
     });

     return res.json(summary);
   } catch (err) {
     console.error(err);
     return res.status(500).json({ error: 'Erro ao gerar summary.' });
   }
 });

app.post('/purge-payments', async (req, res) => {
  try {
    await pool.query('DELETE FROM payment_logs');
    await pool.query('DELETE FROM payments');
    res.status(200).json({ message: 'All payments purged.' });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

app.listen(PORT, () => console.log(`API rodando na porta ${PORT}`));
