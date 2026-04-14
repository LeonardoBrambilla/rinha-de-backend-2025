package org.acme;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import io.quarkus.cache.CacheInvalidateAll;
import io.quarkus.cache.CacheResult;
import io.quarkus.redis.client.RedisClient;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.pgclient.PgPool;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.Tuple;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.math.BigDecimal;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

@Path("")
public class PaymentResource {

    @Inject
    PgPool client;

    @Inject
    RedisClient redis;

    public String DEFAULT_URL = System.getenv("PAYMENT_PROCESSOR_URL_DEFAULT");
    public String FALLBACK_URL = System.getenv("PAYMENT_PROCESSOR_URL_FALLBACK");

    public static record PaymentRequest(UUID correlationId, BigDecimal amount) {}

    public static class HealthData {
        public boolean failing = false;
        public int minResponseTime = 100;
    }
    private static Map<String, Integer> failureCount = new ConcurrentHashMap<>();
    private static final int FAILURE_THRESHOLD = 3;
    static Map<String, HealthData> localHealth = new ConcurrentHashMap<>();
    static {
        localHealth.put("default", new HealthData());
        localHealth.put("fallback", new HealthData());
    }

    private HealthData cachedHealth(String name) {
        return localHealth.getOrDefault(name, new HealthData());
    }

    public static class Summary {
        @JsonProperty("default")
        public ProcessorSummary defaultProcessor = new ProcessorSummary();
        @JsonProperty("fallback")
        public ProcessorSummary fallback = new ProcessorSummary();
    }

    public static class ProcessorSummary {
        public long totalRequests = 0;
        public BigDecimal totalAmount = BigDecimal.ZERO;
    }

    // Substitua a definição atual:
    private static final ExecutorService BACKGROUND_EXECUTOR =
        Executors.newFixedThreadPool(
            Math.max(4, Runtime.getRuntime().availableProcessors())  // ← *1 em vez de *2
        );
    private static final ScheduledExecutorService HEALTH_EXECUTOR = Executors.newSingleThreadScheduledExecutor();

    private static final HttpClient HTTP = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(1))
        .build();

    private static final ObjectMapper mapper = new ObjectMapper()
        .registerModule(new JavaTimeModule())
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    // =========================================================================
    // POST /payments
    // =========================================================================
    @POST
    @Path("/payments")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Uni<Response> payments(PaymentRequest body) {
        if (body.correlationId == null || body.amount == null) {
            return Uni.createFrom().item(
                Response.status(400).entity("correlationId e amount obrigatórios.").build()
            );
        }

        Instant requestedAtInstant = Instant.now();
        OffsetDateTime requestedAtDb = requestedAtInstant.atOffset(ZoneOffset.UTC);

        HealthData defaultH = cachedHealth("default");
        HealthData fallbackH = cachedHealth("fallback");

        String targetUrl = !defaultH.failing ? DEFAULT_URL : (!fallbackH.failing ? FALLBACK_URL : null);
        String processor = !defaultH.failing ? "default" : (!fallbackH.failing ? "fallback" : "none");

        Uni<Void> logInsert = !"none".equals(processor)
            ? client.preparedQuery(
                "INSERT INTO payment_logs(correlationid, processor, success, processed_at) VALUES($1,$2,NULL,$3)"
            ).execute(Tuple.of(body.correlationId, processor, requestedAtDb)).replaceWithVoid()
            : Uni.createFrom().voidItem();

        return client.preparedQuery(
                "INSERT INTO payments(correlationId, amount, requested_at) VALUES($1,$2,$3)"
            ).execute(Tuple.of(body.correlationId, body.amount, requestedAtDb))
            .replaceWithVoid()
            .chain(() -> logInsert)
            .invoke(() -> {
                if (!"none".equals(processor)) {
                    BACKGROUND_EXECUTOR.submit(() ->
                        processPaymentExternal(body.correlationId, body.amount, requestedAtInstant, targetUrl, processor)
                            .subscribe().with(ignored -> {}, error -> System.err.println("ERRO background: " + error.getMessage()))
                    );
                }
            })
            .replaceWith(Response.status(202).entity("Pagamento recebido e será processado.").build())
            .onFailure().recoverWithItem(e -> {
                if (e.getMessage() != null && e.getMessage().contains("23505")) {
                    return Response.status(409).entity("correlationId duplicado.").build();
                }
                return Response.status(500).entity("Erro ao inserir pagamento.").build();
            });
    }

    // =========================================================================
    // 🔥 processPaymentExternal - VERSÃO SIMPLES QUE COMPILE
    // =========================================================================
    public Uni<Void> processPaymentExternal(UUID correlationId, BigDecimal amount, Instant requestedAt, String targetUrl, String processor) {
        // Serialização fora da chain
        final String body;
        try {
            body = mapper.writeValueAsString(Map.of("correlationId", correlationId, "amount", amount, "requestedAt", requestedAt));
        } catch (JsonProcessingException e) {
            return Uni.createFrom().failure(new RuntimeException("Erro ao serializar", e));
        }

        return callProcessor(targetUrl, body, correlationId, processor, amount, requestedAt);
    }

    // Helper: chama o processor (default ou fallback)
    private Uni<Void> callProcessor(String url, String body, UUID correlationId, String processor, BigDecimal amount, Instant requestedAt) {
        return sendHttpRequest(url + "/payments", body)
            .chain(response -> {
                int status = response.statusCode();

                // ✅ Sucesso: nada a fazer no cache
                if (status >= 200 && status < 300) {
                    return updateLog(correlationId, true,
                        processor.equals("fallback") ? "fallback" : null);
                }

                // ❌ Falha 5XX no default: marca como failing IMEDIATAMENTE
                if ("default".equals(processor) && status >= 500) {
                    int count = failureCount.merge("default", 1, Integer::sum);
                    if (count >= FAILURE_THRESHOLD) {
                        markFailingLocally("default");
                        failureCount.put("default", 0);  // Reset após marcar
                    }
                    // Tenta fallback...
                }

                // ❌ Outros erros
                if (status >= 400 && "fallback".equals(processor) && status >= 500) {
                    markFailing("fallback");
                }
                return updateLog(correlationId, false, null);
            });
    }

    private Uni<HttpResponse<String>> sendHttpRequest(String url, String body) {
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(url))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .timeout(Duration.ofSeconds(2))
            .build();

        return Uni.createFrom().completionStage(
                HTTP.sendAsync(request, HttpResponse.BodyHandlers.ofString())
            )
            .onFailure().retry().atMost(1);
    }
    // Helper: atualiza payment_logs
    private Uni<Void> updateLog(UUID correlationId, boolean success, String processor) {
        if (processor != null) {
            return client.preparedQuery("UPDATE payment_logs SET success=$2, processor=$3 WHERE correlationid=$1")
                .execute(Tuple.of(correlationId, success, processor)).replaceWithVoid();
        }
        return client.preparedQuery("UPDATE payment_logs SET success=$2 WHERE correlationid=$1")
            .execute(Tuple.of(correlationId, success)).replaceWithVoid();
    }

    // Helper: marca processor como failing
    private void markFailing(String name) {
        try {
            HealthData failing = new HealthData(); failing.failing = true;
            localHealth.put(name, failing);
            redis.setex("health:" + name, "6", mapper.writeValueAsString(failing));
        } catch (Exception ignored) {}
    }

    // =========================================================================
    // GET /payments-summary
    // =========================================================================
    @GET
    @Path("/payments-summary")
    @Produces(MediaType.APPLICATION_JSON)
    @CacheResult(cacheName = "payments-summary")
    public Uni<Response> paymentsSummary(@QueryParam("from") String from, @QueryParam("to") String to) {
        System.out.println("⚡ CACHE MISS - executando query");
        List<Object> params = new ArrayList<>();
        List<String> where = new ArrayList<>();

        if (from != null && !from.isEmpty()) { where.add("l.processed_at >= $" + (params.size() + 1)); params.add(parseDate(from)); }
        if (to != null && !to.isEmpty()) { where.add("l.processed_at <= $" + (params.size() + 1)); params.add(parseDate(to)); }

        String wc = where.isEmpty() ? "AND l.success IS NOT FALSE" : "AND l.success IS NOT FALSE AND " + String.join(" AND ", where);

        return client.preparedQuery("""
            SELECT l.processor, COUNT(*) AS totalrequests, COALESCE(SUM(p.amount),0) AS totalamount
            FROM payment_logs l JOIN payments p ON p.correlationid = l.correlationid
            WHERE l.processor != 'none' """ + wc + " GROUP BY l.processor")
            .execute(Tuple.from(params))
            .onItem().transform(rows -> {
                Summary summary = new Summary();
                for (Row row : rows) {
                    if ("default".equals(row.getString("processor"))) {
                        summary.defaultProcessor.totalRequests = row.getLong("totalrequests");
                        summary.defaultProcessor.totalAmount = row.getBigDecimal("totalamount");
                    } else {
                        summary.fallback.totalRequests = row.getLong("totalrequests");
                        summary.fallback.totalAmount = row.getBigDecimal("totalamount");
                    }
                }
                return Response.ok(summary).build();
            })
            .onFailure().recoverWithItem(e -> Response.status(500).entity(e.getMessage()).build());
    }

    @POST
    @Path("/purge-payments")
    @Produces(MediaType.APPLICATION_JSON)
    @CacheInvalidateAll(cacheName = "payments-summary")
    public Uni<Response> purgePayments() {
        // Executa em background, mas espera concluir antes de responder
        return Uni.createFrom().voidItem()
            .chain(() -> Uni.combine().all().unis(
                    client.preparedQuery("DELETE FROM payment_logs").execute(),
                    client.preparedQuery("DELETE FROM payments").execute()
                ).combinedWith((r1, r2) -> null))
            .replaceWith(Response.status(200).entity("All payments purged.").build())
            .onFailure().recoverWithItem(e ->
                Response.status(500).entity("erro: " + e.getMessage()).build());
    }

    // =========================================================================
    // Health Check
    // =========================================================================
    public HealthData getServiceHealth(String name, String url) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url + "/payments/service-health")).GET().timeout(Duration.ofSeconds(3)).build();
            HttpResponse<String> response = HTTP.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 429) return cachedHealth(name);
            HealthData data = mapper.readValue(response.body(), HealthData.class);
            redis.setex("health:" + name, "6", mapper.writeValueAsString(data));
            return data;
        } catch (Exception e) { return cachedHealth(name); }
    }

    private OffsetDateTime parseDate(String value) {
        try { return Instant.parse(value).atOffset(ZoneOffset.UTC); }
        catch (Exception e) { return LocalDateTime.parse(value).atOffset(ZoneOffset.UTC); }
    }

    @PostConstruct
    void startHealthRefresh() {
        // Apenas warm-up inicial (opcional)
        BACKGROUND_EXECUTOR.submit(() -> {
            getServiceHealth("default", DEFAULT_URL);
            getServiceHealth("fallback", FALLBACK_URL);
        });

        // ❌ Remova os polling periódicos - use falhas reais como gatilho
    }
    private void markFailingLocally(String name) {
        HealthData failing = new HealthData();
        failing.failing = true;
        localHealth.put(name, failing);
        // ❌ Sem Redis.setex - evita I/O desnecessário sob falha
    }
    @PreDestroy
    void stopHealthRefresh() { HEALTH_EXECUTOR.shutdownNow(); BACKGROUND_EXECUTOR.shutdownNow(); }
}
