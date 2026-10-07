package ru.gamehub;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.lettuce.core.*;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.protocol.ProtocolVersion;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;
import static ru.gamehub.RedisAccess.GROUP;
import static ru.gamehub.RedisAccess.STREAM;

class GameHubIntegrationTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final HttpClient HTTP = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    private static final String BASE = System.getenv().getOrDefault("BASE_URL", "http://localhost:8080");
    private static final Set<String> IDS = ConcurrentHashMap.newKeySet();
    private static RedisClient client;
    private static StatefulRedisConnection<String, String> connection;

    @BeforeAll
    static void connect() throws Exception {
        assertEquals(200, request("GET", "/api/health", null).statusCode());
        String uri = System.getenv().getOrDefault("SENTINEL_URI",
                "redis-sentinel://sentinel-1:26379,sentinel-2:26380,sentinel-3:26381/0#mymaster");
        RedisURI redisUri = RedisURI.create(uri);
        redisUri.setTimeout(Duration.ofSeconds(5));
        client = RedisClient.create(redisUri);
        client.setOptions(ClientOptions.builder().protocolVersion(ProtocolVersion.RESP2).build());
        connection = client.connect();
    }

    @AfterAll
    static void cleanup() {
        if (connection != null) {
            for (String id : IDS) {
                connection.sync().del("player:" + id, "cache:player:" + id, "logins:" + id, "achievements:" + id);
                connection.sync().zrem(GameService.BOARD, id);
            }
            connection.close();
        }
        if (client != null) client.shutdown();
    }

    @Test
    void profileCacheAndInvalidation() throws Exception {
        String id = id();
        JsonNode created = ok("POST", "/api/players/" + id, profile("Alice", 3));
        assertEquals(-1L, connection.sync().ttl("player:" + id));
        var first = request("GET", "/api/players/" + id, null);
        assertEquals(200, first.statusCode());
        assertEquals("MISS", first.headers().firstValue("X-Cache").orElseThrow());
        var second = request("GET", "/api/players/" + id, null);
        assertEquals("HIT", second.headers().firstValue("X-Cache").orElseThrow());
        long ttl = connection.sync().ttl("cache:player:" + id);
        assertTrue(ttl > 0 && ttl <= 60);
        JsonNode changed = ok("PATCH", "/api/players/" + id + "/level", Map.of("delta", 2));
        assertEquals(5, changed.get("level").asInt());
        assertNull(connection.sync().get("cache:player:" + id));
        assertEquals(5, ok("GET", "/api/players/" + id, null).get("level").asInt());
        JsonNode replaced = ok("POST", "/api/players/" + id, profile("Alice renamed", 8));
        assertEquals(created.get("created_at"), replaced.get("created_at"));
        assertNull(connection.sync().get("cache:player:" + id));
    }

    @Test
    void concurrentLoginsAreNotLostAndTtlDoesNotSlide() throws Exception {
        String id = create("Login", 1);
        assertEquals(1, ok("POST", "/api/players/" + id + "/login", null).get("count").asInt());
        long before = connection.sync().pttl("logins:" + id);
        Thread.sleep(1100);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<Integer>> calls = new ArrayList<>();
            for (int i = 0; i < 30; i++) {
                calls.add(executor.submit(() -> request("POST", "/api/players/" + id + "/login", null).statusCode()));
            }
            for (Future<Integer> call : calls) assertEquals(200, call.get(30, TimeUnit.SECONDS).intValue());
        }
        assertEquals("31", connection.sync().get("logins:" + id));
        long after = connection.sync().pttl("logins:" + id);
        assertTrue(after > 0 && after < before - 900);
        connection.sync().pexpire("logins:" + id, 100);
        await(() -> connection.sync().exists("logins:" + id) == 0);
        assertEquals(1, ok("POST", "/api/players/" + id + "/login", null).get("count").asInt());
    }

    @Test
    void concurrentLevelChangesAreNotLost() throws Exception {
        String id = create("Levels", 0);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<Integer>> calls = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                calls.add(executor.submit(() -> request("PATCH", "/api/players/" + id + "/level",
                        Map.of("delta", 1)).statusCode()));
            }
            for (Future<Integer> call : calls) assertEquals(200, call.get(30, TimeUnit.SECONDS).intValue());
        }
        assertEquals("10", connection.sync().hget("player:" + id, "level"));
    }

    @Test
    void leaderboardUsesIncrementsAndBothRankConventions() throws Exception {
        String first = create("First", 1);
        String second = create("Second", 1);
        ok("POST", "/api/leaderboard/score", Map.of("playerId", first, "score", 100));
        JsonNode updated = ok("POST", "/api/leaderboard/score", Map.of("playerId", first, "score", 50));
        assertEquals(150.0, updated.get("score").asDouble());
        ok("POST", "/api/leaderboard/score", Map.of("playerId", second, "score", 90));
        JsonNode rank = ok("GET", "/api/leaderboard/rank/" + first + "?consistent=true", null);
        assertEquals(connection.sync().zrank(GameService.BOARD, first).longValue(), rank.get("rank").asLong());
        assertEquals(connection.sync().zrevrank(GameService.BOARD, first) + 1, rank.get("position").asLong());
        JsonNode top = ok("GET", "/api/leaderboard/top?limit=10&consistent=true", null);
        var expected = connection.sync().zrevrangeWithScores(GameService.BOARD, 0, 9);
        assertEquals(expected.size(), top.size());
        for (int i = 0; i < top.size(); i++) {
            assertEquals(expected.get(i).getValue(), top.get(i).get("playerId").asText());
            assertEquals(i + 1, top.get(i).get("position").asInt());
        }
        await(() -> {
            try {
                return ok("GET", "/api/leaderboard/rank/" + first, null).get("position").asLong()
                        == rank.get("position").asLong();
            } catch (Exception | AssertionError e) {
                return false;
            }
        });
    }

    @Test
    void achievementsAreUniqueAndIntersect() throws Exception {
        String first = create("Achievements A", 1);
        String second = create("Achievements B", 1);
        String path = "/api/players/" + first + "/achievements";
        assertTrue(ok("POST", path, Map.of("name", "winner")).get("added").asBoolean());
        assertFalse(ok("POST", path, Map.of("name", "winner")).get("added").asBoolean());
        ok("POST", path, Map.of("name", "explorer"));
        ok("POST", "/api/players/" + second + "/achievements", Map.of("name", "winner"));
        JsonNode common = ok("GET", path + "/common/" + second + "?consistent=true", null);
        assertEquals(JSON.readTree("[\"winner\"]"), common);
        assertTrue(ok("GET", path + "/winner?consistent=true", null).get("exists").asBoolean());
        assertFalse(ok("GET", path + "/missing?consistent=true", null).get("exists").asBoolean());
    }

    @Test
    void pipelineImportsTwentyProfilesAndInvalidatesExistingCache() throws Exception {
        String old = create("Old", 1);
        String createdAt = ok("GET", "/api/players/" + old, null).get("created_at").asText();
        List<Map<String, Object>> entries = new ArrayList<>();
        entries.add(Map.of("id", old, "profile", profile("Imported", 7)));
        for (int i = 1; i < 20; i++) entries.add(Map.of("id", id(), "profile", profile("Batch " + i, i)));
        JsonNode result = ok("POST", "/api/players/batch", Map.of("players", entries));
        assertEquals(20, result.get("count").asInt());
        assertTrue(result.get("elapsedMs").asDouble() > 0);
        for (var entry : entries) assertEquals(4, connection.sync().hgetall("player:" + entry.get("id")).size());
        assertNull(connection.sync().get("cache:player:" + old));
        assertEquals(createdAt, connection.sync().hget("player:" + old, "created_at"));
        System.out.println("PIPELINE: " + result);
    }

    @Test
    void notificationsAreDeliveredAndAcknowledged() throws Exception {
        String id = create("Notifications", 1);
        ok("PATCH", "/api/players/" + id + "/level", Map.of("delta", 1));
        var events = connection.sync().xrange(STREAM, Range.unbounded()).stream()
                .filter(event -> id.equals(event.getBody().get("player_id"))).toList();
        assertTrue(events.size() >= 2);
        assertTrue(events.stream().anyMatch(event -> "LEVEL_CHANGED".equals(event.getBody().get("type"))));
        String target = events.getLast().getId();
        await(() -> {
            for (Object group : connection.sync().xinfoGroups(STREAM)) {
                List<?> pairs = (List<?>) group;
                Map<String, String> info = new HashMap<>();
                for (int i = 0; i < pairs.size(); i += 2)
                    info.put(pairs.get(i).toString(), String.valueOf(pairs.get(i + 1)));
                if (GROUP.equals(info.get("name"))) {
                    return compareIds(info.get("last-delivered-id"), target) >= 0
                            && connection.sync().xpending(STREAM, GROUP, Range.create(target, target), Limit.from(1)).isEmpty();
                }
            }
            return false;
        });
        assertTrue(connection.sync().xlen(STREAM) > 0);
    }

    @Test
    void retentionDeletesOldEntriesButKeepsFreshOnes() {
        String stream = "test:retention:" + UUID.randomUUID();
        try {
            Instant now = Instant.now();
            String old = now.minus(Duration.ofDays(8)).toEpochMilli() + "-0";
            connection.sync().xadd(stream, new XAddArgs().id(old), Map.of("message", "old"));
            String fresh = connection.sync().xadd(stream, Map.of("message", "fresh"));
            assertEquals(1, NotificationWorker.trimOlderThanSevenDays(connection.sync(), stream, now));
            assertEquals(fresh, connection.sync().xrange(stream, Range.unbounded()).getFirst().getId());
        } finally {
            connection.sync().del(stream);
        }
    }

    @Test
    void validationAndMissingPlayersDoNotCreateBrokenHashes() throws Exception {
        String missing = id();
        assertEquals(404, request("PATCH", "/api/players/" + missing + "/level", Map.of("delta", 1)).statusCode());
        assertEquals(0L, connection.sync().exists("player:" + missing));
        assertEquals(400, request("POST", "/api/players/" + missing, profile("", -1)).statusCode());
        assertEquals(400, request("GET", "/api/leaderboard/top?limit=0", null).statusCode());
        String id = create("Validation", 1);
        assertEquals(400, request("PATCH", "/api/players/" + id + "/level", Map.of("delta", -2)).statusCode());
        assertEquals("1", connection.sync().hget("player:" + id, "level"));
    }

    private static String id() {
        String id = "test-" + UUID.randomUUID();
        IDS.add(id);
        return id;
    }

    private static String create(String name, int level) throws Exception {
        String id = id();
        ok("POST", "/api/players/" + id, profile(name, level));
        return id;
    }

    private static Map<String, Object> profile(String name, int level) {
        return Map.of("name", name, "level", level, "region", "EU");
    }

    private static HttpResponse<String> request(String method, String path, Object body) throws Exception {
        var publisher = body == null ? HttpRequest.BodyPublishers.noBody()
                : HttpRequest.BodyPublishers.ofString(JSON.writeValueAsString(body));
        var request = HttpRequest.newBuilder(URI.create(BASE + path)).timeout(Duration.ofSeconds(20))
                .header("Content-Type", "application/json").method(method, publisher).build();
        return HTTP.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private static JsonNode ok(String method, String path, Object body) throws Exception {
        var response = request(method, path, body);
        assertEquals(200, response.statusCode(), response.body());
        return JSON.readTree(response.body());
    }

    private static void await(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        do {
            if (condition.getAsBoolean()) return;
            Thread.sleep(100);
        } while (System.nanoTime() < deadline);
        fail("Condition was not satisfied within 15 seconds");
    }

    private static int compareIds(String left, String right) {
        String[] a = left.split("-");
        String[] b = right.split("-");
        int time = Long.compare(Long.parseLong(a[0]), Long.parseLong(b[0]));
        return time != 0 ? time : Long.compare(Long.parseLong(a[1]), Long.parseLong(b[1]));
    }
}
