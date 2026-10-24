package ru.gamehub;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.lettuce.core.*;
import org.springframework.core.io.ClassPathResource;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

import static ru.gamehub.ApiModels.*;
import static ru.gamehub.RedisAccess.STREAM;

@Service
public class GameService {
    public static final String BOARD = "tournament:main";
    private final RedisAccess redis;
    private final ObjectMapper json;
    private final String loginScript;

    public GameService(RedisAccess redis, ObjectMapper json) throws IOException {
        this.redis = redis;
        this.json = json;
        loginScript = new ClassPathResource("redis/login.lua").getContentAsString(StandardCharsets.UTF_8);
    }

    public Player save(String id, PlayerInput input) {
        try (var connection = redis.dedicated()) {
            var c = connection.sync();
            c.multi();
            c.hsetnx(playerKey(id), "created_at", Instant.now().toString());
            c.hset(playerKey(id), fields(input));
            c.del(cacheKey(id));
            c.xadd(STREAM, event(id, "PROFILE_UPDATED", "Profile created or updated"));
            c.hgetall(playerKey(id));
            return player(id, checked(c.exec()).get(4));
        }
    }

    public CachedPlayer get(String id) {
        String cached = redis.write().get(cacheKey(id));
        if (cached != null) return new CachedPlayer(decode(cached), true);
        try (var connection = redis.dedicated()) {
            var c = connection.sync();
            for (int attempt = 0; attempt < 20; attempt++) {
                c.watch(playerKey(id));
                Player profile = player(id, c.hgetall(playerKey(id)));
                String value = encode(profile);
                c.multi();
                c.set(cacheKey(id), value, SetArgs.Builder.ex(60));
                TransactionResult result = c.exec();
                if (!result.wasDiscarded()) {
                    checked(result);
                    return new CachedPlayer(profile, false);
                }
            }
        }
        throw conflict();
    }

    public Player changeLevel(String id, long delta) {
        try (var connection = redis.dedicated()) {
            var c = connection.sync();
            for (int attempt = 0; attempt < 20; attempt++) {
                c.watch(playerKey(id));
                Player before = player(id, c.hgetall(playerKey(id)));
                long next = before.level() + delta;
                if (next < 0 || next > 1_000_000) throw badRequest("Level must be between 0 and 1000000");
                c.multi();
                c.hincrby(playerKey(id), "level", delta);
                c.del(cacheKey(id));
                c.xadd(STREAM, event(id, "LEVEL_CHANGED", "Level: " + before.level() + " -> " + next));
                c.hgetall(playerKey(id));
                TransactionResult result = c.exec();
                if (!result.wasDiscarded()) return player(id, checked(result).get(3));
            }
        }
        throw conflict();
    }

    public LoginResult login(String id) {
        requirePlayer(id);
        Long count = redis.write().eval(loginScript, ScriptOutputType.INTEGER,
                new String[]{"logins:" + id}, "86400");
        return new LoginResult(id, count, redis.write().ttl("logins:" + id));
    }

    public double addScore(ScoreInput input) {
        requirePlayer(input.playerId());
        if (!Double.isFinite(input.score())) throw badRequest("Score must be finite");
        return redis.write().zincrby(BOARD, input.score(), input.playerId());
    }

    public List<RankedPlayer> top(int limit, boolean consistent) {
        var values = redis.read(consistent).zrevrangeWithScores(BOARD, 0, limit - 1);
        List<RankedPlayer> result = new ArrayList<>();
        for (var value : values) {
            result.add(new RankedPlayer(value.getValue(), value.getScore(), result.size() + 1L));
        }
        return result;
    }

    public Ranking rank(String id, boolean consistent) {
        var c = redis.read(consistent);
        Long rank = c.zrank(BOARD, id);
        Long reverse = c.zrevrank(BOARD, id);
        if (rank == null || reverse == null) throw notFound("Player is not in the leaderboard");
        return new Ranking(id, rank, reverse + 1);
    }

    public boolean addAchievement(String id, String name) {
        requirePlayer(id);
        return redis.write().sadd("achievements:" + id, name) > 0;
    }

    public boolean hasAchievement(String id, String name, boolean consistent) {
        return redis.read(consistent).sismember("achievements:" + id, name);
    }

    public List<String> common(String first, String second, boolean consistent) {
        return redis.read(consistent).sinter("achievements:" + first, "achievements:" + second)
                .stream().sorted().toList();
    }

    public BatchResult batch(BatchInput input) {
        Set<String> ids = new HashSet<>();
        for (BatchPlayer entry : input.players()) {
            if (!ids.add(entry.id())) throw badRequest("Duplicate player ID in batch: " + entry.id());
        }
        long started = System.nanoTime();
        try (var connection = redis.dedicated()) {
            connection.setAutoFlushCommands(false);
            try {
                var c = connection.async();
                List<RedisFuture<TransactionResult>> futures = new ArrayList<>();
                for (BatchPlayer entry : input.players()) {
                    c.multi();
                    c.hsetnx(playerKey(entry.id()), "created_at", Instant.now().toString());
                    c.hset(playerKey(entry.id()), fields(entry.profile()));
                    c.del(cacheKey(entry.id()));
                    c.xadd(STREAM, event(entry.id(), "PROFILE_UPDATED", "Profile imported by pipeline"));
                    futures.add(c.exec());
                }
                connection.flushCommands();
                if (!LettuceFutures.awaitAll(15, TimeUnit.SECONDS, futures.toArray(new RedisFuture<?>[0]))) {
                    throw new RedisCommandTimeoutException("Pipeline timeout; batch may be partially applied");
                }
                for (var future : futures) checked(future.toCompletableFuture().join());
            } catch (CompletionException e) {
                throw new RedisException("Pipeline failed; batch may be partially applied", e.getCause());
            } finally {
                connection.setAutoFlushCommands(true);
            }
        }
        return new BatchResult(input.players().size(), (System.nanoTime() - started) / 1_000_000.0);
    }

    private void requirePlayer(String id) {
        if (redis.write().exists(playerKey(id)) == 0) throw notFound("Player not found: " + id);
    }

    private static Map<String, String> fields(PlayerInput input) {
        return Map.of("name", input.name(), "level", input.level().toString(), "region", input.region());
    }

    private static Map<String, String> event(String id, String type, String message) {
        return Map.of("player_id", id, "type", type, "message", message, "timestamp", Instant.now().toString());
    }

    private static Player player(String id, Map<String, String> hash) {
        if (hash.isEmpty()) throw notFound("Player not found: " + id);
        return new Player(id, hash.get("name"), Long.parseLong(hash.get("level")),
                hash.get("region"), hash.get("created_at"));
    }

    static TransactionResult checked(TransactionResult result) {
        if (result.wasDiscarded()) throw conflict();
        for (Object value : result) {
            if (value instanceof RedisException error) throw error;
        }
        return result;
    }

    private String encode(Player player) {
        try {
            return json.writeValueAsString(player);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Cannot serialize profile", e);
        }
    }

    private Player decode(String value) {
        try {
            return json.readValue(value, Player.class);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Invalid cached profile", e);
        }
    }

    private static String playerKey(String id) {
        return "player:" + id;
    }

    private static String cacheKey(String id) {
        return "cache:player:" + id;
    }

    private static ResponseStatusException conflict() {
        return new ResponseStatusException(HttpStatus.CONFLICT, "Concurrent profile updates; try again");
    }

    private static ResponseStatusException notFound(String message) {
        return new ResponseStatusException(HttpStatus.NOT_FOUND, message);
    }

    private static ResponseStatusException badRequest(String message) {
        return new ResponseStatusException(HttpStatus.BAD_REQUEST, message);
    }
}
