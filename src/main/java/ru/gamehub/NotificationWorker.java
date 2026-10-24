package ru.gamehub;

import io.lettuce.core.Consumer;
import io.lettuce.core.RedisException;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.XTrimArgs;
import io.lettuce.core.api.sync.RedisCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;

import static ru.gamehub.RedisAccess.GROUP;
import static ru.gamehub.RedisAccess.STREAM;

@Component
public class NotificationWorker {
    private static final Logger log = LoggerFactory.getLogger(NotificationWorker.class);
    private static final String CONSUMER = "gamehub-consumer";
    private final RedisAccess redis;

    public NotificationWorker(RedisAccess redis) {
        this.redis = redis;
    }

    @Scheduled(fixedDelay = 500)
    public void consume() {
        try {
            var c = redis.write();
            process(c, "0");
            process(c, ">");
        } catch (RedisException e) {
            log.warn("Notification processing postponed: {}", e.getMessage());
            if (e.getMessage() != null && e.getMessage().contains("NOGROUP")) redis.ensureGroup("0-0");
        }
    }

    private void process(RedisCommands<String, String> c, String offset) {
        var messages = c.xreadgroup(Consumer.from(GROUP, CONSUMER), XReadArgs.Builder.count(50),
                XReadArgs.StreamOffset.from(STREAM, offset));
        for (var message : messages) {
            log.info("NOTIFICATION id={} body={}", message.getId(), message.getBody());
            c.xack(STREAM, GROUP, message.getId());
            log.info("ACK id={}", message.getId());
        }
    }

    @Scheduled(fixedDelay = 60_000)
    public void trim() {
        try {
            trimOlderThanSevenDays(redis.write(), STREAM, Instant.now());
        } catch (RedisException e) {
            log.warn("Stream cleanup postponed: {}", e.getMessage());
        }
    }

    static long trimOlderThanSevenDays(RedisCommands<String, String> c, String stream, Instant now) {
        String cutoff = now.minus(Duration.ofDays(7)).toEpochMilli() + "-0";
        return c.xtrim(stream, XTrimArgs.Builder.minId(cutoff).exactTrimming());
    }
}