package ru.gamehub;

import io.lettuce.core.*;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.masterreplica.MasterReplica;
import io.lettuce.core.masterreplica.StatefulRedisMasterReplicaConnection;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
public class RedisAccess {
    public static final String STREAM = "notifications";
    public static final String GROUP = "notifications-group";
    private static final Logger log = LoggerFactory.getLogger(RedisAccess.class);
    private final RedisURI uri;
    private final RedisClient client;
    private StatefulRedisMasterReplicaConnection<String, String> master;
    private StatefulRedisMasterReplicaConnection<String, String> replicas;

    public RedisAccess(@Value("${app.redis.sentinel-uri}") String sentinelUri) {
        uri = RedisURI.create(sentinelUri);
        uri.setTimeout(Duration.ofSeconds(3));
        client = RedisClient.create(uri);
        client.setOptions(ClientOptions.builder()
                .autoReconnect(true)
                .disconnectedBehavior(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS)
                .socketOptions(SocketOptions.builder().connectTimeout(Duration.ofSeconds(2)).build())
                .timeoutOptions(TimeoutOptions.enabled(Duration.ofSeconds(3)))
                .build());
    }

    @PostConstruct
    public void initialize() throws InterruptedException {
        RedisException last = null;
        for (int attempt = 1; attempt <= 30; attempt++) {
            try {
                master = MasterReplica.connect(client, StringCodec.UTF8, uri);
                master.setReadFrom(ReadFrom.MASTER);
                replicas = MasterReplica.connect(client, StringCodec.UTF8, uri);
                replicas.setReadFrom(ReadFrom.REPLICA_PREFERRED);
                health();
                ensureGroup("$");
                log.info("Redis ready: Sentinel discovery, master writes, replica-preferred reads");
                return;
            } catch (RedisException e) {
                last = e;
                closeConnections();
                log.warn("Redis startup attempt {}/30: {}", attempt, e.getMessage());
                Thread.sleep(2000);
            }
        }
        client.shutdown();
        throw new IllegalStateException("Redis did not become ready", last);
    }

    public RedisCommands<String, String> write() {
        return master.sync();
    }

    public RedisCommands<String, String> read(boolean consistent) {
        return consistent ? master.sync() : replicas.sync();
    }

    public StatefulRedisConnection<String, String> dedicated() {
        return client.connect(StringCodec.UTF8, uri);
    }

    public String health() {
        String pong = write().ping();
        write().set("health:write", "ok", SetArgs.Builder.ex(10));
        return pong;
    }

    public void ensureGroup(String offset) {
        try {
            write().xgroupCreate(XReadArgs.StreamOffset.from(STREAM, offset), GROUP,
                    XGroupCreateArgs.Builder.mkstream());
        } catch (RedisCommandExecutionException e) {
            if (!e.getMessage().contains("BUSYGROUP")) throw e;
        }
    }

    private void closeConnections() {
        if (replicas != null) {
            replicas.close();
            replicas = null;
        }
        if (master != null) {
            master.close();
            master = null;
        }
    }

    @PreDestroy
    public void close() {
        closeConnections();
        client.shutdown();
    }
}
