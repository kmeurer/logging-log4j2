package org.apache.logging.log4j.redis.appender;

import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractManager;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.io.ByteArrayOutputStream;
import java.nio.charset.Charset;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

final class RedisManager extends AbstractManager {

    byte[][] byteKeys;
    String host;
    int port;
    Charset charset;
    JedisPool jedisPool;

    protected RedisManager(LoggerContext loggerContext, String name, String[] keys, String host, int port, Charset charset) {
        super(loggerContext, name);
        this.byteKeys = new byte[keys.length][];
        for (int i = 0; i < keys.length; i++) {
            this.byteKeys[i] = keys[i].getBytes(charset);
        }
        this.charset = charset;
        this.host = host;
        this.port = port;
    }

    private static JedisPool createPool(String host, int port, boolean ssl){
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxIdle(5);
        poolConfig.setMinIdle(1);
        poolConfig.setTestOnBorrow(true);
        poolConfig.setTestOnReturn(true);
        poolConfig.setTestWhileIdle(true);
        poolConfig.setNumTestsPerEvictionRun(10);
        poolConfig.setTimeBetweenEvictionRunsMillis(60000);
        return new JedisPool(poolConfig, host, port, ssl);
    }

    public void startup() {
        jedisPool = createPool(host, port, false);
    }

    public void send(byte[] value) {
        runInPool(jedis -> {
            for (byte[] key: byteKeys) {
                jedis.rpush(key, value);
            }
            return null;
        }, ex -> {
            LOGGER.error("Unable to send audit log message to Redis. Ensure that it's running on {} : {}", host, port, ex);
            return null;
        });
    }

    private <T> Optional<T> runInPool(Function<Jedis, T> sendFunction, Function<Exception, Void> onError) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return Optional.ofNullable(sendFunction.apply(jedis));
        } catch (JedisConnectionException e) {
            onError.apply(e);
            return Optional.empty();
        } finally {
            if (jedis != null) {
                jedis.resetState();
            }
        }
    }

    @Override
    protected boolean releaseSub(final long timeout, final TimeUnit timeUnit) {
        if (jedisPool != null) {
            jedisPool.destroy();
        }
        return true;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getKeysAsString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < byteKeys.length; i++) {
            sb.append(new String(byteKeys[i], charset));
            if (i != byteKeys.length - 1) {
                sb.append(", ");
            }
        }
        return sb.toString();
    }
}
