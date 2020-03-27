package ru.singglerr.kontur.intern.redis.map;

import redis.clients.jedis.Jedis;

import java.util.*;

/**
 * @author Danil Usov
 */
public class RedisMap implements Map<String, String> {
    private Jedis client;
    private UUID uuid;
    private String redisHKey;
    private boolean isShared;

    public RedisMap() {
        client = new Jedis();
        uuid = UUID.randomUUID();
        redisHKey = uuid.toString();
    }

    public RedisMap(String sharedName) {
        client = new Jedis();
        uuid = UUID.randomUUID();
        redisHKey = sharedName;
        isShared = true;
    }

    public RedisMap(String host, int port) {
        client = new Jedis(host, port);
        uuid = UUID.randomUUID();
        redisHKey = uuid.toString();
    }

    public RedisMap(String host, int port, String sharedName) {
        client = new Jedis(host, port);
        uuid = UUID.randomUUID();
        redisHKey = sharedName;
        isShared = true;
    }

    @Override
    public int size() {
        return client.hlen(redisHKey).intValue();
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean containsKey(Object key) {
        return client.hexists(redisHKey, key.toString());
    }

    @Override
    public boolean containsValue(Object value) {
        return client.hvals(redisHKey).contains(value);
    }

    @Override
    public String get(Object key) {
          return client.hget(redisHKey, key.toString());
    }

    @Override
    public String put(String key, String value) {
        String old = client.hget(redisHKey, key);
        client.hset(redisHKey, key, value);
        return old;
    }

    @Override
    public String remove(Object key) {
        String val = client.hget(redisHKey, key.toString());
        client.hdel(redisHKey, key.toString());
        return val;
    }

    @Override
    public void putAll(Map<? extends String, ? extends String> m) {
        client.hmset(redisHKey, (Map<String, String>) m);
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<String> keySet() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Collection<String> values() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Entry<String, String>> entrySet() {
        throw new UnsupportedOperationException();
    }
}
