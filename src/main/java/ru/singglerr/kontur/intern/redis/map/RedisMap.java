package ru.singglerr.kontur.intern.redis.map;

import redis.clients.jedis.Jedis;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;

/**
 * @author Danil Usov
 */
public class RedisMap implements Map<String, String>, AutoCloseable {
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
        client.del(redisHKey);
    }

    @Override
    public Set<String> keySet() {
        return client.hkeys(redisHKey);
    }

    @Override
    public Collection<String> values() {
        return client.hvals(redisHKey);
    }

    @Override
    public Set<Entry<String, String>> entrySet() {
        Set<Entry<String, String>> set = new HashSet<>();
        for (String key : keySet()) {
            set.add(new RedisEntry(key));
        }

        return new RedisEntrySet(set);
    }

    @Override
    public void close() throws Exception {
        clear();
        client.close();
    }

    public class RedisEntry implements Entry<String, String> {
        private String key;

        public RedisEntry(String key) {
            this.key = key;
        }

        @Override
        public String getKey() {
            return key;
        }

        @Override
        public String getValue() {
            return client.hget(redisHKey, key);
        }

        @Override
        public String setValue(String value) {
            String old = getValue();
            client.hset(redisHKey, key, value);
            return old;
        }
    }

    private final class RedisEntrySet implements Set<Entry<String, String>> {
        Set<Entry<String, String>> set;

        public RedisEntrySet(Set<Entry<String, String>> set) {
            this.set = set;
        }

        @Override
        public int size() {
            return set.size();
        }

        @Override
        public boolean isEmpty() {
            return set.isEmpty();
        }

        @Override
        public boolean contains(Object o) {
            return set.contains(o);
        }

        @Override
        public Iterator<Entry<String, String>> iterator() {
            return set.iterator();
        }

        @Override
        public Object[] toArray() {
            return set.toArray();
        }

        @Override
        public <T> T[] toArray(T[] a) {
            return set.toArray(a);
        }

        @Override
        public boolean add(Entry<String, String> entry) {
            return set.add(entry);
        }

        @Override
        public boolean remove(Object o) {
            return set.remove(o);
        }

        @Override
        public boolean containsAll(Collection<?> c) {
            return set.containsAll(c);
        }

        @Override
        public boolean addAll(Collection<? extends Entry<String, String>> c) {
            return set.addAll(c);
        }

        @Override
        public boolean retainAll(Collection<?> c) {
            boolean res = set.retainAll(c);
            if (!res) {
                return false;
            }

            Map<String, String> map = new HashMap<>();
            for (Entry<String, String> entry : set) {
                map.put(entry.getKey(), entry.getValue());
            }

            RedisMap.this.clear();
            RedisMap.this.putAll(map);

            return true;
        }

        @Override
        public boolean removeAll(Collection<?> c) {
            return set.removeAll(c);
        }

        @Override
        public void clear() {
            set.clear();
            RedisMap.this.clear();
        }
    }
}
