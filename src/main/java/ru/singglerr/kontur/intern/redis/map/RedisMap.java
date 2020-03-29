package ru.singglerr.kontur.intern.redis.map;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanResult;

import java.lang.ref.Cleaner;
import java.util.*;

/**
 * @author Danil Usov
 */
public class RedisMap implements Map<String, String>, AutoCloseable {
    private static final Cleaner cleaner = Cleaner.create();
    private static final String CLIENTS_TAG = "clients";

    private Jedis client;
    private UUID uuid;
    private String redisHKey;
    private Cleaner.Cleanable cleanable;

    class RedisEntry implements Entry<String, String> {
        private String key;

        RedisEntry(String key) {
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

        @Override
        public String toString() {
            return getKey() + "=" + getValue();
        }
    }

    public RedisMap() {
        client = new Jedis();
        init(null);
    }

    public RedisMap(Jedis client) {
        this.client = client;
        init(null);
    }

    public RedisMap(String sharedName) {
        client = new Jedis();
        init(sharedName);
    }

    public RedisMap(Jedis client, String sharedName) {
        this.client = client;
        init(sharedName);
    }

    public RedisMap(String host, int port) {
        client = new Jedis(host, port);
        init(null);
    }

    public RedisMap(String host, int port, String sharedName) {
        client = new Jedis(host, port);
        init(sharedName);
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
        return values().contains(value);
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
    @SuppressWarnings("unchecked")
    public void putAll(Map<? extends String, ? extends String> m) {
        client.hmset(redisHKey, (Map<String, String>) m);
    }

    @Override
    public void clear() {
        client.del(redisHKey);
    }

    @Override
    public Set<String> keySet() {
        return new KeySet();
    }

    final class KeySet extends AbstractSet<String> {
        @Override
        public Iterator<String> iterator() {
            return new KeyRedisIterator();
        }

        @Override
        public int size() {
            return RedisMap.this.size();
        }

        @Override
        public void clear() {
            RedisMap.this.clear();
        }

        @Override
        public boolean remove(Object o) {
            return RedisMap.this.remove(o) != null;
        }

        @Override
        public boolean contains(Object o) {
            return RedisMap.this.containsKey(o);
        }
    }

    @Override
    public Collection<String> values() {
        return new Values();
    }

    final class Values extends AbstractCollection<String> {
        @Override
        public Iterator<String> iterator() {
            return new ValueRedisIterator();
        }

        @Override
        public int size() {
            return RedisMap.this.size();
        }

        @Override
        public void clear() {
            RedisMap.this.clear();
        }
    }

    @Override
    public void close() throws Exception {
        cleanable.clean();
    }

    static class RedisCleaner implements Runnable {
        private boolean isShared;
        private String redisHKey;
        private Jedis client;
        private UUID uuid;

        RedisCleaner(boolean isShared, String redisHKey, Jedis client, UUID uuid) {
            this.isShared = isShared;
            this.redisHKey = redisHKey;
            this.client = client;
            this.uuid = uuid;
        }

        @Override
        public void run() {
            if (isShared) {
                client.hdel(CLIENTS_TAG + "-" + redisHKey, uuid.toString());
                if (client.hlen(CLIENTS_TAG + "-" + redisHKey) == 0) {
                    client.del(CLIENTS_TAG + "-" + redisHKey);
                    client.del(redisHKey);
                }
            } else {
                client.del(redisHKey);
            }
        }
    }

    @Override
    public Set<Entry<String, String>> entrySet() {
        return new RedisEntrySet();
    }

    final class RedisEntrySet extends AbstractSet<Entry<String, String>> {
        @Override
        public int size() {
            return RedisMap.this.size();
        }

        @Override
        public final boolean contains(Object o) {
            if (!(o instanceof Entry))
                return false;

            Entry<?, ?> e = (Entry<?, ?>) o;
            String val = RedisMap.this.get(e.getKey());
            return val != null && val.equals(e.getValue());
        }

        @Override
        public Iterator<Entry<String, String>> iterator() {
            return new EntryRedisIterator();
        }

        @Override
        public boolean remove(Object o) {
            if (!(o instanceof Entry))
                return false;

            Map.Entry<?, ?> e = (Map.Entry<?, ?>) o;
            return RedisMap.this.remove(e.getKey()) != null;
        }

        @Override
        public void clear() {
            RedisMap.this.clear();
        }
    }

    abstract class RedisIterator {
        private String cursor = "0";
        private ScanResult<Entry<String, String>> result;
        private Iterator<Entry<String, String>> iteratorOfCurrentResult;
        private Entry<String, String> currentEntry;

        RedisIterator() {
            scan();
        }

        final Entry<String, String> nextEntry() {
            if (!iteratorOfCurrentResult.hasNext()) {
                scan();
            }

            Entry<String, String> next = iteratorOfCurrentResult.next();
            currentEntry = new RedisEntry(next.getKey());
            return currentEntry;
        }

        public boolean hasNext() {
            return !result.isCompleteIteration() || iteratorOfCurrentResult.hasNext();
        }

        public void remove() {
            if (currentEntry == null) {
                throw new IllegalStateException();
            }

            RedisMap.this.remove(currentEntry.getKey());
        }

        private void scan() {
            result = client.hscan(redisHKey, cursor);
            cursor = result.getCursor();
            iteratorOfCurrentResult = result.getResult().iterator();
        }
    }

    final class KeyRedisIterator extends RedisIterator implements Iterator<String> {
        @Override
        public String next() {
            return nextEntry().getKey();
        }
    }

    final class ValueRedisIterator extends RedisIterator implements Iterator<String> {
        @Override
        public String next() {
            return nextEntry().getValue();
        }
    }

    final class EntryRedisIterator extends RedisIterator implements Iterator<Entry<String, String>> {
        @Override
        public Entry<String, String> next() {
            return nextEntry();
        }
    }

    @Override
    protected void finalize() throws Throwable {
        close();
        super.finalize();
    }

    private void init(String sharedName) {
        uuid = UUID.randomUUID();
        Runnable cleanAction;
        if (sharedName != null) {
            redisHKey = sharedName;
            client.hset(CLIENTS_TAG + "-" + redisHKey, uuid.toString(), "1");
            cleanAction = new RedisCleaner(true, redisHKey, client, uuid);
        } else {
            redisHKey = uuid.toString();
            cleanAction = new RedisCleaner(false, redisHKey, client, uuid);
        }

        cleanable = cleaner.register(this, cleanAction);
    }
}
