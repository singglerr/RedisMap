package ru.singglerr.kontur.intern.redis.map;

import org.junit.Assert;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;

import java.util.*;

/**
 * @author Danil Usov
 */
public class RedisMapTest {
    private static final HostAndPort SOCKET = new HostAndPort("localhost", 6379);
    private static final Jedis client = new Jedis(SOCKET);

    @Test
    public void baseTests() {
        Map<String, String> map1 = new RedisMap(client);
        Map<String, String> map2 = new RedisMap(client);

        map1.put("one", "1");

        map2.put("one", "ONE");
        map2.put("two", "TWO");

        Assert.assertEquals("1", map1.get("one"));
        Assert.assertEquals(1, map1.size());
        Assert.assertEquals(2, map2.size());

        map1.put("one", "first");

        Assert.assertEquals("first", map1.get("one"));
        Assert.assertEquals(1, map1.size());

        Assert.assertTrue(map1.containsKey("one"));
        Assert.assertFalse(map1.containsKey("two"));

        Set<String> keys2 = map2.keySet();
        Assert.assertEquals(2, keys2.size());
        Assert.assertTrue(keys2.contains("one"));
        Assert.assertTrue(keys2.contains("two"));

        Collection<String> values1 = map1.values();
        Assert.assertEquals(1, values1.size());
        Assert.assertTrue(values1.contains("first"));

        map1.clear();
        map2.clear();
    }

    @Test
    public void iterateCollections() {
        Map<String, String> map = new RedisMap(client);
        map.put("count", "1");
        map.put("lasdlas", "2");
        map.put("чxcvxcv", "3");
        map.put("qweqweqe", "4");

        map.entrySet().forEach(System.out::println);
        map.keySet().forEach(System.out::println);
        map.values().forEach(System.out::println);
        map.clear();
    }

    @Test
    public void testSets() {
        Map<String, String> map = new RedisMap(client);
        map.put("1", "one");
        map.put("2", "one");
        map.put("3", "zero");
        map.put("4", "four");

        Set<Map.Entry<String, String>> entries = map.entrySet();
        entries.remove(new Map.Entry<String, String>() {
            public String getKey() {
                return "3";
            }

            public String getValue() {
                return "zero";
            }

            public String setValue(String value) {
                return null;
            }
        });

        Assert.assertEquals(3, map.size());

        Set<String> keys = map.keySet();
        keys.remove("4");
        Assert.assertEquals(2, map.size());

        Collection<String> vals = map.values();
        vals.remove("one");
        Assert.assertEquals(1, map.size());

        map.clear();
        Assert.assertEquals(0, entries.size());
        Assert.assertEquals(0, keys.size());
        Assert.assertEquals(0, vals.size());
        Assert.assertEquals(0, map.size());
    }

    @Test
    public void sharedAccessTest() {
        Map<String, String> map1 = new RedisMap(client, "test");
        map1.put("one", "1");
        map1.put("two", "2");
        map1.put("three", "3");
        map1.put("four", "4");

        Map<String, String> map2 = new RedisMap(client, "test");
        Assert.assertEquals(4, map2.size());
        Assert.assertEquals("1", map2.get("one"));
        Assert.assertEquals("2", map2.get("two"));
        Assert.assertEquals("3", map2.get("three"));
        Assert.assertEquals("4", map2.get("four"));

        map2.put("one", "1000");
        map2.put("five", "5");
        map2.put("six", "6");

        Assert.assertEquals("1000", map1.get("one"));
        Assert.assertEquals("5", map1.get("five"));
        Assert.assertEquals("6", map1.get("six"));

        Assert.assertEquals(map1.size(), map2.size());

        map1.clear();
        Assert.assertEquals(0, map2.size());

        try {
            ((AutoCloseable) map1).close();
            ((AutoCloseable) map2).close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void gcCleanTest() {
        Map<String, String> map1 = new RedisMap(client, "test");
        map1.put("1", "one");
        map1.put("2", "two");
        map1.put("3", "three");
        map1.put("4", "four");

        map1 = null;
        System.gc();
        try {
            Thread.sleep(4000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        map1 = new RedisMap(client, "test");
        Assert.assertEquals(0, map1.size());
        map1.clear();
        try {
            ((AutoCloseable) map1).close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
