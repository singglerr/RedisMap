package ru.singglerr.kontur.intern.redis.map;

import org.junit.Assert;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;

import java.util.*;

/**
 * @author Danil Usov
 */
public class RedisMapTest {
    public static final HostAndPort SOCKET = new HostAndPort("192.168.8.171", 6379);

    @Test
    public void baseTests() {
        Map<String, String> map1 = new RedisMap(SOCKET.getHost(), SOCKET.getPort());
        Map<String, String> map2 = new RedisMap(SOCKET.getHost(), SOCKET.getPort());

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
    }

    @Test
    public void test() {
        Map<String, String> map = new HashMap<>();
        map.put("a", "1");
        System.out.println(map.toString());
        Set<Map.Entry<String, String>> entries = map.entrySet();
        for (Map.Entry<String, String> entry : entries) {
            System.out.println(entry.toString());
            entry.setValue("sfsdfsdfsdf");
        }
        System.out.println(map.toString());

        List<String> list = new ArrayList<>();
        list.add("a");
        map.entrySet().removeAll(list);
        System.out.println(map.toString());
    }
}
