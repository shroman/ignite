/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.rest.protocols.tcp.redis;

import java.util.HashMap;
import java.util.List;
import org.junit.Assert;
import redis.clients.jedis.Jedis;

/**
 * Tests for HASH commands of Redis protocol.
 */
public class RedisProtocolHashSelfTest extends RedisCommonAbstractTest {
    private static final String HKEY = "hash1";

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        jcache(0, "redis-ignite-internal-cache-hash1").clear();

        assertTrue(jcache(0, "redis-ignite-internal-cache-hash1").localSize() == 0);
    }

    /**
     * @throws Exception If failed.
     */
    public void testHSet() throws Exception {
        final String field = "f1";

        try (Jedis jedis = pool.getResource()) {
            Assert.assertEquals(1L, (long)jedis.hset(HKEY, field, "v1"));
            Assert.assertEquals(0L, (long)jedis.hset(HKEY, field, "v1"));

            Assert.assertNull(jcache().get(field));
            Assert.assertEquals("v1",
                jcache(0, "redis-ignite-internal-cache-hash1").get(field));

            Assert.assertEquals(0L, (long)jedis.hset(HKEY, field, "v2"));
            Assert.assertEquals("v2",
                jcache(0, "redis-ignite-internal-cache-hash1").get(field));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testHGet() throws Exception {
        final String field = "f1";

        try (Jedis jedis = pool.getResource()) {
            jedis.hset(HKEY, field, "v1");

            Assert.assertEquals("v1", jedis.hget(HKEY, field));
            Assert.assertNull(jedis.hget(HKEY, "f2"));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testHMSet() throws Exception {
        try (Jedis jedis = pool.getResource()) {
            Assert.assertEquals("OK", jedis.hmset(HKEY, new HashMap<String, String>() {{
                put("f1", "v1");
                put("f2", "v2");
                put("f3", "v3");
            }}));

            for (int i = 1; i <= 3; i++) {
                Assert.assertNull(jcache().get("f" + i));
                Assert.assertEquals("v" + i,
                    jcache(0, "redis-ignite-internal-cache-hash1").get("f" + i));
            }
        }
    }

    /**
     * Note: getAll() does not return null values, so no nil values are returned by hmget().
     *
     * @throws Exception If failed.
     */
    public void testHMGet() throws Exception {
        try (Jedis jedis = pool.getResource()) {
            Assert.assertEquals("OK", jedis.hmset(HKEY, new HashMap<String, String>() {{
                put("f1", "v1");
                put("f2", "v2");
                put("f3", "v3");
            }}));

            List<String> res = jedis.hmget(HKEY, "f1", "f2", "f3", "f_null");
            for (int i = 1; i <= 3; i++) {
                Assert.assertTrue(res.contains("v" + i));
            }
        }
    }

    /**
     * Note: {@link Jedis#hdel(String, String...)} is supposed to respond with the number of actually deleted fields.
     *
     * @throws Exception If failed.
     */
    public void testHDel() throws Exception {
        try (Jedis jedis = pool.getResource()) {
            Assert.assertEquals("OK", jedis.hmset(HKEY, new HashMap<String, String>() {{
                put("f1", "v1");
                put("f2", "v2");
                put("f3", "v3");
            }}));

            Assert.assertEquals(2L, (long)jedis.hdel(HKEY, "f1", "f2"));
            Assert.assertEquals(null, jedis.hget(HKEY, "f2"));
            Assert.assertEquals("v3", jedis.hget(HKEY, "f3"));

            // different from Redis protocol.
            Assert.assertEquals(2L, (long)jedis.hdel(HKEY, "f3", "f4"));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testHExists() throws Exception {
        try (Jedis jedis = pool.getResource()) {
            jedis.hset(HKEY, "f1", "v1");

            Assert.assertTrue(jedis.hexists(HKEY, "f1"));
            Assert.assertFalse(jedis.hexists(HKEY, "f2"));
        }
    }
}
