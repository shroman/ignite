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

package org.apache.ignite.internal.processors.cache;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.integration.CacheLoaderException;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionOptimisticException;
import org.jsr166.ConcurrentHashMap8;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 *
 */
public class CacheSerializableTransactionsTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final boolean FAST = true;

    /** */
    private static Map<Integer, Integer> storeMap = new ConcurrentHashMap8<>();

    /** */
    private static final int SRVS = 4;

    /** */
    private static final int CLIENTS = 3;

    /** */
    private boolean client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        cfg.setClientMode(client);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(SRVS);

        client = true;

        startGridsMultiThreaded(SRVS, CLIENTS);

        client = false;
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 5 * 60_000;
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxLoadFromStore() throws Exception {
        Ignite ignite0 = ignite(0);

        final IgniteTransactions txs = ignite0.transactions();

        for (CacheConfiguration<Integer, Integer> ccfg : cacheConfigurations()) {
            if (ccfg.getCacheStoreFactory() == null)
                continue;

            logCacheInfo(ccfg);

            try {
                IgniteCache<Integer, Integer> cache = ignite0.createCache(ccfg);

                List<Integer> keys = testKeys(cache);

                for (Integer key : keys) {
                    log.info("Test key: " + key);

                    Integer storeVal = -1;

                    storeMap.put(key, storeVal);

                    try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                        Integer val = cache.get(key);

                        assertEquals(storeVal, val);

                        tx.commit();
                    }

                    checkValue(key, storeVal, cache.getName());
                }
            }
            finally {
                destroyCache(ignite0, ccfg.getName());
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxCommitReadOnly1() throws Exception {
        Ignite ignite0 = ignite(0);

        final IgniteTransactions txs = ignite0.transactions();

        for (CacheConfiguration<Integer, Integer> ccfg : cacheConfigurations()) {
            logCacheInfo(ccfg);

            try {
                IgniteCache<Integer, Integer> cache = ignite0.createCache(ccfg);

                List<Integer> keys = testKeys(cache);

                for (Integer key : keys) {
                    log.info("Test key: " + key);

                    try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                        Integer val = cache.get(key);

                        assertNull(val);

                        tx.commit();
                    }

                    checkValue(key, null, cache.getName());

                    try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                        Integer val = cache.get(key);

                        assertNull(val);

                        tx.rollback();
                    }

                    checkValue(key, null, cache.getName());

                    cache.put(key, 1);

                    cache.remove(key);

                    try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                        Integer val = cache.get(key);

                        assertNull(val);

                        tx.commit();
                    }
                }
            }
            finally {
                destroyCache(ignite0, ccfg.getName());
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxCommitReadOnly2() throws Exception {
        Ignite ignite0 = ignite(0);

        final IgniteTransactions txs = ignite0.transactions();

        for (CacheConfiguration<Integer, Integer> ccfg : cacheConfigurations()) {
            logCacheInfo(ccfg);

            try {
                IgniteCache<Integer, Integer> cache = ignite0.createCache(ccfg);

                List<Integer> keys = testKeys(cache);

                for (final Integer key : keys) {
                    log.info("Test key: " + key);

                    try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                        Integer val = cache.get(key);

                        assertNull(val);

                        txAsync(cache, OPTIMISTIC, SERIALIZABLE,
                            new IgniteClosure<IgniteCache<Integer, Integer>, Void>() {
                                @Override
                                public Void apply(IgniteCache<Integer, Integer> cache) {
                                    cache.get(key);

                                    return null;
                                }
                            }
                        );

                        tx.commit();
                    }

                    checkValue(key, null, cache.getName());

                    try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                        Integer val = cache.get(key);

                        assertNull(val);

                        txAsync(cache, PESSIMISTIC, REPEATABLE_READ,
                            new IgniteClosure<IgniteCache<Integer, Integer>, Void>() {
                                @Override
                                public Void apply(IgniteCache<Integer, Integer> cache) {
                                    cache.get(key);

                                    return null;
                                }
                            }
                        );

                        tx.commit();
                    }

                    checkValue(key, null, cache.getName());
                }
            }
            finally {
                destroyCache(ignite0, ccfg.getName());
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxCommitReadOnlyGetAll() throws Exception {
        Ignite ignite0 = ignite(0);

        final IgniteTransactions txs = ignite0.transactions();

        for (CacheConfiguration<Integer, Integer> ccfg : cacheConfigurations()) {
            logCacheInfo(ccfg);

            try {
                IgniteCache<Integer, Integer> cache = ignite0.createCache(ccfg);

                Set<Integer> keys = new HashSet<>();

                for (int i = 0; i < 100; i++)
                    keys.add(i);

                try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                    Map<Integer, Integer> map = cache.getAll(keys);

                    assertTrue(map.isEmpty());

                    tx.commit();
                }

                for (Integer key : keys)
                    checkValue(key, null, cache.getName());

                try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                    Map<Integer, Integer> map = cache.getAll(keys);

                    assertTrue(map.isEmpty());

                    tx.rollback();
                }

                for (Integer key : keys)
                    checkValue(key, null, cache.getName());
            }
            finally {
                destroyCache(ignite0, ccfg.getName());
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxConflictRead1() throws Exception {
        txConflictRead(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxConflictRead2() throws Exception {
        txConflictRead(false);
    }

    /**
     * @param noVal If {@code true} there is no cache value when read in tx.
     * @throws Exception If failed.
     */
    private void txConflictRead(boolean noVal) throws Exception {
        Ignite ignite0 = ignite(0);

        final IgniteTransactions txs = ignite0.transactions();

        for (CacheConfiguration<Integer, Integer> ccfg : cacheConfigurations()) {
            logCacheInfo(ccfg);

            try {
                IgniteCache<Integer, Integer> cache = ignite0.createCache(ccfg);

                List<Integer> keys = testKeys(cache);

                for (Integer key : keys) {
                    log.info("Test key: " + key);

                    Integer expVal = null;

                    if (!noVal) {
                        expVal = -1;

                        cache.put(key, expVal);
                    }

                    try {
                        try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                            Integer val = cache.get(key);

                            assertEquals(expVal, val);

                            updateKey(cache, key, 1);

                            tx.commit();
                        }

                        fail();
                    }
                    catch (TransactionOptimisticException e) {
                        log.info("Expected exception: " + e);
                    }

                    checkValue(key, 1, cache.getName());

                    try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                        Object val = cache.get(key);

                        assertEquals(1, val);

                        tx.commit();
                    }

                    checkValue(key, 1, cache.getName());
                }
            }
            finally {
                destroyCache(ignite0, ccfg.getName());
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxConflictReadWrite1() throws Exception {
        txConflictReadWrite(true, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxConflictReadWrite2() throws Exception {
        txConflictReadWrite(false, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxConflictReadRemove1() throws Exception {
        txConflictReadWrite(true, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxConflictReadRemove2() throws Exception {
        txConflictReadWrite(false, true);
    }

    /**
     * @param noVal If {@code true} there is no cache value when read in tx.
     * @param rmv If {@code true} tests remove, otherwise put.
     * @throws Exception If failed.
     */
    private void txConflictReadWrite(boolean noVal, boolean rmv) throws Exception {
        Ignite ignite0 = ignite(0);

        final IgniteTransactions txs = ignite0.transactions();

        for (CacheConfiguration<Integer, Integer> ccfg : cacheConfigurations()) {
            logCacheInfo(ccfg);

            try {
                IgniteCache<Integer, Integer> cache = ignite0.createCache(ccfg);

                List<Integer> keys = testKeys(cache);

                for (Integer key : keys) {
                    log.info("Test key: " + key);

                    Integer expVal = null;

                    if (!noVal) {
                        expVal = -1;

                        cache.put(key, expVal);
                    }

                    try {
                        try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                            Integer val = cache.get(key);

                            assertEquals(expVal, val);

                            updateKey(cache, key, 1);

                            if (rmv)
                                cache.remove(key);
                            else
                                cache.put(key, 2);

                            tx.commit();
                        }

                        fail();
                    }
                    catch (TransactionOptimisticException e) {
                        log.info("Expected exception: " + e);
                    }

                    checkValue(key, 1, cache.getName());

                    try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                        Integer val = cache.get(key);

                        assertEquals(1, (Object) val);

                        if (rmv)
                            cache.remove(key);
                        else
                            cache.put(key, 2);

                        tx.commit();
                    }

                    checkValue(key, rmv ? null : 2, cache.getName());
                }
            }
            finally {
                destroyCache(ignite0, ccfg.getName());
            }
        }
    }

    /**
     * @throws Exception If failed
     */
    public void testTxConflictGetAndPut1() throws Exception {
        txConflictGetAndPut(true, false);
    }

    /**
     * @throws Exception If failed
     */
    public void testTxConflictGetAndPut2() throws Exception {
        txConflictGetAndPut(false, false);
    }

    /**
     * @throws Exception If failed
     */
    public void testTxConflictGetAndRemove1() throws Exception {
        txConflictGetAndPut(true, true);
    }

    /**
     * @throws Exception If failed
     */
    public void testTxConflictGetAndRemove2() throws Exception {
        txConflictGetAndPut(false, true);
    }

    /**
     * @param noVal If {@code true} there is no cache value when read in tx.
     * @param rmv If {@code true} tests remove, otherwise put.
     * @throws Exception If failed.
     */
    private void txConflictGetAndPut(boolean noVal, boolean rmv) throws Exception {
        Ignite ignite0 = ignite(0);

        final IgniteTransactions txs = ignite0.transactions();

        for (CacheConfiguration<Integer, Integer> ccfg : cacheConfigurations()) {
            logCacheInfo(ccfg);

            try {
                IgniteCache<Integer, Integer> cache = ignite0.createCache(ccfg);

                List<Integer> keys = testKeys(cache);

                for (Integer key : keys) {
                    log.info("Test key: " + key);

                    Integer expVal = null;

                    if (!noVal) {
                        expVal = -1;

                        cache.put(key, expVal);
                    }

                    try {
                        try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                            Integer val = rmv ? cache.getAndRemove(key) : cache.getAndPut(key, 2);

                            assertEquals(expVal, val);

                            updateKey(cache, key, 1);

                            tx.commit();
                        }

                        fail();
                    }
                    catch (TransactionOptimisticException e) {
                        log.info("Expected exception: " + e);
                    }

                    checkValue(key, 1, cache.getName());

                    try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                        Object val = rmv ? cache.getAndRemove(key) : cache.getAndPut(key, 2);

                        assertEquals(1, val);

                        tx.commit();
                    }

                    checkValue(key, rmv ? null : 2, cache.getName());
                }
            }
            finally {
                destroyCache(ignite0, ccfg.getName());
            }
        }
    }

    /**
     * @throws Exception If failed
     */
    public void testTxConflictInvoke1() throws Exception {
        txConflictInvoke(true, false);
    }

    /**
     * @throws Exception If failed
     */
    public void testTxConflictInvoke2() throws Exception {
        txConflictInvoke(false, false);
    }

    /**
     * @throws Exception If failed
     */
    public void testTxConflictInvoke3() throws Exception {
        txConflictInvoke(true, true);
    }

    /**
     * @throws Exception If failed
     */
    public void testTxConflictInvoke4() throws Exception {
        txConflictInvoke(false, true);
    }

    /**
     * @param noVal If {@code true} there is no cache value when read in tx.
     * @param rmv If {@code true} invoke does remove value, otherwise put.
     * @throws Exception If failed.
     */
    private void txConflictInvoke(boolean noVal, boolean rmv) throws Exception {
        Ignite ignite0 = ignite(0);

        final IgniteTransactions txs = ignite0.transactions();

        for (CacheConfiguration<Integer, Integer> ccfg : cacheConfigurations()) {
            logCacheInfo(ccfg);

            try {
                IgniteCache<Integer, Integer> cache = ignite0.createCache(ccfg);

                List<Integer> keys = testKeys(cache);

                for (Integer key : keys) {
                    log.info("Test key: " + key);

                    Integer expVal = null;

                    if (!noVal) {
                        expVal = -1;

                        cache.put(key, expVal);
                    }

                    try {
                        try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                            Integer val = cache.invoke(key, new SetValueProcessor(rmv ? null : 2));

                            assertEquals(expVal, val);

                            updateKey(cache, key, 1);

                            tx.commit();
                        }

                        fail();
                    }
                    catch (TransactionOptimisticException e) {
                        log.info("Expected exception: " + e);
                    }

                    checkValue(key, 1, cache.getName());

                    try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                        Object val = cache.invoke(key, new SetValueProcessor(rmv ? null : 2));

                        assertEquals(1, val);

                        tx.commit();
                    }

                    checkValue(key, rmv ? null : 2, cache.getName());
                }
            }
            finally {
                destroyCache(ignite0, ccfg.getName());
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxConflictPutIfAbsent() throws Exception {
        Ignite ignite0 = ignite(0);

        final IgniteTransactions txs = ignite0.transactions();

        for (CacheConfiguration<Integer, Integer> ccfg : cacheConfigurations()) {
            logCacheInfo(ccfg);

            try {
                IgniteCache<Integer, Integer> cache = ignite0.createCache(ccfg);

                List<Integer> keys = testKeys(cache);

                for (Integer key : keys) {
                    log.info("Test key: " + key);

                    try {
                        try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                            boolean put = cache.putIfAbsent(key, 2);

                            assertTrue(put);

                            updateKey(cache, key, 1);

                            tx.commit();
                        }

                        fail();
                    }
                    catch (TransactionOptimisticException e) {
                        log.info("Expected exception: " + e);
                    }

                    checkValue(key, 1, cache.getName());

                    try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                        boolean put = cache.putIfAbsent(key, 2);

                        assertFalse(put);

                        tx.commit();
                    }

                    checkValue(key, 1, cache.getName());

                    cache.remove(key);

                    try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                        boolean put = cache.putIfAbsent(key, 2);

                        assertTrue(put);

                        tx.commit();
                    }

                    checkValue(key, 2, cache.getName());

                    try {
                        try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                            boolean put = cache.putIfAbsent(key, 2);

                            assertFalse(put);

                            updateKey(cache, key, 3);

                            tx.commit();
                        }

                        fail();
                    }
                    catch (TransactionOptimisticException e) {
                        log.info("Expected exception: " + e);
                    }

                    checkValue(key, 3, cache.getName());
                }
            }
            finally {
                destroyCache(ignite0, ccfg.getName());
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxConflictReplace() throws Exception {
        Ignite ignite0 = ignite(0);

        final IgniteTransactions txs = ignite0.transactions();

        for (CacheConfiguration<Integer, Integer> ccfg : cacheConfigurations()) {
            logCacheInfo(ccfg);

            try {
                IgniteCache<Integer, Integer> cache = ignite0.createCache(ccfg);

                List<Integer> keys = testKeys(cache);

                for (final Integer key : keys) {
                    log.info("Test key: " + key);

                    try {
                        try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                            boolean replace = cache.replace(key, 2);

                            assertFalse(replace);

                            updateKey(cache, key, 1);

                            tx.commit();
                        }

                        fail();
                    }
                    catch (TransactionOptimisticException e) {
                        log.info("Expected exception: " + e);
                    }

                    checkValue(key, 1, cache.getName());

                    try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                        boolean replace = cache.replace(key, 2);

                        assertTrue(replace);

                        tx.commit();
                    }

                    checkValue(key, 2, cache.getName());

                    cache.remove(key);

                    try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                        boolean replace = cache.replace(key, 2);

                        assertFalse(replace);

                        tx.commit();
                    }

                    checkValue(key, null, cache.getName());

                    try {
                        try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                            boolean replace = cache.replace(key, 2);

                            assertFalse(replace);

                            updateKey(cache, key, 3);

                            tx.commit();
                        }

                        fail();
                    }
                    catch (TransactionOptimisticException e) {
                        log.info("Expected exception: " + e);
                    }

                    checkValue(key, 3, cache.getName());

                    try {
                        try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                            boolean replace = cache.replace(key, 2);

                            assertTrue(replace);

                            txAsync(cache, OPTIMISTIC, SERIALIZABLE,
                                new IgniteClosure<IgniteCache<Integer, Integer>, Void>() {
                                    @Override public Void apply(IgniteCache<Integer, Integer> cache) {
                                        cache.remove(key);

                                        return null;
                                    }
                                }
                            );

                            tx.commit();
                        }

                        fail();
                    }
                    catch (TransactionOptimisticException e) {
                        log.info("Expected exception: " + e);
                    }

                    checkValue(key, null, cache.getName());

                    cache.put(key, 1);

                    try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                        boolean replace = cache.replace(key, 2);

                        assertTrue(replace);

                        tx.commit();
                    }

                    checkValue(key, 2, cache.getName());
                }
            }
            finally {
                destroyCache(ignite0, ccfg.getName());
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxConflictGetAndReplace() throws Exception {
        Ignite ignite0 = ignite(0);

        final IgniteTransactions txs = ignite0.transactions();

        for (CacheConfiguration<Integer, Integer> ccfg : cacheConfigurations()) {
            logCacheInfo(ccfg);

            try {
                IgniteCache<Integer, Integer> cache = ignite0.createCache(ccfg);

                List<Integer> keys = testKeys(cache);

                for (final Integer key : keys) {
                    log.info("Test key: " + key);

                    try {
                        try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                            Object old = cache.getAndReplace(key, 2);

                            assertNull(old);

                            updateKey(cache, key, 1);

                            tx.commit();
                        }

                        fail();
                    }
                    catch (TransactionOptimisticException e) {
                        log.info("Expected exception: " + e);
                    }

                    checkValue(key, 1, cache.getName());

                    try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                        Object old = cache.getAndReplace(key, 2);

                        assertEquals(1, old);

                        tx.commit();
                    }

                    checkValue(key, 2, cache.getName());

                    cache.remove(key);

                    try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                        Object old = cache.getAndReplace(key, 2);

                        assertNull(old);

                        tx.commit();
                    }

                    checkValue(key, null, cache.getName());

                    try {
                        try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                            Object old = cache.getAndReplace(key, 2);

                            assertNull(old);

                            updateKey(cache, key, 3);

                            tx.commit();
                        }

                        fail();
                    }
                    catch (TransactionOptimisticException e) {
                        log.info("Expected exception: " + e);
                    }

                    checkValue(key, 3, cache.getName());

                    try {
                        try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                            Object old = cache.getAndReplace(key, 2);

                            assertEquals(3, old);

                            txAsync(cache, OPTIMISTIC, SERIALIZABLE,
                                new IgniteClosure<IgniteCache<Integer, Integer>, Void>() {
                                    @Override public Void apply(IgniteCache<Integer, Integer> cache) {
                                        cache.remove(key);

                                        return null;
                                    }
                                }
                            );

                            tx.commit();
                        }

                        fail();
                    }
                    catch (TransactionOptimisticException e) {
                        log.info("Expected exception: " + e);
                    }

                    checkValue(key, null, cache.getName());

                    cache.put(key, 1);

                    try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                        Object old = cache.getAndReplace(key, 2);

                        assertEquals(1, old);

                        tx.commit();
                    }

                    checkValue(key, 2, cache.getName());
                }
            }
            finally {
                destroyCache(ignite0, ccfg.getName());
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxNoConflictPut1() throws Exception {
        txNoConflictUpdate(true, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxNoConflictPut2() throws Exception {
        txNoConflictUpdate(false, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxNoConflictRemove1() throws Exception {
        txNoConflictUpdate(true, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxNoConflictRemove2() throws Exception {
        txNoConflictUpdate(false, true);
    }

    /**
     * @throws Exception If failed.
     * @param noVal If {@code true} there is no cache value when do update in tx.
     * @param rmv If {@code true} tests remove, otherwise put.
     */
    private void txNoConflictUpdate(boolean noVal, boolean rmv) throws Exception {
        Ignite ignite0 = ignite(0);

        final IgniteTransactions txs = ignite0.transactions();

        for (CacheConfiguration<Integer, Integer> ccfg : cacheConfigurations()) {
            logCacheInfo(ccfg);

            try {
                IgniteCache<Integer, Integer> cache = ignite0.createCache(ccfg);

                List<Integer> keys = testKeys(cache);

                for (Integer key : keys) {
                    log.info("Test key: " + key);

                    if (!noVal)
                        cache.put(key, -1);

                    try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                        if (rmv)
                            cache.remove(key);
                        else
                            cache.put(key, 2);

                        updateKey(cache, key, 1);

                        tx.commit();
                    }

                    checkValue(key, rmv ? null : 2, cache.getName());

                    try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                        cache.put(key, 3);

                        tx.commit();
                    }

                    checkValue(key, 3, cache.getName());
                }

                Map<Integer, Integer> map = new HashMap<>();

                for (int i = 0; i < 100; i++)
                    map.put(i, i);

                try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                    if (rmv)
                        cache.removeAll(map.keySet());
                    else
                        cache.putAll(map);

                    txAsync(cache, PESSIMISTIC, REPEATABLE_READ,
                        new IgniteClosure<IgniteCache<Integer, Integer>, Void>() {
                            @Override public Void apply(IgniteCache<Integer, Integer> cache) {
                                Map<Integer, Integer> map = new HashMap<>();

                                for (int i = 0; i < 100; i++)
                                    map.put(i, -1);

                                cache.putAll(map);

                                return null;
                            }
                        }
                    );

                    tx.commit();
                }

                for (int i = 0; i < 100; i++)
                    checkValue(i, rmv ? null : i, cache.getName());
            }
            finally {
                destroyCache(ignite0, ccfg.getName());
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxRollbackIfLocked1() throws Exception {
        Ignite ignite0 = ignite(0);

        IgniteTransactions txs = ignite0.transactions();

        for (CacheConfiguration<Integer, Integer> ccfg : cacheConfigurations()) {
            logCacheInfo(ccfg);

            try {
                IgniteCache<Integer, Integer> cache = ignite0.createCache(ccfg);

                List<Integer> keys = testKeys(cache);

                for (Integer key : keys) {
                    log.info("Test key: " + key);

                    CountDownLatch latch = new CountDownLatch(1);

                    IgniteInternalFuture<?> fut = lockKey(latch, cache, key);

                    try {
                        try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                            cache.put(key, 2);

                            log.info("Commit");

                            tx.commit();
                        }

                        fail();
                    }
                    catch (TransactionOptimisticException e) {
                        log.info("Expected exception: " + e);
                    }

                    latch.countDown();

                    fut.get();

                    checkValue(key, 1, cache.getName());

                    try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                        cache.put(key, 2);

                        tx.commit();
                    }

                    checkValue(key, 2, cache.getName());
                }
            }
            finally {
                destroyCache(ignite0, ccfg.getName());
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxRollbackIfLocked2() throws Exception {
        rollbackIfLockedPartialLock(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxRollbackIfLocked3() throws Exception {
        rollbackIfLockedPartialLock(true);
    }

    /**
     * @param locKey If {@code true} gets lock for local key.
     * @throws Exception If failed.
     */
    private void rollbackIfLockedPartialLock(boolean locKey) throws Exception {
        Ignite ignite0 = ignite(0);

        final IgniteTransactions txs = ignite0.transactions();

        for (CacheConfiguration<Integer, Integer> ccfg : cacheConfigurations()) {
            logCacheInfo(ccfg);

            try {
                IgniteCache<Integer, Integer> cache = ignite0.createCache(ccfg);

                final Integer key1 = primaryKey(ignite(1).cache(cache.getName()));
                final Integer key2 = locKey ? primaryKey(cache) : primaryKey(ignite(2).cache(cache.getName()));

                CountDownLatch latch = new CountDownLatch(1);

                IgniteInternalFuture<?> fut = lockKey(latch, cache, key1);

                try {
                    try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                        cache.put(key1, 2);
                        cache.put(key2, 2);

                        tx.commit();
                    }

                    fail();
                }
                catch (TransactionOptimisticException e) {
                    log.info("Expected exception: " + e);
                }

                latch.countDown();

                fut.get();

                checkValue(key1, 1, cache.getName());
                checkValue(key2, null, cache.getName());

                try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                    cache.put(key1, 2);
                    cache.put(key2, 2);

                    tx.commit();
                }

                checkValue(key1, 2, cache.getName());
                checkValue(key2, 2, cache.getName());
            }
            finally {
                destroyCache(ignite0, ccfg.getName());
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testConcurrentUpdateNoDeadlock() throws Exception {
        concurrentUpdateNoDeadlock(Collections.singletonList(ignite(0)), 10, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testConcurrentUpdateNoDeadlockNodeRestart() throws Exception {
        concurrentUpdateNoDeadlock(Collections.singletonList(ignite(1)), 10, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testConcurrentUpdateNoDeadlockClients() throws Exception {
        concurrentUpdateNoDeadlock(clients(), 20, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testConcurrentUpdateNoDeadlockClientsNodeRestart() throws Exception {
        concurrentUpdateNoDeadlock(clients(), 20, true);
    }

    /**
     * @return Client nodes.
     */
    private List<Ignite> clients() {
        List<Ignite> clients = new ArrayList<>();

        for (int i = 0; i < CLIENTS; i++) {
            Ignite ignite = ignite(SRVS + i);

            assertTrue(ignite.configuration().isClientMode());

            clients.add(ignite);
        }

        return clients;
    }

    /**
     * @param updateNodes Nodes executing updates.
     * @param threads Number of threads executing updates.
     * @param restart If {@code true} restarts one node.
     * @throws Exception If failed.
     */
    private void concurrentUpdateNoDeadlock(final List<Ignite> updateNodes,
        int threads,
        final boolean restart) throws Exception {
        assert updateNodes.size() > 0;

        final Ignite ignite0 = ignite(0);

        final String cacheName =
            ignite0.createCache(cacheConfiguration(PARTITIONED, FULL_SYNC, 1, false, false)).getName();

        try {
            final int KEYS = 100;

            final AtomicBoolean finished = new AtomicBoolean();

            IgniteInternalFuture<Object> fut = null;

            try {
                if (restart) {
                    fut = GridTestUtils.runAsync(new Callable<Object>() {
                        @Override public Object call() throws Exception {
                            while (!finished.get()) {
                                stopGrid(0);

                                U.sleep(300);

                                Ignite ignite = startGrid(0);

                                assertFalse(ignite.configuration().isClientMode());
                            }

                            return null;
                        }
                    });
                }

                int ITERS = FAST ? 1 : 10;

                for (int i = 0; i < ITERS; i++) {
                    log.info("Iteration: " + i);

                    final long stopTime = U.currentTimeMillis() + 10_000;

                    final AtomicInteger idx = new AtomicInteger();

                    IgniteInternalFuture<?> updateFut = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
                        @Override public Void call() throws Exception {
                            int nodeIdx = idx.getAndIncrement() % updateNodes.size();

                            Ignite node = updateNodes.get(nodeIdx);

                            log.info("Tx thread: " + node.name());

                            final IgniteTransactions txs = node.transactions();

                            final IgniteCache<Integer, Integer> cache = node.cache(cacheName);

                            assertNotNull(cache);

                            ThreadLocalRandom rnd = ThreadLocalRandom.current();

                            while (U.currentTimeMillis() < stopTime) {
                                final Map<Integer, Integer> keys = new LinkedHashMap<>();

                                for (int i = 0; i < KEYS / 2; i++)
                                    keys.put(rnd.nextInt(KEYS), rnd.nextInt());

                                try {
                                    if (restart) {
                                        doInTransaction(node, OPTIMISTIC, SERIALIZABLE, new Callable<Void>() {
                                            @Override public Void call() throws Exception {
                                                cache.putAll(keys);

                                                return null;
                                            }
                                        });
                                    }
                                    else {
                                        try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                                            cache.putAll(keys);

                                            tx.commit();
                                        }
                                    }
                                }
                                catch (TransactionOptimisticException ignore) {
                                    // No-op.
                                }
                                catch (Throwable e) {
                                    log.error("Unexpected error: " + e, e);

                                    throw e;
                                }
                            }

                            return null;
                        }
                    }, threads, "tx-thread");

                    updateFut.get(60, SECONDS);

                    IgniteCache<Integer, Integer> cache = ignite(1).cache(cacheName);

                    for (int key = 0; key < KEYS; key++) {
                        Integer val = cache.get(key);

                        for (int node = 1; node < SRVS + CLIENTS; node++)
                            assertEquals(val, ignite(node).cache(cache.getName()).get(key));
                    }
                }

                finished.set(true);

                if (fut != null)
                    fut.get();
            }
            finally {
                finished.set(true);
            }
        }
        finally {
            destroyCache(ignite(1), cacheName);
        }
    }

    /**
     * @return Cache configurations.
     */
    private List<CacheConfiguration<Integer, Integer>> cacheConfigurations() {
        List<CacheConfiguration<Integer, Integer>> ccfgs = new ArrayList<>();

        // No store, no near.
        ccfgs.add(cacheConfiguration(PARTITIONED, FULL_SYNC, 0, false, false));
        ccfgs.add(cacheConfiguration(PARTITIONED, FULL_SYNC, 1, false, false));
        ccfgs.add(cacheConfiguration(PARTITIONED, FULL_SYNC, 2, false, false));
        ccfgs.add(cacheConfiguration(REPLICATED, FULL_SYNC, 0, false, false));

        // Store, no near.
        ccfgs.add(cacheConfiguration(PARTITIONED, FULL_SYNC, 0, true, false));
        ccfgs.add(cacheConfiguration(PARTITIONED, FULL_SYNC, 1, true, false));
        ccfgs.add(cacheConfiguration(PARTITIONED, FULL_SYNC, 2, true, false));
        ccfgs.add(cacheConfiguration(REPLICATED, FULL_SYNC, 0, true, false));

        // No store, near.
        ccfgs.add(cacheConfiguration(PARTITIONED, FULL_SYNC, 0, false, true));
        ccfgs.add(cacheConfiguration(PARTITIONED, FULL_SYNC, 1, false, true));
        ccfgs.add(cacheConfiguration(PARTITIONED, FULL_SYNC, 2, false, true));

        // Store, near.
        ccfgs.add(cacheConfiguration(PARTITIONED, FULL_SYNC, 0, true, true));
        ccfgs.add(cacheConfiguration(PARTITIONED, FULL_SYNC, 1, true, true));
        ccfgs.add(cacheConfiguration(PARTITIONED, FULL_SYNC, 2, true, true));

        return ccfgs;
    }

    /**
     * @param ccfg Cache configuration.
     */
    private void logCacheInfo(CacheConfiguration<?, ?> ccfg) {
        log.info("Test cache [mode=" + ccfg.getCacheMode() +
            ", sync=" + ccfg.getWriteSynchronizationMode() +
            ", backups=" + ccfg.getBackups() +
            ", near=" + (ccfg.getNearConfiguration() != null) +
            ", store=" + ccfg.isWriteThrough() + ']');
    }

    /**
     * @param cache Cache.
     * @return Test keys.
     * @throws Exception If failed.
     */
    private List<Integer> testKeys(IgniteCache<Integer, Integer> cache) throws Exception {
        CacheConfiguration ccfg = cache.getConfiguration(CacheConfiguration.class);

        List<Integer> keys = new ArrayList<>();

        if (ccfg.getCacheMode() == PARTITIONED)
            keys.add(nearKey(cache));

        keys.add(primaryKey(cache));

        if (ccfg.getBackups() != 0)
            keys.add(backupKey(cache));

        return keys;
    }

    /**
     * @param cache Cache.
     * @param concurrency Transaction concurrency.
     * @param isolation Transaction isolcation.
     * @param c Closure to run in transaction.
     * @throws Exception If failed.
     */
    private void txAsync(final IgniteCache<Integer, Integer> cache,
        final TransactionConcurrency concurrency,
        final TransactionIsolation isolation,
        final IgniteClosure<IgniteCache<Integer, Integer>, Void> c) throws Exception {
        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                IgniteTransactions txs = cache.unwrap(Ignite.class).transactions();

                try (Transaction tx = txs.txStart(concurrency, isolation)) {
                    c.apply(cache);

                    tx.commit();
                }

                return null;
            }
        }, "async-thread");

        fut.get();
    }

    /**
     * @param cache Cache.
     * @param key Key.
     * @param val Value.
     * @throws Exception If failed.
     */
    private void updateKey(
        final IgniteCache<Integer, Integer> cache,
        final Integer key,
        final Integer val) throws Exception {
        txAsync(cache, PESSIMISTIC, REPEATABLE_READ, new IgniteClosure<IgniteCache<Integer, Integer>, Void>() {
            @Override public Void apply(IgniteCache<Integer, Integer> cache) {
                cache.put(key, val);

                return null;
            }
        });
    }

    /**
     * @param key Key.
     * @param expVal Expected value.
     * @param cacheName Cache name.
     */
    private void checkValue(Object key, Object expVal, String cacheName) {
        for (int i = 0; i < SRVS + CLIENTS; i++) {
            IgniteCache<Object, Object> cache = ignite(i).cache(cacheName);

            assertEquals(expVal, cache.get(key));
        }
    }

    /**
     * @param releaseLatch Release lock latch.
     * @param cache Cache.
     * @param key Key.
     * @return Future.
     * @throws Exception If failed.
     */
    private IgniteInternalFuture<?> lockKey(
        final CountDownLatch releaseLatch,
        final IgniteCache<Integer, Integer> cache,
        final Integer key) throws Exception {
        final CountDownLatch lockLatch = new CountDownLatch(1);

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                IgniteTransactions txs = cache.unwrap(Ignite.class).transactions();

                try (Transaction tx = txs.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                    cache.put(key, 1);

                    log.info("Locked key: " + key);

                    lockLatch.countDown();

                    assertTrue(releaseLatch.await(100000, SECONDS));

                    log.info("Commit tx: " + key);

                    tx.commit();
                }

                return null;
            }
        }, "lock-thread");

        assertTrue(lockLatch.await(10, SECONDS));

        return fut;
    }

    /**
     * @param ignite Node.
     * @param cacheName Cache name.
     */
    private void destroyCache(Ignite ignite, String cacheName) {
        storeMap.clear();

        ignite.destroyCache(cacheName);
    }

    /**
     * @param cacheMode Cache mode.
     * @param syncMode Write synchronization mode.
     * @param backups Number of backups.
     * @param storeEnabled If {@code true} adds cache store.
     * @param nearCache If {@code true} near cache is enabled.
     * @return Cache configuration.
     */
    private CacheConfiguration<Integer, Integer> cacheConfiguration(
        CacheMode cacheMode,
        CacheWriteSynchronizationMode syncMode,
        int backups,
        boolean storeEnabled,
        boolean nearCache) {
        CacheConfiguration<Integer, Integer> ccfg = new CacheConfiguration<>();

        ccfg.setCacheMode(cacheMode);
        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setWriteSynchronizationMode(syncMode);

        if (cacheMode == PARTITIONED)
            ccfg.setBackups(backups);

        if (storeEnabled) {
            ccfg.setCacheStoreFactory(new TestStoreFactory());
            ccfg.setWriteThrough(true);
            ccfg.setReadThrough(true);
        }

        if (nearCache)
            ccfg.setNearConfiguration(new NearCacheConfiguration<Integer, Integer>());

        return ccfg;
    }

    /**
     *
     */
    private static class TestStoreFactory implements Factory<CacheStore<Integer, Integer>> {
        /** {@inheritDoc} */
        @Override public CacheStore<Integer, Integer> create() {
            return new CacheStoreAdapter<Integer, Integer>() {
                @Override public Integer load(Integer key) throws CacheLoaderException {
                    return storeMap.get(key);
                }

                @Override public void write(Cache.Entry<? extends Integer, ? extends Integer> entry) {
                    storeMap.put(entry.getKey(), entry.getValue());
                }

                @Override public void delete(Object key) {
                    storeMap.remove(key);
                }
            };
        }
    }

    /**
     * Sets given value, returns old value.
     */
    public static final class SetValueProcessor implements EntryProcessor<Integer, Integer, Integer> {
        /** */
        private Integer newVal;

        /**
         * @param newVal New value to set.
         */
        SetValueProcessor(Integer newVal) {
            this.newVal = newVal;
        }

        /** {@inheritDoc} */
        @Override public Integer process(MutableEntry<Integer, Integer> entry, Object... arguments) {
            Integer val = entry.getValue();

            if (newVal == null)
                entry.remove();
            else
                entry.setValue(newVal);

            return val;
        }
    }
}
