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
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
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
    private static final boolean FAST = false;

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

        cfg.setPeerClassLoadingEnabled(false);

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

                    cache.remove(key);

                    try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                        Integer val = cache.get(key);

                        assertNull(val);

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
                                @Override public Void apply(IgniteCache<Integer, Integer> cache) {
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
    public void testTxCommit() throws Exception {
        Ignite ignite0 = ignite(0);
        Ignite ignite1 = ignite(1);

        final IgniteTransactions txs0 = ignite0.transactions();
        final IgniteTransactions txs1 = ignite1.transactions();

        for (CacheConfiguration<Integer, Integer> ccfg : cacheConfigurations()) {
            logCacheInfo(ccfg);

            try {
                IgniteCache<Integer, Integer> cache0 = ignite0.createCache(ccfg);
                IgniteCache<Integer, Integer> cache1 = ignite1.cache(ccfg.getName());

                List<Integer> keys = testKeys(cache0);

                for (Integer key : keys) {
                    log.info("Test key: " + key);

                    Integer expVal = null;

                    for (int i = 0; i < 100; i++) {
                        try (Transaction tx = txs0.txStart(OPTIMISTIC, SERIALIZABLE)) {
                            Integer val = cache0.get(key);

                            assertEquals(expVal, val);

                            cache0.put(key, i);

                            tx.commit();

                            expVal = i;
                        }

                        try (Transaction tx = txs1.txStart(OPTIMISTIC, SERIALIZABLE)) {
                            Integer val = cache1.get(key);

                            assertEquals(expVal, val);

                            cache1.put(key, val);

                            tx.commit();
                        }

                        try (Transaction tx = txs0.txStart(OPTIMISTIC, SERIALIZABLE)) {
                            Integer val = cache0.get(key);

                            assertEquals(expVal, val);

                            cache0.put(key, val);

                            tx.commit();
                        }
                    }

                    checkValue(key, expVal, cache0.getName());
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
    public void testNearCacheReaderUpdate() throws Exception {
        Ignite ignite0 = ignite(0);

        IgniteCache<Integer, Integer> cache0 =
            ignite0.createCache(cacheConfiguration(PARTITIONED, FULL_SYNC, 1, false, false));

        final String cacheName = cache0.getName();

        try {
            Ignite client1 = ignite(SRVS);
            Ignite client2 = ignite(SRVS + 1);

            IgniteCache<Integer, Integer> cache1 = client1.createNearCache(cacheName,
                new NearCacheConfiguration<Integer, Integer>());
            IgniteCache<Integer, Integer> cache2 = client2.createNearCache(cacheName,
                new NearCacheConfiguration<Integer, Integer>());

            Integer key = primaryKey(ignite(0).cache(cacheName));

            try (Transaction tx = client1.transactions().txStart(OPTIMISTIC, SERIALIZABLE)) {
                assertNull(cache1.get(key));
                cache1.put(key, 1);

                tx.commit();
            }

            try (Transaction tx = client2.transactions().txStart(OPTIMISTIC, SERIALIZABLE)) {
                assertEquals(1, (Object) cache2.get(key));
                cache2.put(key, 2);

                tx.commit();
            }

            try (Transaction tx = client1.transactions().txStart(OPTIMISTIC, SERIALIZABLE)) {
                assertEquals(2, (Object)cache1.get(key));
                cache1.put(key, 3);

                tx.commit();
            }
        }
        finally {
            ignite0.destroyCache(cacheName);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testRollbackNearCache1() throws Exception {
        rollbackNearCacheWrite(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testRollbackNearCache2() throws Exception {
        rollbackNearCacheWrite(false);
    }

    /**
     * @param near If {@code true} locks entry using the same near cache.
     * @throws Exception If failed.
     */
    private void rollbackNearCacheWrite(boolean near) throws Exception {
        Ignite ignite0 = ignite(0);

        IgniteCache<Integer, Integer> cache0 =
            ignite0.createCache(cacheConfiguration(PARTITIONED, FULL_SYNC, 1, false, false));

        final String cacheName = cache0.getName();

        try {
            Ignite ignite = ignite(SRVS);

            IgniteCache<Integer, Integer> cache = ignite.createNearCache(cacheName,
                new NearCacheConfiguration<Integer, Integer>());

            IgniteTransactions txs = ignite.transactions();

            Integer key1 = primaryKey(ignite(0).cache(cacheName));
            Integer key2 = primaryKey(ignite(1).cache(cacheName));
            Integer key3 = primaryKey(ignite(2).cache(cacheName));

            CountDownLatch latch = new CountDownLatch(1);

            IgniteInternalFuture<?> fut = null;

            try {
                try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                    cache.put(key1, key1);
                    cache.put(key2, key2);
                    cache.put(key3, key3);

                    fut = lockKey(latch, near ? cache : cache0, key2);

                    tx.commit();
                }

                fail();
            }
            catch (TransactionOptimisticException e) {
                log.info("Expected exception: " + e);
            }

            latch.countDown();

            assert fut != null;

            fut.get();

            checkValue(key1, null, cacheName);
            checkValue(key2, 1, cacheName);
            checkValue(key3, null, cacheName);

            try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                cache.put(key1, key1);
                cache.put(key2, key2);
                cache.put(key3, key3);

                tx.commit();
            }

            checkValue(key1, key1, cacheName);
            checkValue(key2, key2, cacheName);
            checkValue(key3, key3, cacheName);
        }
        finally {
            ignite0.destroyCache(cacheName);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testRollbackNearCache3() throws Exception {
        rollbackNearCacheRead(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testRollbackNearCache4() throws Exception {
        rollbackNearCacheRead(false);
    }

    /**
     * @param near If {@code true} updates entry using the same near cache.
     * @throws Exception If failed.
     */
    private void rollbackNearCacheRead(boolean near) throws Exception {
        Ignite ignite0 = ignite(0);

        IgniteCache<Integer, Integer> cache0 =
            ignite0.createCache(cacheConfiguration(PARTITIONED, FULL_SYNC, 1, false, false));

        final String cacheName = cache0.getName();

        try {
            Ignite ignite = ignite(SRVS);

            IgniteCache<Integer, Integer> cache = ignite.createNearCache(cacheName,
                new NearCacheConfiguration<Integer, Integer>());

            IgniteTransactions txs = ignite.transactions();

            Integer key1 = primaryKey(ignite(0).cache(cacheName));
            Integer key2 = primaryKey(ignite(1).cache(cacheName));
            Integer key3 = primaryKey(ignite(2).cache(cacheName));

            cache0.put(key1, -1);
            cache0.put(key2, -1);
            cache0.put(key3, -1);

            try {
                try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                    cache.get(key1);
                    cache.get(key2);
                    cache.get(key3);

                    updateKey(near ? cache : cache0, key2, -2);

                    tx.commit();
                }

                fail();
            }
            catch (TransactionOptimisticException e) {
                log.info("Expected exception: " + e);
            }

            checkValue(key1, -1, cacheName);
            checkValue(key2, -2, cacheName);
            checkValue(key3, -1, cacheName);

            try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                cache.put(key1, key1);
                cache.put(key2, key2);
                cache.put(key3, key3);

                tx.commit();
            }

            checkValue(key1, key1, cacheName);
            checkValue(key2, key2, cacheName);
            checkValue(key3, key3, cacheName);
        }
        finally {
            ignite0.destroyCache(cacheName);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testAccountTx1() throws Exception {
        accountTx(false, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAccountTxNearCache() throws Exception {
        accountTx(false, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAccountTx2() throws Exception {
        accountTx(true, false);
    }

    /**
     * @param getAll If {@code true} uses getAll/putAll in transaction.
     * @param nearCache If {@code true} near cache is enabled.
     * @throws Exception If failed.
     */
    private void accountTx(final boolean getAll, final boolean nearCache) throws Exception {
        final Ignite ignite0 = ignite(0);

        final String cacheName =
            ignite0.createCache(cacheConfiguration(PARTITIONED, FULL_SYNC, 1, false, false)).getName();

        try {
            final List<Ignite> clients = clients();

            final int ACCOUNTS = 100;
            final int VAL_PER_ACCOUNT = 10_000;

            IgniteCache<Integer, Account> srvCache = ignite0.cache(cacheName);

            for (int i = 0; i < ACCOUNTS; i++)
                srvCache.put(i, new Account(VAL_PER_ACCOUNT));

            final AtomicInteger idx = new AtomicInteger();

            final int THREADS = 20;

            final long stopTime = System.currentTimeMillis() + 10_000;

            IgniteInternalFuture<?> fut = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    int nodeIdx = idx.getAndIncrement() % clients.size();

                    Ignite node = clients.get(nodeIdx);

                    Thread.currentThread().setName("update-" + node.name());

                    log.info("Tx thread: " + node.name());

                    final IgniteTransactions txs = node.transactions();

                    final IgniteCache<Integer, Account> cache =
                        nearCache ? node.createNearCache(cacheName, new NearCacheConfiguration<Integer, Account>()) :
                            node.<Integer, Account>cache(cacheName);

                    assertNotNull(cache);

                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    while (U.currentTimeMillis() < stopTime) {
                        int id1 = rnd.nextInt(ACCOUNTS);

                        int id2 = rnd.nextInt(ACCOUNTS);

                        while (id2 == id1)
                            id2 = rnd.nextInt(ACCOUNTS);

                        try {
                            while (true) {
                                try {
                                    try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                                        if (getAll) {
                                            Map<Integer, Account> map = cache.getAll(F.asSet(id1, id2));

                                            Account a1 = cache.get(id1);
                                            Account a2 = cache.get(id2);

                                            assertNotNull(a1);
                                            assertNotNull(a2);

                                            if (a1.value() > 0) {
                                                a1 = new Account(a1.value() - 1);
                                                a2 = new Account(a2.value() + 1);
                                            }

                                            map.put(id1, a1);
                                            map.put(id2, a2);

                                            cache.putAll(map);
                                        }
                                        else {
                                            Account a1 = cache.get(id1);
                                            Account a2 = cache.get(id2);

                                            assertNotNull(a1);
                                            assertNotNull(a2);

                                            if (a1.value() > 0) {
                                                a1 = new Account(a1.value() - 1);
                                                a2 = new Account(a2.value() + 1);
                                            }

                                            cache.put(id1, a1);
                                            cache.put(id2, a2);
                                        }

                                        tx.commit();
                                    }

                                    break;
                                }
                                catch (TransactionOptimisticException ignore) {
                                    // Retry.
                                }
                            }
                        }
                        catch (Throwable e) {
                            log.error("Unexpected error: " + e, e);

                            throw e;
                        }
                    }

                    return null;
                }
            }, THREADS, "tx-thread");

            fut.get(30_000);

            int sum = 0;

            for (int i = 0; i < ACCOUNTS; i++) {
                Account a = srvCache.get(i);

                assertNotNull(a);
                assertTrue(a.value() >= 0);

                log.info("Account: " + a.value());

                sum += a.value();
            }

            assertEquals(ACCOUNTS * VAL_PER_ACCOUNT, sum);

            for (int node = 0; node < SRVS + CLIENTS; node++) {
                log.info("Verify node: " + node);

                Ignite ignite = ignite(node);

                IgniteCache<Integer, Account> cache = ignite.cache(cacheName);

                sum = 0;

                try (Transaction tx = ignite.transactions().txStart(OPTIMISTIC, SERIALIZABLE)) {
                    Map<Integer, Account> map = new HashMap<>();

                    for (int i = 0; i < ACCOUNTS; i++) {
                        Account a = cache.get(i);

                        assertNotNull(a);

                        map.put(i, a);

                        sum += a.value();
                    }

                    Account a1 = map.get(0);
                    Account a2 = map.get(1);

                    if (a1.value() > 0) {
                        a1 = new Account(a1.value() - 1);
                        a2 = new Account(a2.value() + 1);

                        map.put(0, a1);
                        map.put(1, a2);
                    }

                    cache.putAll(map);

                    tx.commit();
                }

                assertEquals(ACCOUNTS * VAL_PER_ACCOUNT, sum);
            }
        }
        finally {
            ignite0.destroyCache(cacheName);
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
    public void testConcurrentUpdateNoDeadlockFromClients() throws Exception {
        concurrentUpdateNoDeadlock(clients(), 20, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testConcurrentUpdateNoDeadlockFromClientsNodeRestart() throws Exception {
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
        if (FAST)
            return;

        assert updateNodes.size() > 0;

        final Ignite srv = ignite(1);

        final String cacheName =
            srv.createCache(cacheConfiguration(PARTITIONED, FULL_SYNC, 1, false, false)).getName();

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

                for (int i = 0; i < 10; i++) {
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

                    IgniteCache<Integer, Integer> cache = srv.cache(cacheName);

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
            destroyCache(srv, cacheName);
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

    /**
     *
     */
    static class Account {
        /** */
        private final int val;

        /**
         * @param val Value.
         */
        public Account(int val) {
            this.val = val;
        }

        /**
         * @return Value.
         */
        public int value() {
            return val;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Account.class, this);
        }
    }
}
