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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.integration.CacheLoaderException;
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
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionOptimisticException;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
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
    public void _testTxRollbackRead1() throws Exception {
        txRollbackRead(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxRollbackRead2() throws Exception {
        txRollbackRead(false);
    }

    /**
     * @param noVal If {@code true} there is no cache value when read in tx.
     * @throws Exception If failed.
     */
    private void txRollbackRead(boolean noVal) throws Exception {
        Ignite ignite0 = ignite(0);

        final IgniteTransactions txs = ignite0.transactions();

        for (CacheConfiguration<Integer, Integer> ccfg : cacheConfigurations()) {
            logCacheInfo(ccfg);

            try {
                IgniteCache<Integer, Integer> cache = ignite0.createCache(ccfg);

                List<Integer> keys = new ArrayList<>();

                keys.add(nearKey(cache));

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

                            log.info("Commit");

                            tx.commit();
                        }

                        fail();
                    }
                    catch (TransactionOptimisticException e) {
                        log.info("Expected exception: " + e);
                    }

                    assertEquals(1, (Object) cache.get(key));
                }
            }
            finally {
                ignite0.destroyCache(ccfg.getName());
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void _testTxRollbackReadWrite() throws Exception {
        Ignite ignite0 = ignite(0);

        final IgniteTransactions txs = ignite0.transactions();

        final IgniteCache<Integer, Integer> cache =
            ignite0.createCache(cacheConfiguration(PARTITIONED, FULL_SYNC, 1, false, false));

        final Integer key = nearKey(cache);

        try {
            try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                Integer val = cache.get(key);

                assertNull(val);

                updateKey(cache, key, 1);

                cache.put(key, 2);

                log.info("Commit");

                tx.commit();
            }

            fail();
        }
        catch (TransactionOptimisticException e) {
            log.info("Expected exception: " + e);
        }

        assertEquals(1, (Object)cache.get(key));

        try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
            cache.put(key, 2);

            tx.commit();
        }

        assertEquals(2, (Object) cache.get(key));
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

                final Integer key = nearKey(cache);

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

                assertEquals(1, (Object)cache.get(key));

                try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                    cache.put(key, 2);

                    tx.commit();
                }

                assertEquals(2, (Object)cache.get(key));
            }
            finally {
                ignite0.destroyCache(ccfg.getName());
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
    public void rollbackIfLockedPartialLock(boolean locKey) throws Exception {
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

                        log.info("Commit2");

                        tx.commit();
                    }

                    fail();
                }
                catch (TransactionOptimisticException e) {
                    log.info("Expected exception: " + e);
                }

                latch.countDown();

                fut.get();

                assertEquals(1, (Object) cache.get(key1));
                assertNull(cache.get(key2));

                try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                    cache.put(key1, 2);
                    cache.put(key2, 2);

                    log.info("Commit3");

                    tx.commit();
                }

                assertEquals(2, (Object) cache.get(key2));
                assertEquals(2, (Object) cache.get(key2));
            }
            finally {
                ignite0.destroyCache(ccfg.getName());
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
            ignite(1).destroyCache(cacheName);
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

        // Store, no near.
        ccfgs.add(cacheConfiguration(PARTITIONED, FULL_SYNC, 0, true, false));
        ccfgs.add(cacheConfiguration(PARTITIONED, FULL_SYNC, 1, true, false));
        ccfgs.add(cacheConfiguration(PARTITIONED, FULL_SYNC, 2, true, false));

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
     * @param key Key.
     * @param val Value.
     * @throws Exception If failed.
     */
    private void updateKey(
        final IgniteCache<Integer, Integer> cache,
        final Integer key,
        final Integer val) throws Exception {
        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                IgniteTransactions txs = cache.unwrap(Ignite.class).transactions();

                try (Transaction tx = txs.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                    cache.put(key, val);

                    tx.commit();
                }

                return null;
            }
        }, "update-thread");

        fut.get();
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
        ccfg.setBackups(backups);
        ccfg.setWriteSynchronizationMode(syncMode);

        if (storeEnabled) {
            ccfg.setCacheStoreFactory(new TestStoreFactory());
            ccfg.setWriteThrough(true);
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
                    return null;
                }

                @Override public void write(Cache.Entry<? extends Integer, ? extends Integer> entry) {
                    // No-op.
                }

                @Override public void delete(Object key) {
                    // No-op.
                }
            };
        }
    }
}
