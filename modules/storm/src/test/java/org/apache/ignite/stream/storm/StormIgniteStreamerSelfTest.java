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

package org.apache.ignite.stream.storm;

import backtype.storm.Config;
import backtype.storm.ILocalCluster;
import backtype.storm.Testing;
import backtype.storm.generated.StormTopology;
import backtype.storm.testing.CompleteTopologyParam;
import backtype.storm.testing.MkClusterParam;
import backtype.storm.testing.MockedSources;
import backtype.storm.testing.TestJob;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Values;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_PUT;

/**
 * Tests for {@link StormStreamer}.
 */
public class StormIgniteStreamerSelfTest extends GridCommonAbstractTest {
    /** Cache name. */
    private static final String TEST_CACHE = "testCache";

    /** Ignite test configuration file. */
    private static final String GRID_CONF_FILE = "modules/storm/src/test/resources/example-ignite.xml";

    /** Ignite instance. */
    private Ignite ignite;

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected void beforeTest() throws Exception {
        IgniteConfiguration cfg = loadConfiguration(GRID_CONF_FILE);

        cfg.setClientMode(false);

        ignite = startGrid("igniteServerNode", cfg);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * Tests for the streamer bolt. Ignite started in bolt based on what is specified in the configuration file.
     *
     * @throws TimeoutException
     * @throws InterruptedException
     */
    public void testStormStreamerIgniteBolt() throws TimeoutException, InterruptedException {
        StormStreamer<String, String> stormStreamer = new StormStreamer<>();
        stormStreamer.setAutoFlushFrequency(10L);
        stormStreamer.setAllowOverwrite(true);
        stormStreamer.setCacheName(TEST_CACHE);
        stormStreamer.setIgniteConfigFile(GRID_CONF_FILE);

        MkClusterParam mkClusterParam = new MkClusterParam();
        mkClusterParam.setSupervisors(4);

        Config daemonConf = new Config();
        daemonConf.put(Config.STORM_LOCAL_MODE_ZMQ, false);

        mkClusterParam.setDaemonConf(daemonConf);

        final CountDownLatch latch = new CountDownLatch(TestStormSpout.CNT);

        IgniteBiPredicate<UUID, CacheEvent> putLsnr = new IgniteBiPredicate<UUID, CacheEvent>() {
            @Override public boolean apply(UUID uuid, CacheEvent evt) {
                assert evt != null;

                latch.countDown();

                return true;
            }
        };

        final UUID putLsnrId = ignite.events(ignite.cluster().forCacheNodes(TEST_CACHE)).remoteListen(putLsnr, null, EVT_CACHE_OBJECT_PUT);

        Testing.withSimulatedTimeLocalCluster(mkClusterParam, new TestJob() {
                @Override
                public void run(ILocalCluster cluster) throws IOException, InterruptedException {
                    // Creates a test topology.
                    TopologyBuilder builder = new TopologyBuilder();

                    TestStormSpout testStormSpout = new TestStormSpout();

                    builder.setSpout("test-spout", testStormSpout);
                    builder.setBolt("ignite-bolt", stormStreamer).shuffleGrouping("test-spout");

                    StormTopology topology = builder.createTopology();

                    // Prepares a mock data for the spout.
                    MockedSources mockedSources = new MockedSources();
                    mockedSources.addMockData("test-spout", new Values(testStormSpout.getKeyValMap()));

                    // Prepares the config.
                    Config conf = new Config();
                    conf.setMessageTimeoutSecs(10);

                    CompleteTopologyParam completeTopologyParam = new CompleteTopologyParam();
                    completeTopologyParam.setTimeoutMs(10000);
                    completeTopologyParam.setMockedSources(mockedSources);
                    completeTopologyParam.setStormConf(conf);

                    Testing.completeTopology(cluster, topology, completeTopologyParam);

                    // Checks events successfully processed in 20 seconds.
                    assertTrue(latch.await(10, TimeUnit.SECONDS));

                    ignite.events(ignite.cluster().forCacheNodes(TEST_CACHE)).stopRemoteListen(putLsnrId);

                    compareStreamCacheData(testStormSpout.getKeyValMap());
                }
            }
        );
    }

    private void compareStreamCacheData(HashMap<String, String> keyValMap) {
        IgniteCache<String, String> cache = ignite.cache(TEST_CACHE);

        for (Map.Entry<String, String> entry : keyValMap.entrySet()) {
            assertEquals(entry.getValue(), cache.get(entry.getKey()));
        }

        assertEquals(TestStormSpout.CNT, cache.size(CachePeekMode.PRIMARY));
    }
}
