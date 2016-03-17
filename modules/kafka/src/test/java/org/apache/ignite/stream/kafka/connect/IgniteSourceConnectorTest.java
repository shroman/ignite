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

package org.apache.ignite.stream.kafka.connect;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.stream.kafka.TestKafkaBroker;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.Worker;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.runtime.standalone.StandaloneHerder;
import org.apache.kafka.connect.storage.MemoryOffsetBackingStore;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.connect.util.FutureCallback;

import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_PUT;

/**
 * Tests for {@link IgniteSourceConnector}.
 */
public class IgniteSourceConnectorTest extends GridCommonAbstractTest {
    /** Number of input messages. */
    private static final int EVENT_CNT = 100;

    /** Cache name. */
    private static final String CACHE_NAME = "testCache";

    /** Test topics created by connector. */
    private static final String[] TOPICS = {"test1"};

    /** Test Kafka broker. */
    private TestKafkaBroker kafkaBroker;

    /** Worker to run tasks. */
    private Worker worker;

    /** Workers' herder. */
    private Herder herder;

    /** Ignite server node shared among tests. */
    private static Ignite grid;

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected void beforeTestsStarted() throws Exception {
        IgniteConfiguration cfg = loadConfiguration("modules/kafka/src/test/resources/example-ignite.xml");

        cfg.setClientMode(false);

        grid = startGrid("igniteServerNode", cfg);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        kafkaBroker = new TestKafkaBroker();

        WorkerConfig workerConfig = new StandaloneConfig(makeWorkerProps());

        MemoryOffsetBackingStore offsetBackingStore = new MemoryOffsetBackingStore();
        offsetBackingStore.configure(workerConfig.originals());

        worker = new Worker(workerConfig, offsetBackingStore);
        worker.start();

        herder = new StandaloneHerder(worker);
        herder.start();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        herder.stop();

        worker.stop();

        kafkaBroker.shutdown();

        grid.cache(CACHE_NAME).clear();
    }

    /**
     * Tests data flow from injecting data into grid and transferring it to Kafka cluster
     * without user-specified filter.
     *
     * @throws Exception Thrown in case of the failure.
     */

    public void testEventsInjectedIntoKafkaWithoutFilter() throws Exception {
        Map<String, String> srcProps = makeSourceProps(Utils.join(TOPICS, ","));

        srcProps.remove(IgniteSourceConstants.CACHE_FILTER_CLASS);

        doTest(srcProps, false);
    }

    /**
     * Tests data flow from injecting data into grid and transferring it to Kafka cluster.
     *
     * @throws Exception Thrown in case of the failure.
     */
    public void testEventsInjectedIntoKafka() throws Exception {
        doTest(makeSourceProps(Utils.join(TOPICS, ",")), true);
    }

    /**
     * Tests the source with the specified source configurations.
     *
     * @param srcProps Source properties.
     * @param conditioned Flag indicating whether filtering is enabled.
     * @throws Exception Fails if error.
     */
    private void doTest(Map<String, String> srcProps, boolean conditioned) throws Exception {
        FutureCallback<Herder.Created<ConnectorInfo>> cb = new FutureCallback<>(new Callback<Herder.Created<ConnectorInfo>>() {
            @Override
            public void onCompletion(Throwable error, Herder.Created<ConnectorInfo> info) {
                if (error != null)
                    throw new RuntimeException("Failed to create a job!", error);
            }
        });

        herder.putConnectorConfig(
            srcProps.get(ConnectorConfig.NAME_CONFIG),
            srcProps, true, cb);

        cb.get();

        // Ugh! To be sure Kafka Connect's worker thread is properly started...
        Thread.sleep(5000);

        final CountDownLatch latch = new CountDownLatch(EVENT_CNT * TOPICS.length);

        final IgnitePredicate<CacheEvent> locLsnr = new IgnitePredicate<CacheEvent>() {
            @Override public boolean apply(CacheEvent evt) {
                assert evt != null;

                latch.countDown();

                return true;
            }
        };

        grid.events(grid.cluster().forCacheNodes(CACHE_NAME)).localListen(locLsnr, EVT_CACHE_OBJECT_PUT);

        IgniteCache<String, String> cache = grid.cache(CACHE_NAME);

        assertEquals(0, cache.size(CachePeekMode.PRIMARY));

        Map<String, String> keyValMap = new HashMap<>(EVENT_CNT * TOPICS.length);

        // Produces events for the specified number of topics
        for (String topic : TOPICS)
            keyValMap.putAll(sendData(topic));

        // Checks all events are processed.
        assertTrue(latch.await(10, TimeUnit.SECONDS));

        grid.events(grid.cluster().forCacheNodes(CACHE_NAME)).stopLocalListen(locLsnr);

        assertEquals(EVENT_CNT, cache.size(CachePeekMode.PRIMARY));

        // Checks the events are transferred to Kafka broker.
        checkDataDelivered(conditioned);
    }

    /**
     * Sends messages to the grid.
     *
     * @param topic Topic name.
     * @return Map of key value messages.
     */
    private Map<String, String> sendData(String topic) throws IOException {
        Map<String, String> keyValMap = new HashMap<>();

        for (int evt = 0; evt < EVENT_CNT; evt++) {
            long runtime = System.currentTimeMillis();

            String key = topic + "_" + String.valueOf(evt);
            String msg = runtime + String.valueOf(evt);

            if (evt >= EVENT_CNT / 2)
                key = "conditioned_" + key;

            grid.cache(CACHE_NAME).put(key, msg);

            keyValMap.put(key, msg);
        }

        return keyValMap;
    }

    /**
     * Checks if events were delivered to Kafka server.
     *
     * @param conditioned Flag indicating whether filtering is enabled.
     */
    private void checkDataDelivered(boolean conditioned) {
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker.getBrokerAddress());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-grp");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1);
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 20000);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            "org.apache.ignite.stream.kafka.connect.serialization.CacheEventDeserializer");

        KafkaConsumer<String, CacheEvent> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Arrays.asList(TOPICS));

        int evtCnt = 0;
        long start = System.currentTimeMillis();

        try {
            while (false || (System.currentTimeMillis() - start) < 100000) {
                ConsumerRecords<String, CacheEvent> records = consumer.poll(100);
                for (ConsumerRecord<String, CacheEvent> record : records) {
                    System.out.println("Event: offset = " + record.offset() + ", key = " + record.key()
                        + ", value = " + record.value().toString());

                    if (conditioned) {
                        if (++evtCnt == EVENT_CNT / 2) {
                            return;
                        }
                    }
                    else {
                        if (++evtCnt == EVENT_CNT) {
                            return;
                        }
                    }
                }
            }
        }
        catch (WakeupException e) {
            // ignore for shutdown.
        }
        finally {
            consumer.close();

            if (conditioned)
                assertTrue(evtCnt == EVENT_CNT / 2);
            else
                assertTrue(evtCnt == EVENT_CNT);
        }
    }

    /**
     * Creates properties for test source connector.
     *
     * @param topics Topics.
     * @return Test source connector properties.
     */
    private Map<String, String> makeSourceProps(String topics) {
        Map<String, String> props = new HashMap<>();

        props.put(ConnectorConfig.TASKS_MAX_CONFIG, "1");
        props.put(ConnectorConfig.NAME_CONFIG, "test-src-connector");
        props.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, IgniteSourceConnectorMock.class.getName());
        props.put(IgniteSourceConstants.CACHE_NAME, "testCache");
        props.put(IgniteSourceConstants.CACHE_CFG_PATH, "example-ignite.xml");
        props.put(IgniteSourceConstants.TOPIC_NAME, "test1");
        props.put(IgniteSourceConstants.CACHE_EVENTS, "put");
        props.put(IgniteSourceConstants.CACHE_FILTER_CLASS, TestCacheEventFilter.class.getName());
        props.put(IgniteSourceConstants.INTL_BUF_SIZE, "1000000");

        return props;
    }

    /**
     * Creates properties for Kafka Connect workers.
     *
     * @return Worker configurations.
     */
    private Map<String, String> makeWorkerProps() throws IOException {
        Map<String, String> props = new HashMap<>();

        props.put(WorkerConfig.INTERNAL_KEY_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.storage.StringConverter");
        props.put(WorkerConfig.INTERNAL_VALUE_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.storage.StringConverter");
        props.put("internal.key.converter.schemas.enable", "false");
        props.put("internal.value.converter.schemas.enable", "false");
        props.put(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.storage.StringConverter");
        props.put(WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, "org.apache.ignite.stream.kafka.connect.serialization.CacheEventConverter");
        props.put("key.converter.schemas.enable", "false");
        props.put("value.converter.schemas.enable", "false");
        props.put(WorkerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker.getBrokerAddress());
        // fast flushing for testing.
        props.put(WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG, "10");

        return props;
    }
}
