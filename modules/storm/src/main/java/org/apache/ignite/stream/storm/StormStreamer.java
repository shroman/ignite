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

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.Ignition;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.stream.StreamAdapter;

/**
 * Apache Storm streamer implemented as a Storm bolt.
 * Obtaining data from other bolts and spouts is done by field "ignite."
 */
public class StormStreamer<K, V> extends StreamAdapter<Tuple, K, V> implements IRichBolt {
    /** Default flush frequency. */
    private static final long DFLT_FLUSH_FREQ = 10000L;

    /** Field by which tuple data is obtained in topology. */
    private static final String IGNITE_TUPLE_FIELD = "ignite";

    /** Logger. */
    private IgniteLogger log;

    /** Executor used to submit storm streams. */
    private ExecutorService executor;

    /** Number of threads to process Storm streams. */
    private int threads = 1;

    /** Automatic flush frequency. */
    private long autoFlushFrequency = DFLT_FLUSH_FREQ;

    /** Enables overwriting existing values in cache. */
    private boolean allowOverwrite = false;

    /** Stopped. */
    private volatile boolean stopped = true;

    /** Storm output collector. */
    private OutputCollector collector;

    /** Ignite grid configuration file. */
    private String igniteConfigFile;

    /** Cache name. */
    private String cacheName;

    /**
     * Gets the cache name.
     *
     * @return Cache name.
     */
    public String getCacheName() {
        return cacheName;
    }

    /**
     * Sets the cache name.
     *
     * @param cacheName Cache name.
     */
    public void setCacheName(String cacheName) {
        this.cacheName = cacheName;
    }

    /**
     * Gets Ignite configuration file.
     *
     * @return Configuration file.
     */
    public String getIgniteConfigFile() {
        return igniteConfigFile;
    }

    /**
     * Specifies Ignite configuration file.
     *
     * @param igniteConfigFile Ignite config file.
     */
    public void setIgniteConfigFile(String igniteConfigFile) {
        this.igniteConfigFile = igniteConfigFile;
    }

    /**
     * Obtains the number of threads.
     *
     * @return Number of threads.
     */
    public int getThreads() {
        return threads;
    }

    /**
     * Specifies the number of threads.
     *
     * @param threads Number of threads.
     */
    public void setThreads(int threads) {
        this.threads = threads;
    }

    /**
     * Obtains data flush frequency.
     *
     * @return Flush frequency.
     */
    public long getAutoFlushFrequency() {
        return autoFlushFrequency;
    }

    /**
     * Specifies data flush frequency into the grid.
     *
     * @param autoFlushFrequency Flush frequency.
     */
    public void setAutoFlushFrequency(long autoFlushFrequency) {
        this.autoFlushFrequency = autoFlushFrequency;
    }

    /**
     * Obtains flag for enabling overwriting existing values in cache.
     *
     * @return True if overwriting is allowed, false otherwise.
     */
    public boolean getAllowOverwrite() {
        return allowOverwrite;
    }

    /**
     * Enables overwriting existing values in cache.
     *
     * @param allowOverwrite Flag value.
     */
    public void setAllowOverwrite(boolean allowOverwrite) {
        this.allowOverwrite = allowOverwrite;
    }

    /**
     * Starts streamer.
     *
     * @throws IgniteException If failed.
     */
    public void start() throws IgniteException {
        if (!stopped)
            throw new IgniteException("Attempted to start an already started Storm  Streamer");

        A.notNull(igniteConfigFile, "Ignite config file");
        A.notNull(cacheName, "Cache name");
        A.notNull(getIgnite(), "Ignite");
        A.notNull(getStreamer(), "Streamer");
        A.ensure(threads > 0, "threads > 0");

        log = getIgnite().log();

        executor = Executors.newFixedThreadPool(threads);
    }

    /**
     * Stops streamer.
     */
    public void stop() throws IgniteException {
        if (stopped)
            throw new IgniteException("Attempted to stop an already stopped Storm Streamer");

        stopped = true;

        getIgnite().<K, V>dataStreamer(cacheName).close(true);

        executor.shutdown();

        getIgnite().close();
    }

    /**
     * Initializes Ignite client instance from a configuration file and declares the output collector of the bolt.
     *
     * @param map Map derived from topology.
     * @param topologyContext Context topology in storm.
     * @param collector Output collector.
     */
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        if (stopped) {
            setIgnite(Ignition.start(igniteConfigFile));

            IgniteDataStreamer dataStreamer = getIgnite().<K, V>dataStreamer(cacheName);
            dataStreamer.autoFlushFrequency(autoFlushFrequency);
            dataStreamer.allowOverwrite(allowOverwrite);

            setStreamer(dataStreamer);

            start();

            stopped = false;
        }

        this.collector = collector;
    }

    /**
     * Transfers data into grid.
     *
     * @param tuple Storm tuple.
     */
    @Override
    public void execute(Tuple tuple) {
        if (stopped)
            return;

        final Map<K, V> igniteGrid = (Map)tuple.getValueByField(IGNITE_TUPLE_FIELD);

        if (log.isDebugEnabled()) {
            log.debug("Tuple id: " + tuple.getMessageId());
        }

        executor.submit(new Runnable() {
            @Override public void run() {
                for (K k : igniteGrid.keySet()) {
                    try {
                        if (log.isDebugEnabled())
                            log.debug("Tuple from storm: " + k + ", " + igniteGrid.get(k));

                        getStreamer().addData(k, igniteGrid.get(k));
                    }
                    catch (Exception e) {
                        if (log.isDebugEnabled())
                            log.debug(e.toString());
                    }
                }
            }
        });

        collector.ack(tuple);
    }

    /**
     * Cleans up the streamer when the bolt is going to shutdown.
     */
    @Override
    public void cleanup() {
        stop();
    }

    /**
     * Normally declares output fields for the stream of the topology. Empty because we have no tuples for any further
     * processing.
     *
     * @param declarer OutputFieldsDeclarer.
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // Noop.
    }

    /**
     * Not used.
     *
     * @return
     */
    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
