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

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import java.util.HashMap;
import java.util.Map;

/**
 * Testing Storm spout.
 */
public class TestStormSpout implements IRichSpout {

    /** Field by which tuple data is obtained by the streamer. */
    private static final String IGNITE_TUPLE_FIELD = "ignite";

    /** Number of outgoing tuples. */
    public static final int CNT = 1000;

    /** Spout message prefix. */
    private static final String VAL_PREFIX = "v:";

    /** Spout output collector. */
    private SpoutOutputCollector collector;

    /** Map of messages to be sent. */
    private static HashMap<String, String> keyValMap;

    static {
        keyValMap = generateKeyValMap();
    }

    /**
     * Declares the output field for the component.
     *
     * @param outputFieldsDeclarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(IGNITE_TUPLE_FIELD));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
    }

    /**
     * Requests to emit a tuple to the output collector.
     */
    @Override
    public void nextTuple() {
        // Noop. Sent via mocked sources.
    }

    /**
     * Generates key,value pair to emit to bolt({@link StormStreamer}).
     *
     * @return Key, value pair.
     */
    public static HashMap<String, String> generateKeyValMap() {
        HashMap<String, String> keyValMap = new HashMap<>();

        for (int evt = 0; evt < CNT; evt++) {
            String key = Integer.toString(evt);
            String msg = VAL_PREFIX + key;

            keyValMap.put(key, msg);
        }

        return keyValMap;
    }

    public HashMap<String, String> getKeyValMap() {
        return keyValMap;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void ack(Object msgId) {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void fail(Object msgId) {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void activate() {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deactivate() {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
