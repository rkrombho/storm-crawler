/**
 * Licensed to DigitalPebble Ltd under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * DigitalPebble licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.digitalpebble.storm.crawler.kafka.spout;

import java.util.*;

import backtype.storm.spout.SchemeAsMultiScheme;
import storm.kafka.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;


import com.digitalpebble.storm.crawler.util.ConfUtils;



/**
 * Wrapper Spout for the official Storm-Kafka spout.
 * Just instantiates the Spout according to configuration parameters
 * from the storm-crawler config and delegates all implementations to the
 * official Spout.
 * Uses a simple StringScheme because Kafka logs are expected to be URL strings
 */
public class KafkaSpout extends BaseRichSpout {

    private static final Logger LOG = LoggerFactory
            .getLogger(KafkaSpout.class);

    private static final String ZKHostsParamName = "kafka.spout.zk.hosts";
    private static final String ZKTopicParamName = "kafka.spout.zk.topic";

    private BrokerHosts brokerHosts;

    private SpoutConfig spoutConfig;

    private storm.kafka.KafkaSpout kafkaSpout;

    private SpoutOutputCollector _collector;


    @Override
    public void open(Map stormConf, TopologyContext context,
                     SpoutOutputCollector collector) {
        String zkHosts = ConfUtils.getString(stormConf, ZKHostsParamName,
                "localhost:2181");

        brokerHosts = new ZkHosts(zkHosts);

        String topic = ConfUtils.getString(stormConf, ZKTopicParamName,
                "storm-crawler");

        //use a randomly generated UUID as a client ID
        //TODO: make the clientId configurable
        spoutConfig = new SpoutConfig(brokerHosts, topic , "/" + topic, UUID.randomUUID().toString());
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        kafkaSpout = new storm.kafka.KafkaSpout(spoutConfig);

        _collector = collector;
    }

    @Override
    public void close() {
        kafkaSpout.close();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        kafkaSpout.declareOutputFields(declarer);
    }

    @Override
    public void nextTuple() {
        kafkaSpout.nextTuple();
    }

    @Override
    public void ack(Object msgId) {
        kafkaSpout.ack(msgId);
    }

    @Override
    public void fail(Object msgId) {
        kafkaSpout.fail(msgId);
    }

}
