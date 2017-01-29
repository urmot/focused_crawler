/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.focused_crawler;

import com.focused_crawler.spout.RandomIntegerSpout;
import com.focused_crawler.spout.SeedSpout;
import com.focused_crawler.bolt.StatefulPriorityControllerBolt;
import com.focused_crawler.bolt.StatefulDocumentProbBolt;
import com.focused_crawler.bolt.StatefulLinkParserBolt;
import com.focused_crawler.bolt.CrawlerBolt;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.state.KeyValueState;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseStatefulBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.List;
import java.util.Arrays;
import java.util.Random;

/**
* An example topology that demonstrates the use of {@link org.apache.storm.topology.IStatefulBolt}
* to manage state. To run the example,
* <pre>
* $ storm jar examples/storm-starter/storm-starter-topologies-*.jar storm.starter.StatefulTopology statetopology
* </pre>
* <p/>
* The default state used is 'InMemoryKeyValueState' which does not persist the state across restarts. You could use
* 'RedisKeyValueState' to test state persistence by setting below property in conf/storm.yaml
* <pre>
* topology.state.provider: org.apache.storm.redis.state.RedisKeyValueStateProvider
* </pre>
* <p/>
* You should also start a local redis instance before running the 'storm jar' command. The default
* RedisKeyValueStateProvider parameters can be overridden in conf/storm.yaml, for e.g.
* <p/>
* <pre>
* topology.state.provider.config: '{"keyClass":"...", "valueClass":"...",
*                                   "keySerializerClass":"...", "valueSerializerClass":"...",
*                                   "jedisPoolConfig":{"host":"localhost", "port":6379,
*                                      "timeout":2000, "database":0, "password":"xyz"}}'
*
* </pre>
* </p>
*/
public class StatefulTopology {
  private static final Logger LOG = LoggerFactory.getLogger(StatefulTopology.class);
  /**
  * A bolt that uses {@link KeyValueState} to save its state.
  */
  private static class StatefulSumBolt extends BaseStatefulBolt<KeyValueState<String, Object>> {
    String name;
    KeyValueState<String, Object> kvState;
    List store;
    Long sum;
    private OutputCollector collector;

    StatefulSumBolt(String name) {
      this.name = name;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
      this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
      sum += ((Number) input.getValueByField("value")).longValue();
      LOG.debug("{} sum = {}", name, sum);
      store.set(0, sum);
      kvState.put("sum", store);
      collector.emit(input, new Values(sum));
      collector.ack(input);
    }

    @Override
    public void initState(KeyValueState<String, Object> state) {
      kvState = state;
      store = (List) kvState.get("sum", Arrays.asList(0l));
      sum = (Long) store.get(0);
      LOG.debug("Initstate, sum from saved state = {} ", sum);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("value"));
    }
  }

  public static class PrinterBolt extends BaseBasicBolt {
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
      System.out.println(tuple);
      collector.emit(tuple.getValues());
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
      ofd.declare(new Fields("value"));
    }

  }

  public static class SampleBolt extends BaseRichBolt {
    private Random rand;
    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
      this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
      collector.emit("docSizeStream", tuple, new Values(0, 0, 30, "http://example.com"));
      for (int i = 0; i < 30; i++) {
        Utils.sleep(100);
        collector.emit("probStream", tuple, new Values(0, 0.01));
      }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
      ofd.declareStream("probStream", new Fields("oid", "prob"));
      ofd.declareStream("docSizeStream", new Fields("oid", "sid", "size", "url"));
    }

  }

  public static void main(String[] args) throws Exception {
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("spout", new SeedSpout());
    builder.setBolt("sample", new SampleBolt(), 1)
           .shuffleGrouping("spout", "requestStream");
    builder.setBolt("testee", new StatefulDocumentProbBolt(), 1)
           .shuffleGrouping("sample", "probStream")
           .shuffleGrouping("sample", "docSizeStream");
    builder.setBolt("printer", new PrinterBolt(), 2)
           .shuffleGrouping("sample", "docSizeStream")
           .shuffleGrouping("sample", "probStream")
           .shuffleGrouping("testee", "updateStream");
    Config conf = new Config();
    conf.setDebug(false);

    if (args != null && args.length > 0) {
      conf.setNumWorkers(1);
      StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
    } else {
      LocalCluster cluster = new LocalCluster();
      StormTopology topology = builder.createTopology();
      cluster.submitTopology("test", conf, topology);
      Utils.sleep(40000);
      cluster.killTopology("test");
      cluster.shutdown();
    }
  }
}
