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

import com.focused_crawler.spout.SeedSpout;
import com.focused_crawler.bolt.KafkaParserBolt;
import com.focused_crawler.bolt.FilterBolt;
import com.focused_crawler.bolt.StatefulPriorityControllerBolt;
import com.focused_crawler.bolt.CrawlerBolt;
import com.focused_crawler.bolt.DocumentParserBolt;
import com.focused_crawler.bolt.LinkParserBolt;
import com.focused_crawler.bolt.StemmerBolt;
import com.focused_crawler.bolt.WordProbBolt;
import com.focused_crawler.bolt.StatefulDocumentProbBolt;
import com.focused_crawler.bolt.MysqlInsertBolt;
import com.focused_crawler.bolt.BenchMarkBolt;
import com.focused_crawler.bolt.PrinterBolt;
import com.focused_crawler.utils.Kafka;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.LocalCluster;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.task.ShellBolt;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;
import org.apache.storm.jdbc.common.Column;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.net.URL;
import java.net.URLConnection;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.UUID;

import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.spout.SchemeAsMultiScheme;

/**
 * This topology demonstrates Storm's stream groupings and multilang capabilities.
 */
public class FocusedCrawlerTopology {

  public static class WriteFileBolt extends ShellBolt implements IRichBolt {
    public WriteFileBolt() {
      super("node", "writefile.js");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
      return null;
    }
  }

  public static void main(String[] args) throws Exception {
    String[] category = args[0].split("\\.");
    Integer targetID = category[category.length - 1].hashCode();
    TopologyBuilder builder = new TopologyBuilder();
    Kafka kafka = new Kafka("kafka", "test");
    MysqlInsertBolt mysqlInsertBolt = new MysqlInsertBolt();

    builder.setSpout("kafkaSpout", kafka.getSpout(), 1);
    builder.setSpout("requestSpout", new SeedSpout(), 1);
    builder.setBolt("kafkaParser", new KafkaParserBolt(), 2)
           .shuffleGrouping("kafkaSpout");
    builder.setBolt("filter", new FilterBolt(), 2)
           .fieldsGrouping("kafkaParser", new Fields("oid"));
    builder.setBolt("priorityController", new StatefulPriorityControllerBolt(), 1)
           .shuffleGrouping("filter", "linkStream")
           .shuffleGrouping("requestSpout", "requestStream");
    builder.setBolt("crawler", new CrawlerBolt(), 2)
           .directGrouping("priorityController", "linkStream");
    builder.setBolt("documentParser", new DocumentParserBolt(), 2)
           .localOrShuffleGrouping("crawler", "documentStream");
    builder.setBolt("linkParser", new LinkParserBolt(targetID), 2)
           .fieldsGrouping("documentParser", "linkStream", new Fields("src"))
           .fieldsGrouping("documentProb", "updateStream", new Fields("oid"));
    builder.setBolt("stemmer", new StemmerBolt(), 2)
           .shuffleGrouping("documentParser", "wordStream");
    builder.setBolt("wordProb", new WordProbBolt(targetID), 2)
           .shuffleGrouping("stemmer", "wordStream");
    builder.setBolt("documentProb", new StatefulDocumentProbBolt(targetID), 2)
           .fieldsGrouping("wordProb", "probStream", new Fields("oid"))
           .fieldsGrouping("documentParser", "docSizeStream", new Fields("oid"));
    builder.setBolt("kafkaBolt", kafka.getBolt(), 1)
           .shuffleGrouping("linkParser", "linkStream");
    builder.setBolt("mysqlBolt", mysqlInsertBolt.getBolt(), 1)
           .shuffleGrouping("documentProb", "updateStream");
    builder.setBolt("benchmark", new BenchMarkBolt(), 1)
           .shuffleGrouping("documentProb", "updateStream");
    // builder.setBolt("printer", new PrinterBolt(), 1)
    //        .shuffleGrouping("priorityController", "linkStream")
    //        .shuffleGrouping("documentProb", "updateStream");

    Config conf = new Config();
    conf.setDebug(false);

    if (args.length > 1) {
      conf.setNumWorkers(2);
      StormSubmitter.submitTopologyWithProgressBar(args[1], conf, builder.createTopology());
    } else {
      conf.setMaxTaskParallelism(2);
       LocalCluster cluster = new LocalCluster();
       cluster.submitTopology("focused-crawler", conf, builder.createTopology());
       Thread.sleep(300000);
       cluster.shutdown();
    }
  }
}
