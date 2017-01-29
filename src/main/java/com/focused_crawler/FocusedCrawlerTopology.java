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
import com.focused_crawler.bolt.PriorityControllerBolt;
import com.focused_crawler.bolt.CrawlerBolt;
import com.focused_crawler.bolt.StatefulPriorityControllerBolt;
import com.focused_crawler.bolt.StatefulDocumentProbBolt;
import com.focused_crawler.bolt.LinkParserBolt;
import com.focused_crawler.bolt.StemmerBolt;
import com.focused_crawler.bolt.DocumentProbBolt;
import com.focused_crawler.bolt.WordProbBolt;
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
    TopologyBuilder builder = new TopologyBuilder();
    Kafka kafka = new Kafka("localhost:9092", "links");

    builder.setSpout("spout", kafka.getSpout(), 1);
    builder.setBolt("priorityController", new PriorityControllerBolt(), 1)
           .shuffleGrouping("spout");
    builder.setBolt("crawler", new CrawlerBolt(), 1)
           .fieldsGrouping("priorityController", "linkStream", new Fields("sid"));
    // builder.setBolt("documentParser", new DocumentParserBolt(), 1)
    //       //  .setNumTasks(16)
    //        .localOrShuffleGrouping("crawler", "documentStream");
    // builder.setBolt("linkParser", new LinkParserBolt(), 1)
    //        .fieldsGrouping("documentParser", "linkStream", new Fields("src"));
    //       //  .fieldsGrouping("documentProb", "updateStream", new Fields("oid"));
    // builder.setBolt("stemmer", new StemmerBolt(), 1)
    //        .shuffleGrouping("documentParser", "wordStream");
    // builder.setBolt("wordProb", new WordProbBolt(args[0]), 1)
    //        .shuffleGrouping("stemmer", "wordStream");
    // builder.setBolt("documentProb", new StatefulDocumentProbBolt(), 1)
    //        .fieldsGrouping("stemmer", "probStream", new Fields("oid"))
    //        .fieldsGrouping("wordProb", "probStream", new Fields("oid"))
    //        .fieldsGrouping("documentParser", "docSizeStream", new Fields("oid"));
    // builder.setBolt("updateCrawls", updateCrawlBolt, 1)
    //        .shuffleGrouping("documentProb", "updateStream");
          //  .shuffleGrouping("crawler", "updateStream");
    // builder.setBolt("benchmark", new BenchMarkBolt(), 1)
    //        .shuffleGrouping("documentProb", "updateStream");
    builder.setBolt("print", new PrinterBolt(), 1)
           .shuffleGrouping("priorityController", "linkStream")
           .shuffleGrouping("crawler", "documentStream");
          //  .shuffleGrouping("documentParser", "linkStream")
          // .shuffleGrouping("documentParser", "docSizeStream")
          // .shuffleGrouping("stemmer", "wordStream")
          // .shuffleGrouping("stemmer", "probStream")
          // .shuffleGrouping("wordProb", "probStream")
          // .shuffleGrouping("documentParser", "docSizeStream")
          // .shuffleGrouping("documentProb", "updateStream");
    // builder.setBolt("writeFile", new WriteFileBolt(), 1)
    //        .setNumTasks(8)
    //        .fieldsGrouping("crawler", "documentStream", new Fields("sid"));

    Config conf = new Config();
    conf.setDebug(false);

    if (args.length > 1) {
      conf.setNumWorkers(8);
      StormSubmitter.submitTopologyWithProgressBar(args[1], conf, builder.createTopology());
    } else {
      conf.setMaxTaskParallelism(3);
       LocalCluster cluster = new LocalCluster();
       cluster.submitTopology("focused-crawler", conf, builder.createTopology());
       Thread.sleep(10000);
       cluster.shutdown();
    }
  }
}
