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
import com.focused_crawler.bolt.DocumentParserBolt;
import com.focused_crawler.bolt.StatefulDocumentProbBolt;
import com.focused_crawler.bolt.LinkParserBolt;
import com.focused_crawler.bolt.StemmerBolt;
import com.focused_crawler.bolt.DocumentProbBolt;
import com.focused_crawler.bolt.WordProbBolt;
import com.focused_crawler.bolt.BenchMarkBolt;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.LocalCluster;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.task.ShellBolt;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.spout.Scheme;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.apache.storm.jdbc.common.Column;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.HikariCPConnectionProvider;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;
import org.apache.storm.jdbc.bolt.JdbcInsertBolt;

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

  public static class CrawlerBolt extends ShellBolt implements IRichBolt {
    public CrawlerBolt() {
      super("node", "crawler.js");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declareStream("documentStream", new Fields("oid", "sid", "url", "html"));
      declarer.declareStream("updateStream", new Fields("oid", "sid", "url", "relevance"));
      declarer.declareStream("requestStream", new Fields("request"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
      return null;
    }
  }

  public static void main(String[] args) throws Exception {

    TopologyBuilder builder = new TopologyBuilder();

    Map hikariConfigMap = new HashMap();
    hikariConfigMap.put("dataSourceClassName","com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
    hikariConfigMap.put("dataSource.url", "jdbc:mysql://dbserver/focused_crawler");
    hikariConfigMap.put("dataSource.user", "fc");
    hikariConfigMap.put("dataSource.password", "focused_crawler");
    ConnectionProvider connectionProvider = new HikariCPConnectionProvider(hikariConfigMap);

    List<Column> crawlSchema = new ArrayList<Column>();
    crawlSchema.add(new Column("oid", java.sql.Types.INTEGER));
    crawlSchema.add(new Column("sid", java.sql.Types.INTEGER));
    crawlSchema.add(new Column("url", java.sql.Types.VARCHAR));
    crawlSchema.add(new Column("relevance", java.sql.Types.DOUBLE));
    JdbcMapper crawlJdbcMapper = new SimpleJdbcMapper(crawlSchema);
    JdbcInsertBolt updateCrawlBolt = new JdbcInsertBolt(connectionProvider, crawlJdbcMapper)
      .withInsertQuery("insert into crawl (oid, sid, url, relevance) values (?, ?, ?, ?) on duplicate key update numtries = numtries + 1")
      .withQueryTimeoutSecs(30);

    builder.setSpout("spout", new SeedSpout(), 1);
    builder.setBolt("priorityController", new PriorityControllerBolt(), 1)
           .shuffleGrouping("spout", "requestStream")
           .shuffleGrouping("crawler", "requestStream")
           .shuffleGrouping("linkParser", "linkStream");
    builder.setBolt("crawler", new CrawlerBolt(), 1)
           .fieldsGrouping("priorityController", "linkStream", new Fields("sid"));
    builder.setBolt("documentParser", new DocumentParserBolt(), 1)
          //  .setNumTasks(16)
           .localOrShuffleGrouping("crawler", "documentStream");
    builder.setBolt("linkParser", new LinkParserBolt(), 1)
           .fieldsGrouping("documentParser", "linkStream", new Fields("src"))
           .fieldsGrouping("documentProb", "updateStream", new Fields("oid"));
    builder.setBolt("stemmer", new StemmerBolt(), 1)
           .shuffleGrouping("documentParser", "wordStream");
    builder.setBolt("wordProb", new WordProbBolt(args[0]), 1)
           .shuffleGrouping("stemmer", "wordStream");
    builder.setBolt("documentProb", new DocumentProbBolt(), 1)
           .fieldsGrouping("stemmer", "probStream", new Fields("oid"))
           .fieldsGrouping("wordProb", "probStream", new Fields("oid"))
           .fieldsGrouping("documentParser", "docSizeStream", new Fields("oid"));
    builder.setBolt("updateCrawls", updateCrawlBolt, 1)
           .shuffleGrouping("documentProb", "updateStream");
          //  .shuffleGrouping("crawler", "updateStream");
    builder.setBolt("benchmark", new BenchMarkBolt(), 1)
           .shuffleGrouping("documentProb", "updateStream");
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
       Thread.sleep(100000);
       cluster.shutdown();
    }
  }
}
