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

import com.focused_crawler.bolt.PriorityControllerBolt;
import com.focused_crawler.bolt.DocumentParserBolt;
import com.focused_crawler.util.Stemmer;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.spout.Scheme;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.apache.storm.jdbc.common.Column;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.HikariCPConnectionProvider;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcLookupMapper;
import org.apache.storm.jdbc.bolt.JdbcInsertBolt;
import org.apache.storm.jdbc.bolt.JdbcLookupBolt;
import org.apache.storm.blobstore.ClientBlobStore;
import org.apache.storm.blobstore.InputStreamWithMeta;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.KeyNotFoundException;
import org.jsoup.Jsoup;
import org.jsoup.select.Elements;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Queue;
import java.util.ArrayDeque;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;

/**
 * This topology demonstrates Storm's stream groupings and multilang capabilities.
 */
public class ClassifierTopology {

  public static class SeedSpout extends BaseRichSpout {
    SpoutOutputCollector _collector;
    Queue<List> queue = new ArrayDeque<List>();

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
      _collector = collector;
      Config theconf = new Config();
      theconf.putAll(Utils.readStormConfig());
      ClientBlobStore clientBlobStore = Utils.getClientBlobStore(theconf);
      try {
        InputStreamWithMeta blobInputStream = clientBlobStore.getBlob("training");
        BufferedReader r = new BufferedReader(new InputStreamReader(blobInputStream));
        r.lines().forEach(line -> queue.add(Arrays.asList(line.split(","))));
        r.close();
      } catch (IOException | AuthorizationException | KeyNotFoundException exp) {
        throw new RuntimeException(exp);
      }
    }

    @Override
    public void nextTuple() {
      Utils.sleep(100);
      List training = queue.poll();
      if (training != null) _collector.emit("trainingStream", training);
    }

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declareStream("trainingStream", new Fields("category", "url"));
    }
  }

  public static class Crawler extends BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(Crawler.class);

    String userAgent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_1) AppleWebKit/602.2.14 (KHTML, like Gecko) Version/10.0.1 Safari/602.2.14";
    OutputCollector _collector;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
      _collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
      try {
        URLConnection url = new URL(tuple.getString(1)).openConnection();
        url.setRequestProperty("User-Agent", userAgent);
        Document document = Jsoup.parse(url.getInputStream(), "UTF-8", tuple.getString(1));
        _collector.emit("documentStream", new Values(tuple.getString(0), document.text()));
      } catch (IOException exp) {
        LOG.debug("http/get failed: " + tuple.getString(1));
        LOG.debug(exp.getMessage());
      }
      _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declareStream("documentStream", new Fields("category", "document"));
    }
  }

  public static class DocumentParser extends DocumentParserBolt {
    // 
    // @Override
    // public void execute(Tuple tuple, BasicOutputCollector collector) {
    //   List<String> words= new ArrayList<String>(Arrays.asList(tuple.getString(1).split("\\W")));
    //   words.removeAll(stopwords);
    //   java.util.Iterator<String> iter = words.iterator();
    //   while (iter.hasNext()) if (iter.next().length() >= 52) iter.remove();
    //   words.stream().forEach(word -> {
    //     collector.emit("wordStream", new Values(tuple.getString(0), word));
    //   });
    // }
    //
    // @Override
    // public void declareOutputFields(OutputFieldsDeclarer declarer) {
    //   declarer.declareStream("wordStream", new Fields("category", "word"));
    // }
  }

  public static class StemmerBolt extends BaseBasicBolt {

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
      String word = tuple.getString(1).toLowerCase();
      Stemmer s = new Stemmer();
      s.add(word.toCharArray(), word.length());
      s.stem();
      collector.emit("wordStream", new Values(tuple.getString(0), s.toString()));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declare) {
      declare.declareStream("wordStream", new Fields("category", "word"));
    }
  }

  public static class WordCount extends BaseBasicBolt {
    Map<String, Map<String, Integer>> catStore = new HashMap<String, Map<String, Integer>>();

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
      Map<String, Integer> counts = catStore.get(tuple.getString(0));
      if (counts == null) counts = new HashMap<String, Integer>();
      Integer count = counts.get(tuple.getString(1));
      if (count == null) count = 0;
      count++;
      counts.put(tuple.getString(1), count);
      catStore.put(tuple.getString(0), counts);
      collector.emit(new Values(tuple.getString(0), tuple.getString(1), count));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("category", "word", "count"));
    }
  }

  public static void main(String[] args) throws Exception {

    TopologyBuilder builder = new TopologyBuilder();

    Map hikariConfigMap = new HashMap();
    hikariConfigMap.put("dataSourceClassName","com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
    hikariConfigMap.put("dataSource.url", "jdbc:mysql://slave1/focused_crawler");
    hikariConfigMap.put("dataSource.user", "fc");
    hikariConfigMap.put("dataSource.password", "focused_crawler");
    ConnectionProvider connectionProvider = new HikariCPConnectionProvider(hikariConfigMap);

    JdbcMapper freqJdbcMapper = new SimpleJdbcMapper("frequency", connectionProvider);
    JdbcInsertBolt insertFreqBolt = new JdbcInsertBolt(connectionProvider, freqJdbcMapper)
      .withInsertQuery("insert into frequency values (?, ?, ?) on duplicate key update count = values(count)")
      .withQueryTimeoutSecs(30);

    builder.setSpout("spout", new SeedSpout(), 1);
    builder.setBolt("crawler", new Crawler(), 16)
           .shuffleGrouping("spout", "trainingStream");
    builder.setBolt("documentParser", new DocumentParser(), 8)
           .noneGrouping("crawler", "documentStream");
    builder.setBolt("stemmer", new StemmerBolt(), 16)
           .shuffleGrouping("documentParser", "wordStream");
    builder.setBolt("wordCount", new WordCount(), 16)
           .fieldsGrouping("stemmer", "wordStream", new Fields("category"));
    builder.setBolt("insertFreq", insertFreqBolt, 16)
           .shuffleGrouping("wordCount");

    Config conf = new Config();
    conf.setDebug(true);
    conf.setNumWorkers(16);

    StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
  }
}
