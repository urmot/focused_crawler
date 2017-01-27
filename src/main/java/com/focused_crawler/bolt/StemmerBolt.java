package com.focused_crawler.bolt;

import com.focused_crawler.util.Stemmer;

import org.apache.storm.Config;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.utils.Utils;
import org.apache.storm.blobstore.ClientBlobStore;
import org.apache.storm.blobstore.InputStreamWithMeta;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.KeyNotFoundException;

import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;

public class StemmerBolt extends BaseBasicBolt {
  public Set stopwords = new HashSet<String>();

  @Override
  public void prepare(Map stormConf, TopologyContext context) {
    Config theconf = new Config();
    theconf.putAll(Utils.readStormConfig());
    ClientBlobStore clientBlobStore = Utils.getClientBlobStore(theconf);
    try {
      InputStreamWithMeta blobInputStream = clientBlobStore.getBlob("stopwords");
      BufferedReader r = new BufferedReader(new InputStreamReader(blobInputStream));
      stopwords.addAll(Arrays.asList(r.readLine().split(",")));
      stopwords.add("");
      r.close();
    } catch (IOException | AuthorizationException | KeyNotFoundException exp) {
      throw new RuntimeException(exp);
    }
  }

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    Integer oid = tuple.getInteger(0);
    String word = tuple.getString(1).toLowerCase();
    if (word.length() > 32 || stopwords.contains(word)) {
      collector.emit("probStream", new Values(oid, 0d));
    } else {
      Stemmer s = new Stemmer();
      s.add(word.toCharArray(), word.length());
      s.stem();
      collector.emit("wordStream", new Values(oid, s.toString()));
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declare) {
    declare.declareStream("probStream", new Fields("oid", "prob"));
    declare.declareStream("wordStream", new Fields("oid", "word"));
  }
}
