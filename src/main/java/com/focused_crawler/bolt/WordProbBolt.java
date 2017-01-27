package com.focused_crawler.bolt;

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
import org.apache.storm.blobstore.AtomicOutputStream;
import org.apache.storm.blobstore.InputStreamWithMeta;
import org.apache.storm.blobstore.BlobStoreAclHandler;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.KeyNotFoundException;

import java.util.Map;
import java.util.HashMap;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;


public class WordProbBolt extends BaseBasicBolt {
  String category;
  Map<String, Double> probTable = new HashMap<String, Double>();

  public WordProbBolt(String cat) {
    category = cat;
  }

  @Override
  public void prepare(Map stormConf, TopologyContext context) {
    Config theconf = new Config();
    theconf.putAll(Utils.readStormConfig());
    ClientBlobStore clientBlobStore = Utils.getClientBlobStore(theconf);
    try {
      InputStreamWithMeta blobInputStream = clientBlobStore.getBlob(category);
      BufferedReader r = new BufferedReader(new InputStreamReader(blobInputStream));
      r.lines().forEach(line -> {
        String[] ary = line.split(":");
        probTable.put(ary[0], Double.parseDouble(ary[1]));
      });
      r.close();
    } catch (IOException | AuthorizationException | KeyNotFoundException exp) {
      throw new RuntimeException(exp);
    }
  }

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    Integer oid = tuple.getInteger(0);
    String word = tuple.getString(1);
    Double prob = probTable.get(word);
    if (prob == null) prob = probTable.get("_Smoothing");
    collector.emit("probStream", new Values(oid, prob));
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declare) {
    declare.declareStream("probStream", new Fields("oid", "prob"));
  }
}
