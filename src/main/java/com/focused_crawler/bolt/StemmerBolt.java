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

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    Integer oid = tuple.getInteger(0);
    String word = tuple.getString(1).toLowerCase();
    Stemmer s = new Stemmer();
    s.add(word.toCharArray(), word.length());
    s.stem();
    collector.emit("wordStream", new Values(oid, s.toString(), tuple.getInteger(2)));
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declare) {
    declare.declareStream("wordStream", new Fields("oid", "word", "count"));
  }
}
