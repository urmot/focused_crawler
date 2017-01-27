package com.focused_crawler.bolt;

import org.apache.storm.state.KeyValueState;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Arrays;

public class DocumentProbBolt extends BaseBasicBolt {
  private static final Logger LOG = LoggerFactory.getLogger(DocumentProbBolt.class);
  /* metadata formatted by  { prob, sid, size, url }*/
  Map<Integer, List> metaStore = new HashMap<Integer, List>();

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    switch (tuple.getSourceStreamId()) {
    case "docSizeStream":
      Integer oid = tuple.getInteger(0);
      List meta = tuple.getValues();
      meta.set(0, 0d);
      meta.add(0);
      metaStore.put(oid, meta);
      break;
    case "probStream":
      documentProb(tuple, collector);
      break;
    }
  }

  public void documentProb(Tuple tuple, BasicOutputCollector collector) {
    Integer oid = tuple.getInteger(0);
    List meta = metaStore.getOrDefault(oid, Arrays.asList(0d, 0, 100, "url", 0));
    Double prob = (Double) meta.get(0);
    Integer size = (Integer) meta.get(2);
    Integer count = (Integer) meta.get(4);
    prob += tuple.getDouble(1);
    count++;
    if (size.equals(count)) {
      collector.emit("updateStream", new Values(oid, meta.get(1), meta.get(3), prob));
    } else {
      meta.set(0, prob);
      meta.set(4, count);
      metaStore.put(oid, meta);
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declare) {
    declare.declareStream("updateStream", new Fields("oid", "sid", "url", "relevance"));
  }
}
