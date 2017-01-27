package com.focused_crawler.bolt;

import org.apache.storm.state.KeyValueState;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseStatefulBolt;
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

public class StatefulDocumentProbBolt extends BaseStatefulBolt<KeyValueState<Integer, List>> {
  private static final Logger LOG = LoggerFactory.getLogger(DocumentProbBolt.class);
  private OutputCollector collector;
  /* metadata formatted by  { prob, sid, size, url }*/
  KeyValueState<Integer, List> metaStore;
  Map<Integer, Integer> counter = new HashMap<Integer, Integer>();

  @Override
  public void initState(KeyValueState<Integer, List> state) {
    metaStore = state;
    System.out.println("initState");
  }

  @Override
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    this.collector = collector;
    System.out.println("prepare");
  }

  @Override
  public void execute(Tuple tuple) {
    System.out.println("execute");
    switch (tuple.getSourceStreamId()) {
    case "docSizeStream":
      Integer oid = tuple.getInteger(0);
      List meta = tuple.getValues();
      meta.set(0, 0d);
      counter.put(oid, 0);
      metaStore.put(oid, meta);
      collector.ack(tuple);
      break;
    case "probStream":
      documentProb(tuple);
      collector.ack(tuple);
      break;
    }
  }

  public void documentProb(Tuple tuple) {
    Integer oid = tuple.getInteger(0);
    List meta = metaStore.get(oid);
    Double prob = (Double) meta.get(0);
    Integer size = (Integer) meta.get(2);
    Integer count = counter.get(oid);
    prob += tuple.getDouble(1);
    count++;
    if (size.equals(count)) {
      collector.emit("updateStream", tuple, new Values(oid, meta.get(1), meta.get(3), prob));
    } else {
      meta.set(0, prob);
      counter.put(oid, count);
      metaStore.put(oid, meta);
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declare) {
    declare.declareStream("updateStream", new Fields("oid", "sid", "url", "relevance"));
  }
}
