package com.focused_crawler.bolt;

import org.apache.storm.state.KeyValueState;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseStatefulBolt;
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

public class StatefulDocumentProbBolt extends BaseStatefulBolt<KeyValueState<String, Map>> {
  private static final Logger LOG = LoggerFactory.getLogger(DocumentProbBolt.class);
  private OutputCollector collector;
  /* metadata formatted by  { oid, sid, size, url, prob, count }*/
  KeyValueState<String, Map> kvState;
  Map<Integer, List> metaStore;
  Map<Integer, List> linkStore;

  @Override
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    this.collector = collector;
  }

  @Override
  public void initState(KeyValueState<String, Map> state) {
    kvState = state;
    metaStore = kvState.get("metaStore", new HashMap<Integer, List>());
    linkStore = kvState.get("linkStore", new HashMap<Integer, List>());
  }

  @Override
  public void execute(Tuple tuple) {
    switch (tuple.getSourceStreamId()) {
    case "docSizeStream":
      Integer oid = tuple.getInteger(0);
      List meta = tuple.getValues();
      meta.add(4, 0d);
      meta.add(5, 0);
      metaStore.put(oid, meta);
      kvState.put("metaStore", metaStore);
      break;
    case "probStream":
      documentProb(tuple);
      break;
    case "linkStream":
      String link = tuple.getString(0);
      Integer src = tuple.getInteger(1);
      List links = linkStore.getOrDefault(src, new ArrayList());
      links.add(link);
      linkStore.put(src, links);
      kvState.put("linkStore", linkStore);
      break;
    }
    collector.ack(tuple);
  }

  public void documentProb(Tuple tuple) {
    Integer oid = tuple.getInteger(0);
    List meta = (List) metaStore.getOrDefault(oid, Arrays.asList(oid, 0, 1000, "http://example.com", 0d, 0));
    Integer size = (Integer) meta.get(2);
    Double prob = (Double) meta.get(4);
    Integer count = (Integer) meta.get(5);
    prob += tuple.getDouble(1);
    count++;
    if (size.equals(count)) {
      System.out.println("count/size: " + count + "/" + size);
      collector.emit("updateStream", tuple, new Values(oid, meta.get(1), meta.get(3), prob));
      List links = linkStore.get(oid);
      if (links != null)
        for (Object link : links) {
          String l = (String) link;
          collector.emit("linkStream", tuple, new Values("key", l+"|"+prob.toString()));
        }
    } else {
      meta.set(4, prob);
      meta.set(5, count);
      metaStore.put(oid, meta);
      kvState.put("metaStore", metaStore);
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declare) {
    declare.declareStream("updateStream", new Fields("oid", "sid", "url", "relevance"));
    declare.declareStream("linkStream", new Fields("key", "message"));
  }
}
