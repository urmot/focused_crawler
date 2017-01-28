package com.focused_crawler.bolt;

import org.apache.storm.state.KeyValueState;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseStatefulBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;

public class StatefulLinkParserBolt extends BaseStatefulBolt<KeyValueState<String, Map>> {
  private OutputCollector collector;
  KeyValueState<String, Map> kvState;
  Map<Integer, List> linkStore = new HashMap<Integer, List>();

  @Override
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    this.collector = collector;
  }

  @Override
  public void initState(KeyValueState<String, Map> state) {
    kvState = state;
    linkStore = kvState.get("linkStore", new HashMap<Integer, List>());
  }

  @Override
  public void execute(Tuple tuple) {
    switch (tuple.getSourceStreamId()) {
    case "linkStream":
      storeLink(tuple);
      break;
    case "updateStream":
      Integer src = tuple.getInteger(0);
      Double relevance = tuple.getDouble(3);
      List links = linkStore.remove(src);
      if (links != null)
        for (Object link : links) {
          List l = (List) link;
          l.set(3, relevance);
          collector.emit("linkStream", tuple, l);
        }
      kvState.put("linkStore", linkStore);
      break;
    }
    collector.ack(tuple);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declareStream("linkStream", new Fields("oid", "sid", "url", "relevance"));
  }

  public void storeLink(Tuple tuple) {
    Integer src = tuple.getInteger(0);
    String link = tuple.getString(1);
    List links = linkStore.get(src);
    if (links == null) links = new ArrayList();
    Integer oid = link.hashCode();
    try {
      String server = new URI(link).getHost();
      if (server != null) {
        links.add(new Values(oid, server.hashCode(), link, 0d));
        linkStore.put(src, links);
      }
    } catch (URISyntaxException ignore) {
    }
    kvState.put("linkStore", linkStore);
  }
}
