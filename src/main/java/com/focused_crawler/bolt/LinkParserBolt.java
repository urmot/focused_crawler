 package com.focused_crawler.bolt;

 import org.apache.storm.state.KeyValueState;
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
import java.util.Set;
import java.util.HashSet;

public class LinkParserBolt extends BaseStatefulBolt<KeyValueState<String, Map>> {
  private OutputCollector collector;
  private Integer targetID;
  private Set<String> blackList;
  Map<Integer, List> linkStore;
  KeyValueState<String, Map> kvState;

  public LinkParserBolt(Integer targetID) {
    this.targetID = targetID;
  }

  @Override
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    this.collector = collector;
    blackList = new HashSet<String>();
    blackList.add("www.cafepress.com");
    blackList.add("dx.doi.org");
    blackList.add("link.aps.org");
    blackList.add("taxondiversity.fieldofscience.com");
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
      Integer cid = tuple.getInteger(4);
      System.out.println("cid: "+cid+", targetID: "+targetID+", match: "+cid.equals(targetID));
      if (cid.equals(targetID)) emitLinks(tuple);
      break;
    }
    collector.ack(tuple);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declareStream("linkStream", new Fields("key", "message"));
  }

  public void storeLink(Tuple tuple) {
    Integer src = tuple.getInteger(0);
    String url = tuple.getString(1);
    Integer oid = url.hashCode();
    try {
      String server = new URI(url).getHost();
      if (server == null || blackList.contains(server)) {
      } else {
        Integer sid = server.hashCode();
        String[] link = {oid.toString(), sid.toString(), url};
        List<String> links = linkStore.getOrDefault(src, new ArrayList<String>());
        links.add(String.join("|", link));
        linkStore.put(src, links);
        kvState.put("linkStore", linkStore);
      }
    } catch (URISyntaxException ignore) {
    }
  }

  public void emitLinks(Tuple tuple) {
    Integer src = tuple.getInteger(0);
    Double prob = tuple.getDouble(3);
    List links = linkStore.remove(src);
    if (links != null) {
      for (Object link : links) {
        String l = (String) link;
        collector.emit("linkStream", tuple, new Values("key", l+"|"+prob.toString()));
      }
      kvState.put("linkStore", linkStore);
    }
  }
}
