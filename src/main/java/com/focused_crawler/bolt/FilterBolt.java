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
import java.sql.*;

import java.util.Map;
import java.util.Set;
import java.util.HashSet;

public class FilterBolt extends BaseStatefulBolt<KeyValueState<String, Set<Integer>>> {
  private OutputCollector collector;
  KeyValueState<String, Set<Integer>> kvState;
  Set<Integer> crawled;
  String[] blackList = new String[4];

  @Override
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    this.collector = collector;
    blackList[0] = "https?://link.aps.org.*";
    blackList[1] = "https?://www.cafepress.com.*";
    blackList[2] = "https?://dx.doi.org.*";
    blackList[3] = "https?://taxondiversity.fieldofscience.com.*";
    crawled = new HashSet<Integer>();
    try (Connection con = DriverManager.getConnection("jdbc:mysql://dbserver/focused_crawler", "fc", "focused_crawler")) {
      Statement stm = con.createStatement();
      String sql = "select oid from crawl";
      ResultSet rs = stm.executeQuery(sql);
      while(rs.next()) crawled.add(rs.getInt("oid"));
      rs.close();
      stm.close();
      con.close();
    } catch (SQLException exp) {
      throw new RuntimeException(exp);
    }
  }

  @Override
  public void initState(KeyValueState<String, Set<Integer>> state) {
    kvState = state;
    kvState.put("crawled", crawled);
    crawled = kvState.get("crawled", new HashSet<Integer>());
  }

  @Override
  public void execute(Tuple tuple) {
    Integer oid = tuple.getInteger(0);
    if (crawled.contains(oid) || isBlackList(tuple.getString(2))) {
      // skip crawled or black list tuple
    } else {
      collector.emit("linkStream", tuple, tuple.getValues());
      crawled.add(oid);
      kvState.put("crawled", crawled);
    }
    collector.ack(tuple);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declareStream("linkStream", new Fields("oid", "sid", "url", "relevance"));
  }

  public Boolean isBlackList(String url) {
    Boolean result = false;
    for (String regex : blackList) result |= url.matches(regex);
    return result;
  }
}
