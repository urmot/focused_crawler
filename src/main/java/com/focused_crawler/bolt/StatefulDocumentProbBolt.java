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
import org.apache.commons.math3.util.CombinatoricsUtils;
import java.sql.*;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;

public class StatefulDocumentProbBolt extends BaseStatefulBolt<KeyValueState<String, Map>> {
  private static final Logger LOG = LoggerFactory.getLogger(DocumentProbBolt.class);
  private OutputCollector collector;
  private Integer targetID;
  private Map<Integer, Double> probTable = new HashMap<Integer, Double>();
  /* metadata formatted by  { oid, sid, size, url }*/
  Map<Integer, Integer> counter;
  Map<Integer, Double> combStore;
  Map<Integer, List> metaStore;
  Map<Integer, Map<Integer, Double>> probStore;
  KeyValueState<String, Map> kvState;

  public StatefulDocumentProbBolt(Integer targetID) {
    this.targetID = targetID;
  }

  @Override
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    this.collector = collector;
    try (Connection con = DriverManager.getConnection("jdbc:mysql://dbserver/focused_crawler", "fc", "focused_crawler")) {
      Statement stm = con.createStatement();
      String sql = "select cid, prob from category where pid =";
      sql += "(select pid from category where cid = " +targetID+")";
      ResultSet rs = stm.executeQuery(sql);
      while(rs.next())
        probTable.put(rs.getInt("cid"), rs.getDouble("prob"));
      rs.close();
      stm.close();
      con.close();
    } catch (SQLException exp) {
      throw new RuntimeException(exp);
    }
  }

  @Override
  public void initState(KeyValueState<String, Map> state) {
    kvState = state;
    counter = kvState.get("counter", new HashMap<Integer, Integer>());
    combStore = kvState.get("combStore", new HashMap<Integer, Double>());
    probStore = kvState.get("probStore", new HashMap<Integer, Map<Integer, Double>>());
    metaStore = kvState.get("metaStore", new HashMap<Integer, List>());
  }

  @Override
  public void execute(Tuple tuple) {
    switch (tuple.getSourceStreamId()) {
    case "docSizeStream":
      storeMetadata(tuple);
      break;
    case "probStream":
      documentProb(tuple);
      break;
    }
    collector.ack(tuple);
  }

  public void storeMetadata(Tuple tuple) {
    Integer oid = tuple.getInteger(0);
    Integer size = tuple.getInteger(2);
    Map<Integer, Double> row = probStore.getOrDefault(oid, new HashMap<Integer, Double>());
    probTable.forEach((cid, classProb) -> {
      Double prob = row.getOrDefault(cid, 0d);
      prob += classProb;
      row.put(cid, prob);
    });
    probStore.put(oid, row);
    List meta = tuple.getValues();
    meta.set(2, size * probTable.size());
    metaStore.put(oid, meta);
    Double prob = combStore.getOrDefault(oid, 0d);
    prob += CombinatoricsUtils.factorialLog(size); // n(d)!
    combStore.put(oid, prob);
    kvState.put("probStore", probStore);
    kvState.put("metaStore", metaStore);
    kvState.put("combStore", combStore);
  }

  public void documentProb(Tuple tuple) {
    Integer oid = tuple.getInteger(0);
    Integer cid = tuple.getInteger(1);
    List meta = metaStore.getOrDefault(oid, Arrays.asList(oid, 0, 100000, "url"));
    Integer size = (Integer) meta.get(2);
    Integer count = counter.getOrDefault(oid, 0);
    Map<Integer, Double> probTable = probStore.getOrDefault(oid, new HashMap<Integer, Double>());
    Double prob = probTable.getOrDefault(cid, 0d);

    prob += tuple.getDouble(2);
    count += tuple.getInteger(3);
    if (tuple.getInteger(3) > 1) {
      Double comb = combStore.getOrDefault(oid, 0d);
      comb -= CombinatoricsUtils.factorialLog(tuple.getInteger(3));
      combStore.put(oid, comb);
      kvState.put("combStore", combStore);
    }
    probTable.put(cid, prob);
    if (size.equals(count)) {
      Integer classifyID = probTable.entrySet().stream()
        .max(Comparator.comparing(e -> e.getValue())).get().getKey();
      prob = probTable.get(targetID);
      Double comb = combStore.remove(oid);
      collector.emit("updateStream", tuple, new Values(oid, meta.get(1), meta.get(3), comb +  prob, classifyID));
      counter.remove(oid);
      probStore.remove(oid);
      metaStore.remove(oid);
      kvState.put("metaStore", metaStore);
    } else {
      counter.put(oid, count);
      probStore.put(oid, probTable);
    }
    kvState.put("counter", counter);
    kvState.put("probStore", probStore);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declare) {
    declare.declareStream("updateStream", new Fields("oid", "sid", "url", "relevance", "cid"));
  }
}
