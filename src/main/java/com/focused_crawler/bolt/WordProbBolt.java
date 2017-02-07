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
import org.apache.commons.math3.util.CombinatoricsUtils;
import java.sql.*;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;


public class WordProbBolt extends BaseBasicBolt {
  private Integer targetID;
  String category;
  Map<Integer, Map<String, Double>> probTable = new HashMap<Integer, Map<String, Double>>();

  public WordProbBolt(Integer targetID) {
    this.targetID = targetID;
  }

  @Override
  public void prepare(Map stormConf, TopologyContext context) {
    try (Connection con = DriverManager.getConnection("jdbc:mysql://dbserver/focused_crawler", "fc", "focused_crawler")) {
      Statement stm = con.createStatement();
      String sql = "select cid from category where pid =";
      sql += "(select pid from category where cid = " +targetID+")";
      ResultSet rs = stm.executeQuery(sql);
      List<Integer> cids = new ArrayList<Integer>();
      while(rs.next()) cids.add(rs.getInt("cid"));
      rs.close();
      cids.forEach(cid -> {
        Map<String, Double> probMap = new HashMap<String, Double>();
        String query = "select word, prob from prob where cid = " + cid;
        try (ResultSet r = stm.executeQuery(query)) {
          while(r.next()) probMap.put(r.getString("word"), r.getDouble("prob"));
          r.close();
        } catch(SQLException exp) {
          throw new RuntimeException(exp);
        }
        probTable.put(cid, probMap);
      });
      stm.close();
      con.close();
    } catch (SQLException exp) {
      throw new RuntimeException(exp);
    }
  }

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    Integer oid = tuple.getInteger(0);
    String word = tuple.getString(1);
    Integer count = tuple.getInteger(2);

    probTable.forEach((cid, probMap) -> {
      Double prob = probMap.get(word);
      if (prob == null) prob = probMap.get("_Smoothing");
      prob *= count;
      collector.emit("probStream", new Values(oid, cid, prob, count));
    });
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declare) {
    declare.declareStream("probStream", new Fields("oid", "cid", "prob", "count"));
  }
}
