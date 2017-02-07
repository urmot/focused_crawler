package com.focused_crawler.bolt;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class BenchMarkBolt extends BaseBasicBolt {
  Long start;
  Long end;
  Integer count;
  Integer size;
  Double relevance;
  Double relevanceAvg;

  @Override
  public void prepare(Map stormConf, TopologyContext context) {
    count = 0;
    relevance = 0d;
    relevanceAvg = 0d;
    size = 100;
    start = System.currentTimeMillis();
    end = System.currentTimeMillis();
    writeFile("count,time,throughput,throughputAvg,relevance,relevanceAvg");
  }

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    count++;
    relevance += tuple.getDouble(3);
    if (count % size == 0) {
      Long now = System.currentTimeMillis();
      Long time = now - end;
      Double relAvg = relevance / (double) count;
      Double rel = (relevance - relevanceAvg) / (double) size;
      Double thoughputAvg = (now - start) / (double) count;
      Double thoughput = time / (double) size;
      String result = count.toString()+","+time.toString()+","+thoughput.toString()+
        ","+ thoughputAvg.toString()+","+rel.toString()+","+relAvg.toString();
      writeFile(result);
      System.out.println(result);
      end = now;
      relevanceAvg = relevance;
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
  }

  public void writeFile(String result) {
    try {
      File file = new File("/tmp/storm/result/scalable.csv");
      FileWriter filewriter = new FileWriter(file, true);
      filewriter.write(result + "\n");
      filewriter.close();
    } catch (IOException exp) {
      throw new RuntimeException(exp);
    }
  }
}
