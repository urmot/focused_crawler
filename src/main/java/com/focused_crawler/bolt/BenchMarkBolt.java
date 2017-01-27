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
  Integer count;
  Double relevanceAvg;

  @Override
  public void prepare(Map stormConf, TopologyContext context) {
    count = 0;
    relevanceAvg = 0d;
    start = System.currentTimeMillis();
    writeFile("count,time,throughput,relevanceAvg");
  }

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    count++;
    relevanceAvg += tuple.getDouble(3);
    if (count % 1000 == 0) {
      Long end = System.currentTimeMillis();
      Long time = end - start;
      Double thoughput = 1000d / time;
      relevanceAvg /= 1000;
      String result = count.toString() + "," + time.toString() +
                      "," + thoughput.toString() + "," + relevanceAvg.toString();
      writeFile(result);
      System.out.println(result);
      start = end;
      relevanceAvg = 0d;
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
