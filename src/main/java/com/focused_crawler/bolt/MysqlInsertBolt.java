package com.focused_crawler.bolt;

import org.apache.storm.jdbc.common.Column;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.HikariCPConnectionProvider;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;
import org.apache.storm.jdbc.bolt.JdbcInsertBolt;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

public class MysqlInsertBolt {
  Map hikariConfig = new HashMap();
  List<Column> schema = new ArrayList<Column>();

  public void MysqlInsertBolt(Map conf) {
    String host = (String) conf.get("host");
    String db = (String) conf.get("database");
    hikariConfig.put("dataSourceClassName","com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
    hikariConfig.put("dataSource.url", "jdbc:mysql://" + host + db);
    hikariConfig.put("dataSource.user", conf.get("user"));
    hikariConfig.put("dataSource.password", conf.get("password"));
  }

  public void setSchema(Column column) {
    schema.add(column);
  }

  public JdbcInsertBolt getBolt(String query) {
    ConnectionProvider connectionProvider = new HikariCPConnectionProvider(hikariConfig);
    JdbcMapper crawlJdbcMapper = new SimpleJdbcMapper(schema);
    JdbcInsertBolt bolt = new JdbcInsertBolt(connectionProvider, crawlJdbcMapper)
      .withInsertQuery(query)
      // .withInsertQuery("insert into crawl (oid, sid, url, relevance) values (?, ?, ?, ?) on duplicate key update numtries = numtries + 1")
      .withQueryTimeoutSecs(30);
    return bolt;
  }
}
