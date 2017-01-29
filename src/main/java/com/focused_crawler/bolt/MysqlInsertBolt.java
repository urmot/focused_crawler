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

  public JdbcInsertBolt getBolt() {
    String query;
    Map hikariConfig = new HashMap();
    List<Column> schema = new ArrayList<Column>();

    hikariConfig.put("dataSourceClassName","com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
    hikariConfig.put("dataSource.url", "jdbc:mysql://dbserver/focused_crawler");
    hikariConfig.put("dataSource.user", "fc");
    hikariConfig.put("dataSource.password", "focused_crawler");
    schema.add(new Column("oid", java.sql.Types.INTEGER));
    schema.add(new Column("sid", java.sql.Types.INTEGER));
    schema.add(new Column("url", java.sql.Types.VARCHAR));
    schema.add(new Column("relevance", java.sql.Types.DOUBLE));
    query = "insert into crawl (oid, sid, url, relevance) values (?, ?, ?, ?)";
    query += " on duplicate key update numtries = numtries + 1";

    ConnectionProvider connectionProvider = new HikariCPConnectionProvider(hikariConfig);
    JdbcMapper crawlJdbcMapper = new SimpleJdbcMapper(schema);
    JdbcInsertBolt bolt = new JdbcInsertBolt(connectionProvider, crawlJdbcMapper)
      .withInsertQuery(query)
      .withQueryTimeoutSecs(30);
    return bolt;
  }
}
