package com.zjrb.bigdata.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class HbaseConPool {
    static Connection conn;

     public static Connection getConn() throws IOException {
         Configuration conf = HBaseConfiguration.create();
         conf.set("hbase.zookeeper.property.clientPort", "2181");
         conf.set("hbase.zookeeper.quorum", "hue.zbjt.com:2181,dwdb01.zbjt.com:2181,coreserver05.zbjt.com:2181");
         try {
             /*
             * 使用ConnectionFactory，在获取getTable的时候会调用默认的连接池，默认配置最大连接数256
             * */
             conn = ConnectionFactory.createConnection(conf);

         } catch (Exception e) {
             e.printStackTrace();
             if (conn != null) {
                 conn.close();
             }
         }
         return conn;
     }
}
