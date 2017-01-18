package com.thoughtworks.sparkyhbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;

/* How to run me
java -cp sparkyhbase-1.0-SNAPSHOT.jar:`hbase classpath` com.thoughtworks.sparkyhbase.HelloWorldOfHBase
*/
public class HelloWorldOfHBase {
    public static void main(String[] args) {
        System.out.println("hello world");
        Configuration config = HBaseConfiguration.create();
        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection(config);
            Table table = connection.getTable(TableName.valueOf("t1"));

            Get get = new Get(Bytes.toBytes("r1"));

            Result result = table.get(get);
            for (Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> navigableMapEntry : result.getMap().entrySet()) {
                System.out.println(Bytes.toString(navigableMapEntry.getKey()));
                for (Map.Entry<byte[], NavigableMap<Long, byte[]>> mapEntry : navigableMapEntry.getValue().entrySet()) {
                    System.out.println(Bytes.toString(mapEntry.getKey()));
                }
            }
            System.out.println(result);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}