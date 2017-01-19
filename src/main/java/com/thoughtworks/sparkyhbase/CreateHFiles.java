package com.thoughtworks.sparkyhbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.IOException;

/**
 * http://www.opencore.com/blog/2016/10/efficient-bulk-load-of-hbase-using-spark/
 */
/*
 spark-submit --jars `echo /usr/lib/hbase/*.jar | sed 's/ /,/g'` \
     --class com.thoughtworks.sparkyhbase.CreateHFiles sparkyhbase-1.0-SNAPSHOT.jar \
     input_path output_path
 */
public class CreateHFiles {


    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.err.println("Need input data as 1st argument");
            System.exit(1);
        }
        if (args.length < 2) {
            System.err.println("Need output data as 2nd argument");
            System.exit(1);
        }
        String input_path = args[0];
        String output_path = args[1];
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .getOrCreate();


        Dataset<Row> inputData = spark.read().json(input_path)
                .orderBy("id").cache();


        String[] columns = {"a"};
        JavaRDD<Put> puts = inputData.javaRDD().map(
                (Row row) -> Functions.makePut(row, "id", columns, "f1")
        );
        JavaRDD<Cell> cells = puts.flatMap(
                (Put put) -> Functions.makeCells(put)
        );
        JavaPairRDD<byte[], Cell> pairs = cells.mapToPair(
                (Cell cell) -> new Tuple2<>(cell.getRow(), cell)
        ).sortByKey(new BytesComparator());

        JavaPairRDD<ImmutableBytesWritable, Cell> writables = pairs.mapToPair(
                (Tuple2<byte[], Cell> tuple) ->
                        new Tuple2<>(new ImmutableBytesWritable(tuple._1()), tuple._2())
        );

        // write HFiles onto HDFS
        writables.saveAsNewAPIHadoopFile(
                output_path,
                ImmutableBytesWritable.class,
                Cell.class,
                HFileOutputFormat2.class);
    }
}
