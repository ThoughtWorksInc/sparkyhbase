package load;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

/**
 * http://www.opencore.com/blog/2016/10/efficient-bulk-load-of-hbase-using-spark/
 */
/*
 spark-submit --jars `echo /usr/lib/hbase/*.jar | sed 's/ /,/g'` \
     --class load.CreateHFiles sparkyhbase-1.0-SNAPSHOT.jar
 */
public class CreateHFiles {
    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Need input data as argument");
            System.exit(1);
        }
        String input_path = args[0];
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .getOrCreate();


        Dataset<Row> inputData = spark.read().json(input_path)
                .orderBy("id").cache();

        JavaRDD<KeyValue> keyvalues = inputData.javaRDD().map((Row row) -> new KeyValue(
                        Bytes.toBytes(row.getLong(row.fieldIndex("id"))),
                        Bytes.toBytes("f1"),  // column family
                        Bytes.toBytes("a"),
                        Bytes.toBytes(row.getDouble(row.fieldIndex("a")))
                )
        );

        JavaPairRDD<ImmutableBytesWritable, KeyValue> writables = keyvalues.mapToPair(
                (KeyValue kv) -> new Tuple2<>(
                        new ImmutableBytesWritable(kv.getRow()), kv)
        );

        // write HFiles onto HDFS
        writables.saveAsNewAPIHadoopFile(
                "/user/hadoop/tmphfiles",
                ImmutableBytesWritable.class,
                KeyValue.class,
                HFileOutputFormat2.class);
    }
}
