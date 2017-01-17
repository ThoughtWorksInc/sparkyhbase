package load;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;

/**
 * http://www.opencore.com/blog/2016/10/efficient-bulk-load-of-hbase-using-spark/
 */
/*
 spark-submit --jars `echo /usr/lib/hbase/*.jar | sed 's/ /,/g'` \
     --class load.CreateHFiles ca-1.0-SNAPSHOT.jar
 */
public class CreateHFiles {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Simple Application");
        JavaSparkContext sc = new JavaSparkContext(conf);
        String logFile = "foo"; // Should be some file on your system
        JavaRDD<String> logData = sc.textFile(logFile).cache();
        List<String> stuff = logData.collect();

        JavaRDD<KeyValue> keyvalues = logData.map((String x) -> new KeyValue(
                        Bytes.toBytes("r5"),
                        Bytes.toBytes("f1"),  // column family
                        Bytes.toBytes("c3"), // column qualifier (i.e. cell name)
                        Bytes.toBytes(x)
                )
        );

        JavaPairRDD<ImmutableBytesWritable, KeyValue> writables = keyvalues.mapToPair(
                (KeyValue kv) -> new Tuple2<>(
                    new ImmutableBytesWritable(kv.getRowArray()), kv)
        );

        // write HFiles onto HDFS
        writables.saveAsNewAPIHadoopFile(
                "/user/hadoop/tmphfiles",
                ImmutableBytesWritable.class,
                KeyValue.class,
                HFileOutputFormat2.class);
    }
}
