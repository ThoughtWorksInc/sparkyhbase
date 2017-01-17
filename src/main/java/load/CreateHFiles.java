package load;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;
import java.util.function.Function;

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

        JavaRDD rdd = logData.map((String x) -> new KeyValue(
                Bytes.toBytes("r5"),
                Bytes.toBytes("f1"),  // column family
                Bytes.toBytes("c3"), // column qualifier (i.e. cell name)
                Bytes.toBytes(x)
            )
        );

        // TODO

        for (String x : stuff) {
            System.out.println(x);
        }
    }


}
