
# Create HFiles

Inspired by this blog post:
http://www.opencore.com/blog/2016/10/efficient-bulk-load-of-hbase-using-spark/

```
spark-submit --jars `echo /usr/lib/hbase/*.jar | sed 's/ /,/g'` \
     --class com.thoughtworks.sparkyhbase.CreateHFiles sparkyhbase-1.0-SNAPSHOT.jar
```

# Load HFiles into HBase

```
hdfs dfs -chmod +w /user/hadoop/tmp-hfiles/
hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles \
    -Dcreate.table=no /user/hadoop/tmphfiles/ tablename
```