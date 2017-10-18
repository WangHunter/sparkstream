import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Created by 11325 on 2017/9/22.
 */
public class sparkWindowTest {

    public static void main(String[] args){
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("sparkWindowTest").setMaster("local[2]");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
        //jsc.checkpoint(".");
        JavaReceiverInputDStream lines = jsc.socketTextStream("localhost",9999);
        JavaPairDStream words = lines.flatMap(new FlatMapFunction<String, String>() {
            public Iterable<String> call(String line ) throws Exception {
                return Arrays.asList( line.split( ","));
            }
        }).mapToPair(new PairFunction<String, String,Long>() {
                            public Tuple2<String, Long> call(String line ) throws Exception {
                                return new Tuple2<String, Long>(line, 1L);
                            }
                        }
        ).reduceByKey(new Function2<Long, Long,Long>() {
                                       public Long call(Long value1,Long value2 ) throws Exception {
                                           return value1+value2;
                                       }
                                   }
        ).window(Durations.seconds(10),Durations.seconds(10));
        words.foreach(new Function2<JavaPairRDD<String, Long>, Time, Void>() {
            public Void call(JavaPairRDD<String, Long> javaPairRDD, Time time)
                    throws Exception {
                javaPairRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Long>>>() {
                    public void call(Iterator<Tuple2<String, Long>> wordcounts) throws Exception {
                        //建立hbase连接，可以在HbaseConPool中配置连接池，每一次遍历都会新建一个conn对象，但是使用一个hbase连接
                        Connection conn = HbaseConPool.getConn();
                        Table table = conn.getTable(TableName.valueOf("xu_sparkToHbase_test"));
                        Tuple2<String, Long> wordcount = null;
                        //遍历每一条并插入hbase
                        while (wordcounts.hasNext()) {
                            wordcount = wordcounts.next();
                            System.out.println("---------------------xu_sparkToHbase_test:" + wordcount._1.toString() + "," + wordcount._2.toString());
                            Put put = new Put(wordcount._1.toString().getBytes());
                            put.addColumn("f".getBytes(), wordcount._1.toString().getBytes(), wordcount._2.toString().getBytes());
                            //注意：这里是利用了在hbase中对同一rowkey同一列再查入数据会覆盖前一次值的特征，所以hbase中linecount表的版本号必须是1，建表的时候如果你不修改版本号的话默认是1
                            table.put(put);
                        }
                        //别忘了关闭conn和table对象
                        if (conn != null) {
                            conn.close();
                        }
                        if (table != null) {
                            table.close();
                        }
                    }
                });

                return null;
            }
        });
        words .print();

        jsc.start();
        jsc.awaitTermination();
    }
}
