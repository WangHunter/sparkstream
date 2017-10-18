import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;
public class sparkKafkaZk {

    static final String ZK_QUORUM = "hue.zbjt.com:2181,dwdb01.zbjt.com:2181,coreserver05.zbjt.com:2181/";
    static final String GROUP = "cloudera_mirrormaker";
    static final String TOPICSS = "kafkaTosparktest";
    static final String NUM_THREAD = "1";

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("wordcount").setMaster("local[2]");
        // Create the context with 2 seconds batch size
        //每5秒读取一次kafka
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(5));
        int numThreads = Integer.parseInt(NUM_THREAD);
        Map<String, Integer> topicMap = new HashMap<String, Integer>();
        String[] topics = TOPICSS.split("\t");
        for (String topic: topics) {
            topicMap.put(topic, numThreads);
        }
        JavaPairReceiverInputDStream<String, String> messages =
                KafkaUtils.createStream(jssc, ZK_QUORUM, GROUP, topicMap);
        JavaPairDStream<String, Long> jds=messages.mapToPair(new PairFunction<Tuple2<String,String>, String, Long>() {

            public Tuple2<String, Long> call(Tuple2<String,String> t) throws Exception {
                String[] logs = t._2.split("\t");
                String time =logs[0];
                return new Tuple2<String,Long>(time, 1L);
            }
        }).reduceByKey(new Function2<Long, Long, Long>() { //对相同的Key，进行Value的累计（包括Local和Reducer级别同时Reduce）
            public Long call(Long v1, Long v2) throws Exception {
                return v1 + v2;
            }
        });
        jds.foreachRDD(new Function2<JavaPairRDD<String, Long>, Time, Void>() {
            public Void call(JavaPairRDD<String, Long> values, Time time)
                    throws Exception {
                System.out.println("开始遍历" + values + time);
                //foreachPartition这个方法好像和kafka的topic的分区个数有关系，如果你topic有两个分区，则这个方法会执行两次
                values.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Long>>>() {

                    public void call(Iterator<Tuple2<String, Long>> tuples) throws Exception {
                        Tuple2<String, Long> tuple = null;
                        while(tuples.hasNext()){
                            tuple = tuples.next();
                            System.out.println("Counter:" + tuple._1() + "," + tuple._2());
                            System.out.println("---------------------Counter:" + tuple._1() + "," + tuple._2());
                            insert("zjrb_gao", "Counter", "c1", tuple._1(), tuple._2.toString()) ;
                        }
                    }
                });
                return null;
            }
        });

        jds.print();
        jssc.start();
        jssc.awaitTermination();
        jssc.stop(true,true);
        jssc.close();

    }


    public static Configuration getConfiguration() {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.zookeeper.quorum", "hue.zbjt.com:2181,dwdb01.zbjt.com:2181,coreserver05.zbjt.com:2181");
        return conf;
    }
    public static void insert(String tableName, String rowKey, String family,
                              String quailifer, String value) {


        try {

            Connection conn = ConnectionFactory.createConnection(getConfiguration());
            HTable table = (HTable) conn.getTable(TableName.valueOf(tableName));
            Put put = new Put(Bytes.toBytes(rowKey));
            // 在put对象中设置列族、列、值
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes(quailifer), Bytes.toBytes(value));
            table.put(put);
            table.close();
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}