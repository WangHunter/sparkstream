

import java.io.IOException;
import java.util.*;

import kafka.serializer.StringDecoder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;


/**
 * Created by 11325 on 2017/9/20.
 */
public class SparkOnStreamingToHbase {
    public static void main(String[] args) throws Exception {
//        if (args.length < 3) {
//            printUsage();    }
        //String checkPointDir = args[0];
        String topics = "test2";
        //final String brokers = args[2];
        Duration batchDuration = Durations.seconds(5);
        SparkConf sparkConf = new SparkConf().setAppName("SparkOnStreamingToHbase");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, batchDuration);
        // 设置Spark Streaming的CheckPoint目录

        final String columnFamily = "f";
        HashMap<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list","coreserver05.zbjt.com:9092");
        String[] topicArr = topics.split(",");
        Set<String> topicSet = new HashSet<String>(Arrays.asList(topicArr));
        // 通过brokers和topics直接创建kafka stream    // 接收Kafka中数据，生成相应DStream
        JavaDStream<String> lines = KafkaUtils.createDirectStream(
                jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topicSet).map(
                new Function<Tuple2<String, String>, String>() {
                    public String call(Tuple2<String, String> tuple2) {
                        // map(_._1)是消息的key, map(_._2)是消息的value
                        return tuple2._2();
                    }
                }    );
        lines.print();
        lines.foreachRDD(
                new Function<JavaRDD<String>, Void>() {
                    public Void call(JavaRDD<String> rdd) throws Exception {
                        rdd.collect();
                        rdd.foreachPartition(
                                new VoidFunction<Iterator<String>>() {
                                    public void call(Iterator<String> iterator) throws Exception {

                                        hBaseWriter(iterator, columnFamily);
                                    }
                                }          );
                        return null;
                    }
                }    );

        jssc.start();
        jssc.awaitTermination();  }
    /**   * 在executor端写入数据   * @param iterator  消息   * @param columnFamily   */
    private static void hBaseWriter(Iterator<String> iterator, String columnFamily) {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.zookeeper.quorum", "hue.zbjt.com:2181,dwdb01.zbjt.com:2181,coreserver05.zbjt.com:2181");
        Connection connection = null;
        Table table = null;
        try {
            connection = ConnectionFactory.createConnection(conf);
            table = connection.getTable(TableName.valueOf("test1"));
            while (iterator.hasNext()) {
                Put put = new Put("test".getBytes());
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("cid"), Bytes.toBytes(String.valueOf(iterator.next())));
                table.put(put);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (connection != null) {
                try {
                    // 关闭Hbase连接.
                    connection.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static void printUsage() {
        System.out.println("Usage: {checkPointDir} {topic} {brokerList}");
        System.exit(1);  } }
