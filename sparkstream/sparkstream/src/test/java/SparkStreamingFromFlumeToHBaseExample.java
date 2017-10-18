import com.clearspring.analytics.util.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Created by 11325 on 2017/9/20.
 */
public class SparkStreamingFromFlumeToHBaseExample {

    private static final Pattern SPACE = Pattern.compile(",");

    public static void main(String[] args) {


        // String master = args[0];
        // String host = args[1];
        // int port = Integer.parseInt(args[2]);
        String tableName = "test1";// args[3];
        String columnFamily = "f";// args[4];
        // int windowInSeconds = 3;// Integer.parseInt(args[5]);
        // int slideInSeconds = 1;// Integer.parseInt(args[5]);

        String zkQuorum = "coreserver05.zbjt.com:2181 ,dwdb01.zbjt.com:2181 ,hue.zbjt.com:2181";
        String group = "test2";
        String topicss = "test2";
        String numThread = "2";

        Duration batchInterval = new Duration(5000);
        // Duration windowInterval = new Duration(windowInSeconds * 1000);
        // Duration slideInterval = new Duration(slideInSeconds * 1000);

        SparkConf sparkConf = new SparkConf().setAppName("JavaKafkaWordCount");
        JavaStreamingContext jssc =
                new JavaStreamingContext(sparkConf, new Duration(2000));

        final Broadcast<String> broadcastTableName =
                jssc.sparkContext().broadcast(tableName);
        final Broadcast<String> broadcastColumnFamily =
                jssc.sparkContext().broadcast(columnFamily);

        // JavaDStream<SparkFlumeEvent> flumeStream = sc.flumeStream(host, port);

        int numThreads = Integer.parseInt(numThread);
        Map<String, Integer> topicMap = new HashMap<String, Integer>();
        String[] topics = topicss.split(",");
        for (String topic : topics) {
            topicMap.put(topic, numThreads);
        }

        JavaPairReceiverInputDStream<String, String> messages =
                KafkaUtils.createStream(jssc, zkQuorum, group, topicMap);

        JavaDStream<String> lines =
                messages.map(new Function<Tuple2<String, String>, String>() {

                    public String call(Tuple2<String, String> tuple2) {
                        return tuple2._2();
                    }
                });

        JavaDStream<String> words =
                lines.flatMap(new FlatMapFunction<String, String>() {

                    public Iterable<String> call(String x) {
                        return Lists.newArrayList(Arrays.asList(SPACE.split(x)));
                    }
                });

        JavaPairDStream<String, Integer> lastCounts =
                messages.map(new Function<Tuple2<String, String>, String>() {

                    public String call(Tuple2<String, String> tuple2) {
                        return tuple2._2();
                    }
                }).flatMap(new FlatMapFunction<String, String>() {

                    public Iterable<String> call(String x) {
                        return Lists.newArrayList(Arrays.asList(SPACE.split(x)));
                    }
                }).mapToPair(new PairFunction<String, String, Integer>() {

                    public Tuple2<String, Integer> call(String s) {
                        return new Tuple2<String, Integer>(s, 1);
                    }
                }).reduceByKey(new Function2<Integer, Integer, Integer>() {


                    public Integer call(Integer x, Integer y) throws Exception {
                        // TODO Auto-generated method stub
                        return x.intValue() + y.intValue();
                    }
                });

        lastCounts
                .foreach(new Function2<JavaPairRDD<String, Integer>, Time, Void>() {


                    public Void call(JavaPairRDD<String, Integer> values, Time time)
                            throws Exception {

                        values.foreach(new VoidFunction<Tuple2<String, Integer>>() {


                            public void call(Tuple2<String, Integer> tuple) throws Exception {
                                HBaseCounterIncrementor incrementor =
                                        HBaseCounterIncrementor.getInstance(
                                                broadcastTableName.value(),
                                                broadcastColumnFamily.value());
                                incrementor.incerment("Counter", tuple._1(), tuple._2());
                                System.out.println("Counter:" + tuple._1() + "," + tuple._2());

                            }
                        });

                        return null;
                    }
                });

        jssc.start();

    }
}
