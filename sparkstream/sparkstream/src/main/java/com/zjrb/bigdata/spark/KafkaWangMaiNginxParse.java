package com.zjrb.bigdata.spark;
/**
 * Created by 11325 on 2017/9/14.
 */

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

/*
 *消费者消费SparkStreamingDataManuallyProducerForKafka类中逻辑级别产生的数据，这里是计算pv，uv，注册人数，跳出率的方式
 */
public class KafkaWangMaiNginxParse {

    static SparkConf conf = new SparkConf().setAppName("KafkaWangMaiNginxParse").setMaster("local[2]");
    static JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
    private static String beginTime = null;
    static final String ZK_QUORUM = "hue.zbjt.com:2181,dwdb01.zbjt.com:2181,coreserver05.zbjt.com:2181/";
    static final String GROUP = "cloudera_mirrormaker";
    static final String TOPICSS = "kafkaTosparktest";
    static final String NUM_THREAD = "1";

    public static void main(String[] args) {
        /**
         * 第一步：配置SparkConf：
         * 1，local模式至少2条线程：因为Spark Streaming应用程序在运行的时候，至少有一条
         * 线程用于不断的循环接收数据，并且至少有一条线程用于处理接受的数据（否则的话无法
         * 有线程用于处理数据，随着时间的推移，内存和磁盘都会不堪重负）；
         * 2，yarn-client模式对于集群而言，每个Executor一般肯定不止一个Thread，那对于处理Spark Streaming的
         * 应用程序而言，每个Executor一般分配多少Core比较合适？根据我们过去的经验，5个左右的
         * Core是最佳的（一个段子分配为奇数个Core表现最佳，例如3个、5个、7个Core等）；
         */
        /**
         * 第二步：创建SparkStreamingContext：
         * 1，这个是SparkStreaming应用程序所有功能的起始点和程序调度的核心
         * SparkStreamingContext的构建可以基于SparkConf参数，也可基于持久化的SparkStreamingContext的内容
         * 来恢复过来（典型的场景是Driver崩溃后重新启动，由于Spark Streaming具有连续7*24小时不间断运行的特征，
         * 所有需要在Driver重新启动后继续上次的状态，此时的状态恢复需要基于曾经的Checkpoint）；
         * 2，在一个Spark Streaming应用程序中可以创建若干个SparkStreamingContext对象，使用下一个SparkStreamingContext
         * 之前需要把前面正在运行的SparkStreamingContext对象关闭掉，由此，我们获得一个重大的启发SparkStreaming框架也只是
         * Spark Core上的一个应用程序而已，只不过Spark Streaming框架箱运行的话需要Spark工程师写业务逻辑处理代码；
         */
        /**
         * 第三步：创建Spark Streaming输入数据来源input Stream：
         * 1，数据输入来源可以基于File、HDFS、Flume、Kafka、Socket等
         * 2, 在这里我们指定数据来源于网络Socket端口，Spark Streaming连接上该端口并在运行的时候一直监听该端口
         * 		的数据（当然该端口服务首先必须存在）,并且在后续会根据业务需要不断的有数据产生(当然对于Spark Streaming
         * 		应用程序的运行而言，有无数据其处理流程都是一样的)；
         * 3,如果经常在每间隔5秒钟没有数据的话不断的启动空的Job其实是会造成调度资源的浪费，因为并没有数据需要发生计算，所以
         * 		实例的企业级生成环境的代码在具体提交Job前会判断是否有数据，如果没有的话就不再提交Job；
         */
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
        int numThreads = Integer.parseInt(NUM_THREAD);
        Map<String, Integer> topicMap = new HashMap<String, Integer>();
        String[] topics = TOPICSS.split("\t");
        for (String topic: topics) {
            topicMap.put(topic, numThreads);
        }
        JavaPairReceiverInputDStream<String, String> messages =
                KafkaUtils.createStream(jssc, ZK_QUORUM, GROUP, topicMap);
        SimpleDateFormat time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        java.util.Date date = new java.util.Date();
        System.out.println("StreamingContext started->" + time.format(new Date()));
        beginTime = time.format(date);
        //行为数据实时插入hbase
        detailDataToHbase(messages, "browse_detail", "timeIp");

        //在线PV计算
//        ipOnlinePV(lines);
//        browserUV(lines);
//      onlinePagePV(lines);
      /*
       * Spark Streaming执行引擎也就是Driver开始运行，Driver启动的时候是位于一条新的线程中的，当然其内部有消息循环体，用于
       * 接受应用程序本身或者Executor中的消息；
       */
        jsc.start();
        jsc.awaitTermination();
        jsc.close();
    }

    //将kafka的明细数据导入hbase用作之后计算PV UV
    private static void detailDataToHbase(JavaPairInputDStream<String, String> lines, String tableName, String columeFamily) {
        /*
        * kafka推送过来的数据存放在lines这个对象中，数据格式：ipAddress  dataTime request response bytesSent referer browser agentIP
        * 下面的方法是对lines对象进行过滤，过滤掉异常数据
        * */
        JavaPairDStream<String, String> logsDStream = lines.filter(new Function<Tuple2<String, String>, Boolean>() {

            public Boolean call(Tuple2<String, String> v1) throws Exception {
                String[] log = v1._2.split("\t");
                String no_Match = log[0];
                if ("".equals(no_Match)) {
                    return false;
                } else {
                    return true;
                }
            }
        });
        /*
        *将logsDStream进行mapToPair操作，将Tuple2中的value根据'\t'分隔并取第2(时间)和第8（agentIP）个字段作为输出的key，给个初始值1
        *通过reduceByKey对相同的Key，进行Value的累计（包括Local和Reducer级别同时Reduce） Function2（T，T，R） T为输入 R为输出
         */
        JavaPairDStream<String, Long> ipjds = logsDStream.mapToPair(new PairFunction<Tuple2<String, String>, String, Long>() {


            public Tuple2<String, Long> call(Tuple2<String, String> t) throws Exception {
                String[] logs = t._2.split("\t");
                String dataTime = logs[1].toString();
                String agentIP = logs[7].toString();
                SimpleDateFormat format = new SimpleDateFormat("d/MMM/yyyy:HH:mm:ss", Locale.ENGLISH);
                String time = new SimpleDateFormat("yyyy-MM-dd").format(format.parse(dataTime));
                return new Tuple2<String, Long>(time + "+" + agentIP, 1L);
            }
        }).reduceByKey(new Function2<Long, Long, Long>() { //对相同的Key，进行Value的累计（包括Local和Reducer级别同时Reduce）
            public Long call(Long v1, Long v2) throws Exception {
                return v1 + v2;
            }
        });
        /*
        * 最重要的就是这里 先对jds进行foreach， 会在集群里的不同机器上创建spark工作线程，
      * 如果Connection是全局变量，就会因为spark工作线程之间连接不共享导致的Connection对象没有被初始化的执行错误
        * 遍历每个分区foreachPartition，这个地方涉及到广播变量的问题，不同分区不共享广播变量，所以也需要在分区内建立一个Connection对象
      * 但是这样显然效率性能都很差，所以可以维护一个全局的静态的连接池对象HbaseConPool.getConn()， 这样就可以最好的复用connection
        * 针对为什么foreach内部还要foreachPartition或者嵌套foreach：foreachRDD 是一个输出操作符，
      * 它返回的不是RDD里的一行数据， 而是输出DStream后面的RDD,在一个时间间隔里， 只返回一个RDD的“微批次”，
      * 为了访问这个“微批次”RDD里的数据， 我们还需要在RDD数据对象上做进一步操作。
        * */
        System.out.println("-----------------计算完成，开始写入hbase------------------------------");
        ipjds.foreach(new Function2<JavaPairRDD<String, Long>, Time, Void>() {
            public Void call(JavaPairRDD<String, Long> javaPairRDD, Time time)
                    throws Exception {
                javaPairRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Long>>>() {
                    public void call(Iterator<Tuple2<String, Long>> wordcounts) throws Exception {
                        //建立hbase连接，可以在HbaseConPool中配置连接池，每一次遍历都会新建一个conn对象，但是使用一个hbase连接
                        Connection conn = HbaseConPool.getConn();
                        Table table = conn.getTable(TableName.valueOf(tableName));
                        Tuple2<String, Long> wordcount = null;
                        //遍历每一条并插入hbase
                        while (wordcounts.hasNext()) {
                            wordcount = wordcounts.next();
                            System.out.println("---------------------browse_detail:" + wordcount._1.toString() + "," + wordcount._2.toString());
                            Put put = new Put(wordcount._1.toString().getBytes());
                            put.addColumn(columeFamily.getBytes(), wordcount._1.toString().getBytes(), wordcount._2.toString().getBytes());
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
        //从hbase的细节表中计算UV 总共4个参数 细节表名、结果表名、结果表列族、统计时间
        hbaseDayBrowseUV("browse_detail", "browse_pv_uv", "info", "2017-09-13");
        //从hbase的细节表中计算UV 总共5个参数 细节表名、结果表名、细节表列族、结果表列族、统计时间
        hbaseDayBrowsePV("browse_detail", "browse_pv_uv", "timeIp", "info", "2017-09-13");
        //打印
        ipjds.print();
    }

    //通过对hbase的browse_detail中rowkey的时候和value计算当天PV
    private static void hbaseDayBrowsePV(String fromTableName, String resultTableName, String fromColumeFamaly, String columeFamaly, String time) {
        long rowCount = 0;
        Connection conn = null;
        try {
            //获取hbase的连接信息
            conn = HbaseConPool.getConn();
            //获取浏览细节表
            Table table = conn.getTable(TableName.valueOf(fromTableName));
            Scan scan = new Scan();
            //使用FirstKeyOnlyFilter的方式只会取每条数据的第一个KV，计算总数的速度更快
            scan.setFilter(new FirstKeyOnlyFilter());
            scan.setCaching(500);
            scan.setCacheBlocks(false);

            //scan扫描
            ResultScanner resultScanner = table.getScanner(scan);
            for (Result result : resultScanner) {
                //获取rowkey
                String rowkey = Bytes.toString(result.getRow());
                byte[] value1 = result.getValue(Bytes.toBytes(fromColumeFamaly), result.getRow());
                //System.out.println("value------------"+value1[0]);
                Long value = Long.parseLong(String.valueOf(value1[0]));
                //获取rowkey中的时间 rowkey的格式是time+ip
                String rowkeyTime = rowkey.replace("+", ",").split(",")[0];
                //System.out.println("按天统计的UV数------------"+rowkey);
                //筛选时间等于2017-09-13的数据
                if (rowkeyTime.equals(time)) {
                    rowCount += value;
                }
            }
            System.out.println("按天统计的UV数------------" + rowCount);
            //获取结果表
            Table table1 = conn.getTable(TableName.valueOf(resultTableName));
            //把时间作为rowkey
            Put put = new Put(time.getBytes());
            //声明列族 列 值
            String colume = "PV";
            put.addColumn(columeFamaly.getBytes(), colume.getBytes(), String.valueOf(rowCount).getBytes());
            table1.put(put);
            //关闭对象
            if (conn != null) {
                conn.close();
            }
            if (table != null) {
                table.close();
            }
            if (table1 != null) {
                table.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //通过对hbase的browse_detail中的rowkey的时间作为维度计数统计当天UV
    private static void hbaseDayBrowseUV(String fromTableName, String resultTableName, String columeFamaly, String time) {
        long rowCount = 0;
        Connection conn = null;
        try {
            //获取hbase的连接信息
            conn = HbaseConPool.getConn();
            //获取浏览细节表
            Table table = conn.getTable(TableName.valueOf(fromTableName));
            Scan scan = new Scan();
            //使用FirstKeyOnlyFilter的方式只会取每条数据的第一条，计算总数的速度更快
            scan.setFilter(new FirstKeyOnlyFilter());
            scan.setCaching(500);
            scan.setCacheBlocks(false);

            //scan扫描
            ResultScanner resultScanner = table.getScanner(scan);
            for (Result result : resultScanner) {
                //获取rowkey
                String rowkey = Bytes.toString(result.getRow());
                //获取rowkey中的时间 rowkey的格式是time+ip
                String rowkeyTime = rowkey.replace("+", ",").split(",")[0];
                //System.out.println("按天统计的UV数------------"+rowkey);
                //筛选时间等于2017-09-13的数据
                if (rowkeyTime.equals(time)) {
                    rowCount += result.size();
                }
            }
            System.out.println("按天统计的UV数------------" + rowCount);
            //获取结果表
            Table table1 = conn.getTable(TableName.valueOf(resultTableName));
            //把时间作为rowkey
            Put put = new Put(time.getBytes());
            //声明列族 列 值
            put.addColumn(columeFamaly.getBytes(), "UV".getBytes(), String.valueOf(rowCount).getBytes());
            table1.put(put);
            //关闭对象
            if (conn != null) {
                conn.close();
            }
            if (table != null) {
                table.close();
            }
            if (table1 != null) {
                table.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //以IP为维度计算PV
    private static void ipPVToHbase(JavaPairInputDStream<String, String> lines) {
        /*
        * kafka推送过来的数据存放在lines这个对象中，数据格式：2017-09-21 08:42:49,1505954569944,305,1979,Impala,View
        * 下面的方法是对lines对象进行过滤，过滤下来第6个字段为View的数据，将结果存到一个新的RDD
        * */

        /*
        *将logsDStream进行mapToPair操作，将Tuple2中的value根据','分隔并取第1(时间)和第5（channel）个字段作为输出的key，给个初始值1
        *通过reduceByKey对相同的Key，进行Value的累计（包括Local和Reducer级别同时Reduce） Function2（T，T，R） T为输入 R为输出
         */
        JavaPairDStream<String, Long> ipjds = lines.mapToPair(new PairFunction<Tuple2<String, String>, String, Long>() {


            public Tuple2<String, Long> call(Tuple2<String, String> t) throws Exception {
                String[] logs = t._2.split("\t");
                //String ip = logs[0];
                return new Tuple2<String, Long>("PV", 1L);
            }
        }).reduceByKey(new Function2<Long, Long, Long>() { //对相同的Key，进行Value的累计（包括Local和Reducer级别同时Reduce）
            public Long call(Long v1, Long v2) throws Exception {
                return v1 + v2;
            }
        });
        /*
        * 最重要的就是这里 先对jds进行foreach， 会在集群里的不同机器上创建spark工作线程，
      * 如果Connection是全局变量，就会因为spark工作线程之间连接不共享导致的Connection对象没有被初始化的执行错误
        * 遍历每个分区foreachPartition，这个地方涉及到广播变量的问题，不同分区不共享广播变量，所以也需要在分区内建立一个Connection对象
      * 但是这样显然效率性能都很差，所以可以维护一个全局的静态的连接池对象HbaseConPool.getConn()， 这样就可以最好的复用connection
        * 针对为什么foreach内部还要foreachPartition或者嵌套foreach：foreachRDD 是一个输出操作符，
      * 它返回的不是RDD里的一行数据， 而是输出DStream后面的RDD,在一个时间间隔里， 只返回一个RDD的“微批次”，
      * 为了访问这个“微批次”RDD里的数据， 我们还需要在RDD数据对象上做进一步操作。
        * */
        System.out.println("-----------------计算完成，开始写入hbase------------------------------");
        ipjds.foreach(new Function2<JavaPairRDD<String, Long>, Time, Void>() {
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
        //打印
        ipjds.print();
    }

    //计算browser的UV
    private static void ipUVToHbase(JavaPairInputDStream<String, String> lines) {
        JavaPairDStream<String, String> logsDStream = lines.filter(new Function<Tuple2<String, String>, Boolean>() {

            public Boolean call(Tuple2<String, String> v1) throws Exception {
                String[] log = v1._2.split("\t");
                String no_Match = log[0];
                if ("No match!!!".equals(no_Match)) {
                    return false;
                } else {
                    return true;
                }
            }
        });
      /*
       * 第四步：接下来就像对于RDD编程一样基于DStream进行编程！！！原因是DStream是RDD产生的模板（或者说类），在Spark Streaming具体
       * 发生计算前，其实质是把每个Batch的DStream的操作翻译成为对RDD的操作！！！
       * 对初始的DStream进行Transformation级别的处理，例如map、filter等高阶函数等的编程，来进行具体的数据计算
       */

        //在单词拆分的基础上对每个单词实例计数为1，也就是word => (word, 1)
        JavaPairDStream<String, Long> browserjds = logsDStream.map(new Function<Tuple2<String, String>, String>() {

            public String call(Tuple2<String, String> v1) throws Exception {
                String[] logs = v1._2.split("\t");
                System.out.println("------------------------------------" + logs[0]);
                String agentIp = String.valueOf(logs[7]);
                //String ip = String.valueOf(logs[6] != null ? logs[6] : "-1" );
                //原文是Long usrID = Long.valueOf(logs[2] != null ? logs[2] : "-1" );
                //报错：java.lang.NumberFormatException: For input string: "null"
                //String dateToday = String.valueOf(logs[6]);
                return "UV" + "_" + agentIp;
            }
        }).transform(new Function<JavaRDD<String>, JavaRDD<String>>() {

            public JavaRDD<String> call(JavaRDD<String> v1) throws Exception {
                // TODO Auto-generated method stub
                return v1.distinct();
            }
        }).mapToPair(new PairFunction<String, String, Long>() {

            public Tuple2<String, Long> call(String t) throws Exception {
                String[] logs = t.split("_");
                String UV = String.valueOf(logs[0]);
                return new Tuple2<String, Long>(UV, 1L);
            }
        }).reduceByKey(new Function2<Long, Long, Long>() { //对相同的Key，进行Value的累计（包括Local和Reducer级别同时Reduce）

            public Long call(Long v1, Long v2) throws Exception {
                return v1 + v2;
            }
        });
        browserjds.foreach(new Function2<JavaPairRDD<String, Long>, Time, Void>() {
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
        //打印
        browserjds.print();

    }

    private static void visitTimePV(JavaPairInputDStream<String, String> lines) {
        /*
        * kafka推送过来的数据存放在lines这个对象中，数据格式：2017-09-21 08:42:49,1505954569944,305,1979,Impala,View
        * 下面的方法是对lines对象进行过滤，过滤下来第6个字段为View的数据，将结果存到一个新的RDD
        * */
        JavaPairDStream<String, String> logsDStream = lines.filter(new Function<Tuple2<String, String>, Boolean>() {

            public Boolean call(Tuple2<String, String> v1) throws Exception {
                String[] log = v1._2.split(",");
                String action = log[5];
                if ("View".equals(action)) {
                    return true;
                } else {
                    return false;
                }
            }
        });

        /*
        *将logsDStream进行mapToPair操作，将Tuple2中的value根据','分隔并取第1(时间)和第5（channel）个字段作为输出的key，给个初始值1
        *通过reduceByKey对相同的Key，进行Value的累计（包括Local和Reducer级别同时Reduce） Function2（T，T，R） T为输入 R为输出
         */
        JavaPairDStream<String, Long> jds = logsDStream.mapToPair(new PairFunction<Tuple2<String, String>, String, Long>() {


            public Tuple2<String, Long> call(Tuple2<String, String> t) throws Exception {
                String[] logs = t._2.split(",");
                String time = logs[0];
                String type = logs[4];
                return new Tuple2<String, Long>(time + "+" + type, 1L);
            }
        }).reduceByKey(new Function2<Long, Long, Long>() { //对相同的Key，进行Value的累计（包括Local和Reducer级别同时Reduce）
            public Long call(Long v1, Long v2) throws Exception {
                return v1 + v2;
            }
        });
        /*
        * 最重要的就是这里 先对jds进行foreach， 会在集群里的不同机器上创建spark工作线程，
      * 如果Connection是全局变量，就会因为spark工作线程之间连接不共享导致的Connection对象没有被初始化的执行错误
        * 遍历每个分区foreachPartition，这个地方涉及到广播变量的问题，不同分区不共享广播变量，所以也需要在分区内建立一个Connection对象
      * 但是这样显然效率性能都很差，所以可以维护一个全局的静态的连接池对象HbaseConPool.getConn()， 这样就可以最好的复用connection
        * 针对为什么foreach内部还要foreachPartition或者嵌套foreach：foreachRDD 是一个输出操作符，
      * 它返回的不是RDD里的一行数据， 而是输出DStream后面的RDD,在一个时间间隔里， 只返回一个RDD的“微批次”，
      * 为了访问这个“微批次”RDD里的数据， 我们还需要在RDD数据对象上做进一步操作。
        * */

        System.out.println("-----------------计算完成，开始写入hbase------------------------------");
        jds.foreach(new Function2<JavaPairRDD<String, Long>, Time, Void>() {
            public Void call(JavaPairRDD<String, Long> values, Time time)
                    throws Exception {
                values.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Long>>>() {
                    public void call(Iterator<Tuple2<String, Long>> wordcounts) throws Exception {
                        //建立hbase连接，可以在HbaseConPool中配置连接池，每一次遍历都会新建一个conn对象，但是使用一个hbase连接
                        Connection conn = HbaseConPool.getConn();
                        Table table = conn.getTable(TableName.valueOf("test1"));
                        Tuple2<String, Long> wordcount = null;
                        //遍历每一条并插入hbase
                        while (wordcounts.hasNext()) {
                            wordcount = wordcounts.next();
                            System.out.println("---------------------test:" + wordcount._1.toString() + "," + wordcount._2.toString());
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
        //打印
        jds.print();
    }

    /**
     * 一个测试的例子
     * 因为要计算UV，所以需要获得同样的Page的不同的User，这个时候就需要去重操作，DStreamzhong有distinct吗？当然没有（截止到Spark 1.6.1的时候还没有该Api）
     * 此时我们就需要求助于DStream魔术般的方法tranform,在该方法内部直接对RDD进行distinct操作，这样就是实现了用户UserID的去重，进而就可以计算出UV了。
     *
     * @param lines
     */
    private static void onlineUV(JavaPairInputDStream<String, String> lines) {
      /*
       * 第四步：接下来就像对于RDD编程一样基于DStream进行编程！！！原因是DStream是RDD产生的模板（或者说类），在Spark Streaming具体
       * 发生计算前，其实质是把每个Batch的DStream的操作翻译成为对RDD的操作！！！
       * 对初始的DStream进行Transformation级别的处理，例如map、filter等高阶函数等的编程，来进行具体的数据计算
       */
        JavaPairDStream<String, String> logsDStream = lines.filter(new Function<Tuple2<String, String>, Boolean>() {

            public Boolean call(Tuple2<String, String> v1) throws Exception {
                String[] logs = v1._2.split(",");
                String action = logs[5];
                if ("View".equals(action)) {
                    return true;
                } else {
                    return false;
                }
            }
        });

        //在单词拆分的基础上对每个单词实例计数为1，也就是word => (word, 1)
        logsDStream.map(new Function<Tuple2<String, String>, String>() {

            public String call(Tuple2<String, String> v1) throws Exception {
                String[] logs = v1._2.split(",");
                String usrID = String.valueOf(logs[2] != null ? logs[2] : "-1");
                //原文是Long usrID = Long.valueOf(logs[2] != null ? logs[2] : "-1" );
                //报错：java.lang.NumberFormatException: For input string: "null"
                String dateToday = String.valueOf(logs[0]);
                return dateToday + "_" + usrID;
            }
        }).transform(new Function<JavaRDD<String>, JavaRDD<String>>() {

            public JavaRDD<String> call(JavaRDD<String> v1) throws Exception {
                // TODO Auto-generated method stub
                return v1.distinct();
            }
        }).mapToPair(new PairFunction<String, String, Long>() {

            public Tuple2<String, Long> call(String t) throws Exception {
                String[] logs = t.split("_");
                String dateToday = String.valueOf(logs[0]);
                return new Tuple2<String, Long>(dateToday, 1L);
            }
        }).reduceByKey(new Function2<Long, Long, Long>() { //对相同的Key，进行Value的累计（包括Local和Reducer级别同时Reduce）

            public Long call(Long v1, Long v2) throws Exception {
                return v1 + v2;
            }
        }).print();

    }
}
