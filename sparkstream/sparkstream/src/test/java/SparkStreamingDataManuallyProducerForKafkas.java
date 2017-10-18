/**
 * Created by 11325 on 2017/9/14.
 */
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * 这里产生数据，就会发送给kafka，kafka那边启动消费者，就会接收到数据，这一步是用来测试生成数据和消费数据没有问题的，确定没问题后要关闭消费者，
 * 启动OnlineBBSUserLogss.java的类作为消费者，就会按pv，uv等方式处理这些数据。
 * 因为一个topic只能有一个消费者，所以启动程序前必须关闭kafka方式启动的消费者（我这里没有关闭关闭kafka方式启动的消费者也没正常啊）
 */
public class SparkStreamingDataManuallyProducerForKafkas extends Thread{

    //具体的论坛频道
    static String[] channelNames = new  String[]{
            "Spark","Scala","Kafka","Flink","Hadoop","Storm",
            "Hive","Impala","HBase","ML"
    };
    //用户的两种行为模式
    static String[] actionNames = new String[]{"View", "Register"};
    private static Producer<String, String> producerForKafka;
    //private static String dateToday;
    private static Random random;

    //2、作为线程而言，要复写run方法，先写业务逻辑，再写控制
    @Override
    public void run() {
        int counter = 0;//搞500条
        while(true){//模拟实际情况，不断循环，异步过程，不可能是同步过程
            counter++;
            String userLog = userlogs();
            System.out.println("product:"+userLog);
            //"test"为topic
            producerForKafka.send(new KeyedMessage<String, String>("test2", userLog));
            if(0 == counter%10){
                counter = 0;
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
    }

    private static String userlogs() {
        StringBuffer userLogBuffer = new StringBuffer("");
        int[] unregisteredUsers = new int[]{1, 2, 3, 4, 5, 6, 7, 8};
        long timestamp = new Date().getTime();
        Long userID = 0L;
        long pageID = 0L;
        //随机生成的用户ID
        if(unregisteredUsers[random.nextInt(8)] == 1) {
            userID = null;
        } else {
            userID = (long) random.nextInt((int) 2000);
        }
        //随机生成的页面ID
        pageID =  random.nextInt((int) 2000);
        //随机生成Channel
        String channel = channelNames[random.nextInt(10)];
        //随机生成action行为
        String action = actionNames[random.nextInt(2)];

        userLogBuffer.append(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()))
                .append(",")
                .append(timestamp)
                .append(",")
                .append(userID)
                .append(",")
                .append(pageID)
                .append(",")
                .append(channel)
                .append(",")
                .append(action);   //这里不要加\n换行符，因为kafka自己会换行，再append一个换行符，消费者那边就会处理不出数据
        return userLogBuffer.toString();
    }

    public static void main(String[] args) throws Exception {
        //dateToday = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
        random = new Random();
        Properties props = new Properties();
        props.put("zk.connect", "hue:2181,dwdb01:2181,coreserver05:2181");
        props.put("metadata.broker.list","coreserver05.zbjt.com:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        ProducerConfig config = new ProducerConfig(props);
        producerForKafka = new Producer<String, String>(config);
        new SparkStreamingDataManuallyProducerForKafkas().start();
    }
}
