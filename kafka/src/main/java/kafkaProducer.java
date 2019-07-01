import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class kafkaProducer extends Thread{

    private String topic;

    private String filePath;

    public kafkaProducer(String topic,String filePath){
        super();
        this.topic = topic;
        this.filePath = filePath;
    }

    private Producer createProducer(){
        Properties props = new Properties();
        props.put("bootstrap.servers", "172.31.42.222:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //0: 不需要进行确认，速度最快。存在丢失数据的风险。
        //1: 仅需要Leader进行确认，不需要ISR进行确认。是一种效率和安全折中的方式。
        //all: 需要ISR中所有的Replica给予接收确认，速度最慢，安全性最高，但是由于ISR可能会缩小到仅包含一个Replica，所以设置参数为all并不能一定避免数据丢失。
        props.put("acks", "all");
        //重新发送消息次数，到达次数返回错误
        props.put("retries", 30);
        //Producer会尝试去把发往同一个Partition的多个Requests进行合并，batch.size指明了一次Batch合并后Requests总大小的上限。如果这个值设置的太小，可能会导致所有的Request都不进行Batch。
        //props.put("batch.size", 1024);
        //Producer默认会把两次发送时间间隔内收集到的所有Requests进行一次聚合然后再发送，以此提高吞吐量，而linger.ms则更进一步，这个参数为每次发送增加一些delay，以此来聚合更多的Message。
        props.put("linger.ms", 1);
        //在Producer端用来存放尚未发送出去的Message的缓冲区大小
        props.put("buffer.memory", 33554432);

        props.put("request.timeout.ms", "60000");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        return producer;
    }

    public void run() {
        Producer producer = createProducer();
        String str = null;
        int i = 0;

        FileInputStream inputStream = null;

        try {
            inputStream = new FileInputStream(filePath);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));

        try {
            long startTime =  System.currentTimeMillis();
            while ((str = bufferedReader.readLine()) != null){
                i++;
                producer.send(new ProducerRecord<String, String>(topic,Integer.toString(i),str)).get();

                //Thread.sleep(1);

                //System.out.println(i);
            }
            long endTime =  System.currentTimeMillis();
            long usedTime = (endTime-startTime)/1000;
            System.out.println(i);
            System.out.println(usedTime);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args){

        new kafkaProducer("ccTestProducer","D:\\test\\data\\userBehavior\\userBehavior").start();
        //new kafkaProducer("cc_ariticleInfo","D:\\test\\articleInfo\\articleInfo").start();
        //new kafkaProducer("cc_userBasic","D:\\test\\userBasic\\userBasic").start();
        //new kafkaProducer("cc_userBehavior","D:\\test\\userBehavior\\userBehavior").start();
        //new kafkaProducer("cc_userEdu","D:\\test\\userEdu\\userEdu").start();
        //new kafkaProducer("cc_userInterest","D:\\test\\userInterest\\userInterest").start();
        //new kafkaProducer("cc_userSkill","D:\\test\\userSkill\\userSkill").start();
    }
}