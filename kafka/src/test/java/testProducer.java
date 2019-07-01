
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;


import java.io.*;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class testProducer{
    public static void main(String[] args) throws InterruptedException, ExecutionException, IOException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "172.31.42.222:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", "all");
        //重新发送消息次数，到达次数返回错误
        props.put("retries", 30);
        //Producer会尝试去把发往同一个Partition的多个Requests进行合并，batch.size指明了一次Batch合并后Requests总大小的上限。如果这个值设置的太小，可能会导致所有的Request都不进行Batch。
        props.put("batch.size", 1024);
        //Producer默认会把两次发送时间间隔内收集到的所有Requests进行一次聚合然后再发送，以此提高吞吐量，而linger.ms则更进一步，这个参数为每次发送增加一些delay，以此来聚合更多的Message。
        props.put("linger.ms", 1);
        //在Producer端用来存放尚未发送出去的Message的缓冲区大小
        props.put("buffer.memory", 33554432);

        props.put("request.timeout.ms", "60000");
        //props.put("acks", "all");
        //String topic = "mykafka";
        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        String str = null;
        int i = 0;
        FileInputStream inputStream = new FileInputStream("D:\\test\\data\\userBehavior\\userBehavior");
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));

        long startTime =  System.currentTimeMillis();

        while ((str = bufferedReader.readLine()) != null){
            i++;
            producer.send(new ProducerRecord<String, String>("newUserProducer",Integer.toString(i),str));

        }
        /*for (int j = 0; j < 10000000; j++) {
            producer.send(new ProducerRecord<String, String>("ccTestProducer",Integer.toString(j),Integer.toString(j)));
        }*/
        System.out.println(i);
        long endTime =  System.currentTimeMillis();
        long usedTime = (endTime-startTime)/1000;
        System.out.println(usedTime);
        producer.close();
    }
}