import org.apache.kafka.clients.producer.*;

import java.io.*;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class testProducerB {
    public static void main(String[] args) throws InterruptedException, ExecutionException, IOException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "172.31.42.222:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
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
            ProducerRecord<String,String> record = new ProducerRecord<String, String>("ccTestProducerB",Integer.toString(i),str);
            producer.send(record,
                    new Callback() {
                        public void onCompletion(RecordMetadata metadata, Exception e) {
                            if(e != null) {
                                e.printStackTrace();
                            } else {
                                System.out.println("The offset of the record we just sent is: " + metadata.offset());
                            }
                        }
                    });
        }

        long endTime =  System.currentTimeMillis();
        long usedTime = (endTime-startTime)/1000;
        System.out.println(usedTime);
        producer.close();
    }
}
