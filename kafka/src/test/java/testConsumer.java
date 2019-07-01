import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class testConsumer {
    private static String sql = "";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "172.31.42.222:9092");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");
        props.put("group.id","C4");
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList("cc_userBehavior"));
        int count = 0;
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                count++;

            }
        } finally {
            System.out.println(count);
            consumer.close();
        }
    }
}
