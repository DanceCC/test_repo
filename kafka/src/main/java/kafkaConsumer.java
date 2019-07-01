import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class kafkaConsumer extends Thread{
    private String topic;
    private String sql;

    public kafkaConsumer(String topic){
        super();
        this.topic = topic;
    }

    private KafkaConsumer createConsumer(){
        Properties props = new Properties();
        props.put("bootstrap.servers", "172.31.42.222:9092");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");
        props.put("group.id","BDH3");
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(props);
        return consumer;
    }

    public void run(){
        KafkaConsumer consumer = createConsumer();
        consumer.subscribe(Arrays.asList(topic));
        Connection con = DBConnection.getConnection();
        while (true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records){
                String[] temp = record.value().split("\u0001");
                if (topic.equals("cc_ariticleInfo")){
                    String subSql = "'"+temp[0]+"','"+temp[1]+"','"+temp[2]+"','"+temp[3]+"','"+temp[4]+"','"+temp[5]+"','"+temp[6]+"','"+temp[7]+"'";
                    sql = "INSERT INTO ArticleInfo(Aid,DiggCount,BuryCount,ViewCount,CommentCount,AType,IsTop,Status) VALUES ("+ subSql +");";
                }else if (topic.equals("cc_userBasic")){
                    String subSql = "'"+temp[0]+"','"+temp[1]+"','"+temp[2]+"','"+temp[3]+"','"+temp[4]+"','"+temp[5]+"'";
                    sql = "INSERT INTO UserBasic(Uid,Gender,Status,FollowNum,FansNum,FriendNum) VALUES ("+ subSql +");";
                }else if (topic.equals("cc_userBehavior")){
                    String subSql = "'"+temp[0]+"','"+temp[1]+"','"+temp[2]+"','"+temp[3]+"'";
                    sql = "INSERT INTO UserBehavior(Uid,Behavior,Aid,BehaviorTime) VALUES ("+ subSql +");";
                }else if (topic.equals("cc_userEdu")){
                    String subSql = "'"+temp[0]+"','"+temp[1]+"','"+temp[2]+"','"+temp[3]+"'";
                    sql = "INSERT INTO UserEdu(Uid,Degree,SchoolName,MajorStr) VALUES ("+ subSql +");";
                }else if (topic.equals("cc_userInterest")){
                    String subSql = "'"+temp[0]+"','"+temp[1]+"'";
                    sql = "INSERT INTO UserInterest(Uid,InterestName) VALUES ("+ subSql +");";
                }else if (topic.equals("cc_userSkill")){
                    String subSql = "'"+temp[0]+"','"+temp[1]+"'";
                    sql = "INSERT INTO UserSkill(Uid,SkillName) VALUES ("+ subSql +");";
                }
                //System.out.println(sql);
                try {
                    Statement st = con.createStatement();
                    st.execute(sql);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    public static void main(String[] args) {
        new kafkaConsumer("cc_ariticleInfo").start();
        new kafkaConsumer("cc_userBasic").start();
        new kafkaConsumer("cc_userBehavior").start();
        //new kafkaConsumer("cc_userEdu").start();
        new kafkaConsumer("cc_userInterest").start();
        new kafkaConsumer("cc_userSkill").start();
    }
}
