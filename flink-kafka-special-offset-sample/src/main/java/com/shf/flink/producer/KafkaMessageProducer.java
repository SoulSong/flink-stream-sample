package com.shf.flink.producer;

import com.alibaba.fastjson.JSONObject;
import com.shf.flink.constants.Constants;
import com.shf.flink.event.UserBehaviorEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class KafkaMessageProducer {

    static SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd");
    static SimpleDateFormat sdf2 = new SimpleDateFormat("HH:mm:ss");

    public static void main(String[] args) {
        Map<String, Object> kafkaParam = new HashMap<>(3);
        String topic = args.length == 0 ? Constants.SPECIAL_OFFSET_TOPIC : args[0];
        kafkaParam.put("bootstrap.servers", Constants.KAFKA_BOOTSTRAP_SERVER);
        kafkaParam.put("key.serializer", StringSerializer.class.getName());
        kafkaParam.put("value.serializer", StringSerializer.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(kafkaParam);
        int index = 0;
        while (index < 3000) {
            String msg = generateMessage();
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, msg);
            kafkaProducer.send(record);
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            index++;
        }
        kafkaProducer.close();
    }

    /**
     * 构造消息
     * @return
     */
    public static String generateMessage() {
        Date date = new Date();
        StringBuffer ts = new StringBuffer();
        ts.append(sdf1.format(date))
                .append("T")
                .append(sdf2.format(date))
                .append("Z");
        String[] action = {"click", "bug", "login", "logout"};
        String[] category = {"c1", "c2", "c3", "c4"};
        UserBehaviorEvent userBehaviorEvent = new UserBehaviorEvent();
        userBehaviorEvent.setUserId(new Random().nextInt(3));
        userBehaviorEvent.setItemId(new Random().nextInt(200));
        userBehaviorEvent.setCategory(category[new Random().nextInt(category.length)]);
        userBehaviorEvent.setAction(action[new Random().nextInt(action.length)]);
        userBehaviorEvent.setTs(System.currentTimeMillis());
        return JSONObject.toJSONString(userBehaviorEvent);
    }
}




