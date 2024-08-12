package com.qsdi.bigdata.graph.gstore.performance.test.job.kafka;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Description
 *
 * @author lijie0203 2024/7/16 9:15
 */
public class GraphKafkaProducer {

    private final KafkaProducer<String, String> kafkaProducer;

    public GraphKafkaProducer(Properties properties) {
        this.kafkaProducer = new KafkaProducer<>(properties);
    }


    public void close() throws Exception {
        kafkaProducer.close();
    }

    public <T> void send(String topic, T value) {
        kafkaProducer.send(
                new ProducerRecord<>(topic, JSON.toJSONString(value)));
    }
}
