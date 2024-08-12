//package com.qsdi.bigdata.graph.gstore.performance.test.job.kafka;
//
//import com.alibaba.fastjson.JSONObject;
//import com.qsdi.bigdata.graph.gstore.performance.test.job.kafka.deserialization.DefaultKafkaDeserialization;
//import com.qsdi.bigdata.graph.gstore.performance.test.job.kafka.deserialization.KafkaDeserializationSchema;
//import org.apache.kafka.clients.admin.AdminClient;
//import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
//import org.apache.kafka.clients.admin.TopicDescription;
//import org.apache.kafka.clients.consumer.ConsumerRecord;
//import org.apache.kafka.clients.consumer.ConsumerRecords;
//import org.apache.kafka.clients.consumer.KafkaConsumer;
//import org.apache.kafka.clients.consumer.OffsetAndMetadata;
//import org.apache.kafka.common.KafkaFuture;
//import org.apache.kafka.common.TopicPartition;
//import org.apache.kafka.common.TopicPartitionInfo;
//
//import java.time.Duration;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.Properties;
//import java.util.concurrent.ExecutionException;
//
///**
// * Description
// *
// * @author lijie0203 2024/7/16 9:15
// */
//public class GraphKafkaConsumer {
//    private List<TopicPartition> topicPartitions;
//    private final KafkaConsumer<byte[], byte[]> consumer;
//
//    private AdminClient adminClient;
//
//    private Properties properties;
//
//    private final KafkaDeserializationSchema<JSONObject> kafkaDeserializationSchema = new DefaultKafkaDeserialization();
//
//
//
//    public GraphKafkaConsumer(Properties properties) {
//        this.properties=properties;
//        this.consumer=new KafkaConsumer<>(properties);
//         adminClient = AdminClient.create(properties);
//    }
//
//    public void subscribe(List<String> topics) throws ExecutionException, InterruptedException {
//        consumer.subscribe(topics);
//        findTopicAndPartition(topics);
//    }
//
//    private void findTopicAndPartition(List<String> topics)
//            throws ExecutionException, InterruptedException {
//        Map<String, KafkaFuture<TopicDescription>> describeTopicsResult =
//                adminClient.describeTopics(topics).values();
//        topicPartitions = new ArrayList<>();
//        for (Map.Entry<String, KafkaFuture<TopicDescription>> entry : describeTopicsResult.entrySet()) {
//            KafkaFuture<TopicDescription> topicDescriptionKafkaFuture =
//                    describeTopicsResult.get(entry.getKey());
//            for (TopicPartitionInfo partition : topicDescriptionKafkaFuture.get().partitions()) {
//                topicPartitions.add(new TopicPartition(entry.getKey(), partition.partition()));
//            }
//        }
//    }
//
//    /**
//     * 获取值
//     *
//     * @return List<T>
//     * @throws Exception e
//     */
//    public List<JSONObject> getRecords() throws Exception {
//        ConsumerRecords<byte[], byte[]> sources = consumer.poll(Duration.ofSeconds(3));
//        List<JSONObject> lines = new ArrayList<>();
//        if (!sources.isEmpty()) {
//            for (ConsumerRecord<byte[], byte[]> source : sources) {
//                lines.add(kafkaDeserializationSchema.deserialize(source));
//            }
//        }
//        return lines;
//    }
//
//    public void commitSync() {
//        consumer.commitSync();
//    }
//
//    /**
//     * 回滚偏移量
//     *
//     * @throws ExecutionException   e
//     * @throws InterruptedException e
//     */
//    public void resetOffset() throws ExecutionException, InterruptedException {
//        String groupId = properties.getProperty("group.id");
//        String autoOffsetReset = properties.getProperty("auto.offset.reset", "earliest");
//        ListConsumerGroupOffsetsResult listConsumerGroupOffsetsResult =
//                adminClient.listConsumerGroupOffsets(groupId);
//        Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadataMap =
//                listConsumerGroupOffsetsResult.partitionsToOffsetAndMetadata().get();
//        Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsetMap = new HashMap<>();
//        List<TopicPartition> noOffset = new ArrayList<>();
//        topicPartitions.forEach(
//                topicPartition -> {
//                    OffsetAndMetadata offsetAndMetadata =
//                            topicPartitionOffsetAndMetadataMap.get(topicPartition);
//                    if (offsetAndMetadata == null) {
//                        noOffset.add(topicPartition);
//                    } else {
//                        topicPartitionOffsetMap.put(topicPartition, offsetAndMetadata);
//                    }
//                });
//
//        if (!noOffset.isEmpty()) {
//            if (autoOffsetReset.equals("earliest")) {
//                consumer.seekToBeginning(noOffset);
//            } else {
//                consumer.seekToEnd(noOffset);
//            }
//        }
//        topicPartitionOffsetMap.forEach(
//                (topicPartition, offsetAndMetadata) ->
//                        consumer.seek(topicPartition, offsetAndMetadata.offset()));
//    }
//
//    public void close() throws Exception {
//        consumer.close();
//        adminClient.close();
//    }
//}
