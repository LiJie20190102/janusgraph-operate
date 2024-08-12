///*
// * Copyright (c) 2022. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
// * Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
// * Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
// * Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
// * Vestibulum commodo. Ut rhoncus gravida arcu.
// */
//
//package com.qsdi.bigdata.graph.gstore.performance.test.job.kafka.deserialization;
//
//import com.alibaba.fastjson.JSONException;
//import com.alibaba.fastjson.JSONObject;
//import org.apache.kafka.clients.consumer.ConsumerRecord;
//
//import java.nio.charset.StandardCharsets;
//
//import static com.alibaba.fastjson.JSON.parseObject;
//import static com.qsdi.bigdata.graph.gstore.performance.test.job.constant.Constants.KAFKA_FORMAT_ERROR_KEY;
//import static com.qsdi.bigdata.graph.gstore.performance.test.job.constant.Constants.KAFKA_METADATA_KEY;
//
///** 默认kafka发序列化 */
//public class DefaultKafkaDeserialization implements KafkaDeserializationSchema<JSONObject> {
//  private final boolean metaData;
//
//  public DefaultKafkaDeserialization(boolean metaData) {
//    this.metaData = metaData;
//  }
//
//  public DefaultKafkaDeserialization() {
//    this(true);
//  }
//
//  /**
//   * @param record record
//   * @return JSONObject
//   * @throws Exception E
//   */
//  @Override
//  public JSONObject deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
//
//    String line = new String(record.value(), StandardCharsets.UTF_8);
//    JSONObject jsonObject = null;
//    try {
//      jsonObject = parseObject(line);
//    } catch (JSONException jsonException) {
//      jsonObject = new JSONObject();
//      jsonObject.put(KAFKA_FORMAT_ERROR_KEY, "1");
//      return jsonObject;
//    }
//
//    // 保存元数据信息
//    if (metaData) {
//      long offset = record.offset();
//      int partition = record.partition();
//      long timestamp = record.timestamp();
//      JSONObject metadata = new JSONObject();
//      metadata.put("timestamp", timestamp);
//      metadata.put("partition", partition);
//      metadata.put("offset", offset);
//      jsonObject.put(KAFKA_METADATA_KEY, metadata);
//    }
//
//    return jsonObject;
//  }
//}
