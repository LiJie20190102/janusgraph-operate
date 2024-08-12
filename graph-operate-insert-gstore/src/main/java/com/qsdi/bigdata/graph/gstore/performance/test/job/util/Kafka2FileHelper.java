//package com.qsdi.bigdata.graph.gstore.performance.test.job.util;
//
//import cn.hutool.core.thread.ThreadUtil;
//import com.alibaba.fastjson.JSON;
//import com.alibaba.fastjson.JSONArray;
//import com.alibaba.fastjson.JSONObject;
//import com.qsdi.bigdata.graph.gstore.performance.test.job.conf.BaseConf;
//import com.qsdi.bigdata.graph.gstore.performance.test.job.kafka.GraphKafkaConsumer;
//import com.qsdi.bigdata.graph.gstore.structure.graph.GStoreBatchVertexRequest;
//import com.qsdi.bigdata.multi.graph.api.struct.enums.DataSource;
//import com.qsdi.bigdata.multi.graph.api.struct.enums.QsdiElementUpdateStrategy;
//import com.qsdi.bigdata.multi.graph.api.struct.enums.QsdiUpdateStrategy;
//import com.qsdi.bigdata.multi.graph.api.struct.model.graph.QsdiVertex;
//import com.qsdi.bigdata.multi.graph.common.util.CollectionUtil;
//import com.qsdi.bigdata.multi.graph.common.util.ConcurrencyUtil;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.BufferedWriter;
//import java.io.FileWriter;
//import java.io.IOException;
//import java.util.Arrays;
//import java.util.Comparator;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.Objects;
//import java.util.Properties;
//import java.util.concurrent.ExecutionException;
//import java.util.function.Consumer;
//import java.util.stream.Collectors;
//
//import static com.qsdi.bigdata.graph.gstore.performance.test.job.constant.Constants.KAFKA_FORMAT_ERROR_KEY;
//import static com.qsdi.bigdata.graph.gstore.performance.test.job.constant.Constants.KAFKA_METADATA_KEY;
//
///**
// * Description
// *
// * @author lijie0203 2024/7/17 9:05
// */
//public class Kafka2FileHelper implements AutoCloseable {
//    private static final Logger LOGGER = LoggerFactory.getLogger(Kafka2FileHelper.class);
//
//    private final GraphKafkaConsumer graphKafkaConsumer;
//
//    private final BaseConf baseConf;
//
//    private static final long fileElementCount = 1000000;
//    private static  long saveCount = 0;
//
//    private final BufferedWriter writer;
//
//
//    public Kafka2FileHelper() throws ExecutionException, InterruptedException, IOException {
//        baseConf = new BaseConf();
//        Properties kafkaProperties = baseConf.getKafkaProperties();
//        graphKafkaConsumer = new GraphKafkaConsumer(kafkaProperties);
//        String relationTopic = baseConf.getRelationTopic();
//        graphKafkaConsumer.subscribe(Arrays.asList(relationTopic));
//
//        writer = new BufferedWriter(new FileWriter(baseConf.getVertexDataFile()));
//
//    }
//
//    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
//        Kafka2FileHelper job = new Kafka2FileHelper();
//        job.run();
//    }
//
//
//    public void run() {
//            for (; ; ) {
//                // 无限循环执行任务
//                jobStart();
//            }
//    }
//
//
//    private void jobStart() {
//        try {
//            List<JSONObject> records = graphKafkaConsumer.getRecords();
//            if (!records.isEmpty()) {
//                logicHandle(records);
////                graphKafkaConsumer.commitSync();
//            }
//        } catch (Exception e) {
//            LOGGER.warn(e.getMessage(), e);
//            for (; ; ) {
//                try {
//                    graphKafkaConsumer.resetOffset();
//                    break;
//                } catch (Exception re) {
//                    LOGGER.warn(e.getMessage(), re);
//                    ThreadUtil.safeSleep(5000);
//                }
//            }
//        }
//    }
//
//    /**
//     * 处理逻辑
//     *
//     * @param records records
//     * @param writer
//     */
//    private void logicHandle(List<JSONObject> records) {
//        // 过滤不需要的数据
//        List<JSONObject> kafkaResults =
//                records.stream()
//                        .map(
//                                jsonObject -> {
//                                    try {
//                                        if (jsonObject.containsKey(KAFKA_FORMAT_ERROR_KEY)) {
//                                            return null;
//                                        }
//                                        return jsonObject;
//                                    } catch (Exception e) {
//                                        LOGGER.warn("解析错误 ---> " + jsonObject.toJSONString());
//                                        return null;
//                                    }
//                                })
//                        .filter(Objects::nonNull)
//                        .sorted(
//                                Comparator.comparing(
//                                        a ->
//                                                (a.getJSONObject(KAFKA_METADATA_KEY)
//                                                        .getLong("timestamp")))) // sorted for save
//                        .collect(Collectors.toList());
//
//        if (!kafkaResults.isEmpty()) {
//
//            saveData2Graph(kafkaResults, writer);
//
//        }
//    }
//
////    private void saveData2Graph(List<JSONObject> kafkaResults, BufferedWriter writer) {
////        Map<String, List<QsdiVertex>> map =
////                kafkaResults.stream()
////                        .map(
////                                jsonObject -> {
////                                    String taskId = jsonObject.getString("taskId");
////
////                                    JSONObject entityInfo = jsonObject.getJSONObject("entityInfo");
////                                    String label = entityInfo.getString("label");
////
////                                    QsdiVertex qsdiVertex = new QsdiVertex();
////                                    qsdiVertex.setLabel(label);
////                                    qsdiVertex.setTaskId(taskId);
////                                    qsdiVertex.setDataSource(DataSource.TASK);
////
////                                    Integer partition =
////                                            jsonObject
////                                                    .getJSONObject(KAFKA_METADATA_KEY)
////                                                    .getInteger("partition");
////                                    Long offset =
////                                            jsonObject.getJSONObject(KAFKA_METADATA_KEY).getLong("offset");
////                                    String record = partition + "#" + offset;
////
////                                    qsdiVertex.setRecordId(record);
////
////                                    JSONObject propertiesMapping = entityInfo.getJSONObject("propertiesMapping");
////
////                                    Map<String, JSONObject> jsonObjectMap = new HashMap<>();
////
////                                    if (propertiesMapping != null) {
////                                        for (Object key : propertiesMapping.keySet()) {
////                                            JSONObject keyObj = JSON.parseObject(key.toString());
////                                            jsonObjectMap.put(keyObj.getString("columnName"), keyObj);
////                                        }
////                                    }
////
////                                    JSONArray fieldInfoList = jsonObject.getJSONArray("fieldInfoList");
////                                    if (fieldInfoList != null && fieldInfoList.size() > 0) {
////                                        Map<String, Object> properies = new HashMap<>();
////                                        for (int i = 0; i < fieldInfoList.size(); i++) {
////                                            JSONObject fieldInfo = (JSONObject) fieldInfoList.get(i);
////                                            String fieldName = fieldInfo.getString("fieldName");
////                                            JSONObject jsonObject1 = jsonObjectMap.get(fieldName);
////                                            if (!jsonObjectMap.containsKey(fieldName)) {
////                                                LOGGER.warn("can not find property info " + label + "--->" + fieldName);
////                                            } else {
////                                                String columnDataType = jsonObject1.getString("columnDataType");
////                                                Object value = GraphUtil.graphValue(columnDataType, fieldInfo);
////                                                properies.put(fieldName, value);
////                                            }
////                                        }
////                                        qsdiVertex.setProperties(properies);
////                                    }
////                                    String storageType = jsonObject.getString("storageType");
////                                    qsdiVertex.setElementUpdateStrategy(
////                                            QsdiElementUpdateStrategy.valueOf(storageType));
////                                    if (qsdiVertex.getElementUpdateStrategy() == QsdiElementUpdateStrategy.RETAIN
////                                            || qsdiVertex.getElementUpdateStrategy()
////                                            == QsdiElementUpdateStrategy.OVERRIDE) {
////                                        Map<String, Object> properties = qsdiVertex.getProperties();
////                                        if (properties == null) {
////                                            properties = new HashMap<>();
////                                        }
////                                        for (String key : jsonObjectMap.keySet()) {
////                                            if (!properties.containsKey(key)) {
////                                                properties.put(key, null);
////                                            }
////                                        }
////                                        qsdiVertex.setProperties(properties);
////                                    }
////
////                                    Map<String, String> propertyUpdateStrategies =
////                                            (Map<String, String>) jsonObject.get("propertyUpdateStrategies");
////                                    if (CollectionUtil.isNotEmpty(propertyUpdateStrategies)) {
////                                        Map<String, QsdiUpdateStrategy> updateStrategyMap = new HashMap<>();
////                                        propertyUpdateStrategies.forEach(
////                                                (properyName, strategy) -> {
////                                                    updateStrategyMap.put(
////                                                            properyName, QsdiUpdateStrategy.valueOf(strategy));
////                                                });
////                                        qsdiVertex.setPropertyUpdateStrategies(updateStrategyMap);
////                                    }
////
////                                    return new Object[]{taskId, qsdiVertex};
////                                })
////                        .collect(
////                                Collectors.groupingBy(
////                                        o -> o[0] == null ? "" : o[0].toString(),
////                                        Collectors.mapping(o -> (QsdiVertex) o[1], Collectors.toList())));
////
////
////        map.forEach(
////                (k, v) -> {
////
////
////                    v.forEach(vertex -> {
////                        try {
////                            writer.write(JSON.toJSONString(vertex));
////                            writer.newLine();
////                        } catch (IOException e) {
////                            throw new RuntimeException(e);
////                        }
////                    });
////                    saveCount += v.size();
////
////                    LOGGER.info("save size {},and all is {}",v.size(),saveCount);
////
////
////                });
////    }
//
//
//    private void saveData2Graph(List<JSONObject> kafkaResults, BufferedWriter writer) {
//        Map<String, List<QsdiVertex>> map =
//                kafkaResults.stream()
//                        .map(
//                                jsonObject -> {
//
//                                    JSONObject entityInfo = jsonObject.getJSONObject("entityInfo");
//
//                                    QsdiVertex qsdiVertex = new QsdiVertex();
//                                    qsdiVertex.setLabel("real_name_archive_test");
//                                    qsdiVertex.setDataSource(DataSource.TASK);
//
//                                    Integer partition =
//                                            jsonObject
//                                                    .getJSONObject(KAFKA_METADATA_KEY)
//                                                    .getInteger("partition");
//                                    Long offset =
//                                            jsonObject.getJSONObject(KAFKA_METADATA_KEY).getLong("offset");
//                                    String record = partition + "#" + offset;
//
//                                    qsdiVertex.setRecordId(record);
//
//                                    JSONObject propertiesMapping = entityInfo.getJSONObject("propertiesMapping");
//
//                                    Map<String, JSONObject> jsonObjectMap = new HashMap<>();
//
//                                    if (propertiesMapping != null) {
//                                        for (Object key : propertiesMapping.keySet()) {
//                                            JSONObject keyObj = JSON.parseObject(key.toString());
//                                            jsonObjectMap.put(keyObj.getString("columnName"), keyObj);
//                                        }
//                                    }
//
//                                    JSONArray fieldInfoList = jsonObject.getJSONArray("fieldInfoList");
//                                    if (fieldInfoList != null && fieldInfoList.size() > 0) {
//                                        Map<String, Object> properies = new HashMap<>();
//                                        for (int i = 0; i < fieldInfoList.size(); i++) {
//                                            JSONObject fieldInfo = (JSONObject) fieldInfoList.get(i);
//                                            String fieldName = fieldInfo.getString("fieldName");
//                                            JSONObject jsonObject1 = jsonObjectMap.get(fieldName);
//                                            if (!jsonObjectMap.containsKey(fieldName)) {
//                                                LOGGER.warn("can not find property info " + label + "--->" + fieldName);
//                                            } else {
//                                                String columnDataType = jsonObject1.getString("columnDataType");
//                                                Object value = GraphUtil.graphValue(columnDataType, fieldInfo);
//                                                properies.put(fieldName, value);
//                                            }
//                                        }
//                                        qsdiVertex.setProperties(properies);
//                                    }
//                                    String storageType = jsonObject.getString("storageType");
//                                    qsdiVertex.setElementUpdateStrategy(
//                                            QsdiElementUpdateStrategy.valueOf(storageType));
//                                    if (qsdiVertex.getElementUpdateStrategy() == QsdiElementUpdateStrategy.RETAIN
//                                            || qsdiVertex.getElementUpdateStrategy()
//                                            == QsdiElementUpdateStrategy.OVERRIDE) {
//                                        Map<String, Object> properties = qsdiVertex.getProperties();
//                                        if (properties == null) {
//                                            properties = new HashMap<>();
//                                        }
//                                        for (String key : jsonObjectMap.keySet()) {
//                                            if (!properties.containsKey(key)) {
//                                                properties.put(key, null);
//                                            }
//                                        }
//                                        qsdiVertex.setProperties(properties);
//                                    }
//
//                                    Map<String, String> propertyUpdateStrategies =
//                                            (Map<String, String>) jsonObject.get("propertyUpdateStrategies");
//                                    if (CollectionUtil.isNotEmpty(propertyUpdateStrategies)) {
//                                        Map<String, QsdiUpdateStrategy> updateStrategyMap = new HashMap<>();
//                                        propertyUpdateStrategies.forEach(
//                                                (properyName, strategy) -> {
//                                                    updateStrategyMap.put(
//                                                            properyName, QsdiUpdateStrategy.valueOf(strategy));
//                                                });
//                                        qsdiVertex.setPropertyUpdateStrategies(updateStrategyMap);
//                                    }
//
//                                    return new Object[]{taskId, qsdiVertex};
//                                })
//                        .collect(
//                                Collectors.groupingBy(
//                                        o -> o[0] == null ? "" : o[0].toString(),
//                                        Collectors.mapping(o -> (QsdiVertex) o[1], Collectors.toList())));
//
//
//        map.forEach(
//                (k, v) -> {
//
//
//                    v.forEach(vertex -> {
//                        try {
//                            writer.write(JSON.toJSONString(vertex));
//                            writer.newLine();
//                        } catch (IOException e) {
//                            throw new RuntimeException(e);
//                        }
//                    });
//                    saveCount += v.size();
//
//                    LOGGER.info("save size {},and all is {}",v.size(),saveCount);
//
//
//                });
//    }
//
//
//    @Override
//    public void close() throws Exception {
//        writer.flush();
//        writer.close();
//        graphKafkaConsumer.close();
//    }
//}
