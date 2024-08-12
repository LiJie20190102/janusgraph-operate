package com.qsdi.bigdata.graph.gstore.performance.test.job.conf;

import com.google.common.base.Preconditions;
import com.qsdi.bigdata.graph.gstore.performance.test.job.util.PropertiesUtil;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;

import java.util.Properties;

/**
 * Description
 *
 * @author lijie0203 2024/7/15 18:06
 */
@Data
public class BaseConf {

    private String graphName=PropertiesUtil.getValue("graph.name");

    private String gstoreUrl=PropertiesUtil.getValue("graph.gstore.connect.url");
    private int gstoreConnectTimeOut=PropertiesUtil.getIntValue("graph.gstore.connect.timeout",20);

    private String insertGraphType=PropertiesUtil.getValue("insert.graph.type","insert");


    private String dataSource=PropertiesUtil.getValue("data.source","relation-center");


    private Boolean vertexEnable=PropertiesUtil.getBooleanValue("vertex.enable",true);
    private Boolean edgeEnable=PropertiesUtil.getBooleanValue("edge.enable",true);

    private String vertexDataFile=PropertiesUtil.getValue("vertex.data.file");

    private String edgeDataFileDir=PropertiesUtil.getValue("edge.data.file_directory");


    private  String vertexLabel = PropertiesUtil.getValue("vertex.label","person");;

    private  String edgeLabel = PropertiesUtil.getValue("edge.label","friend");


    private int queuePrepareSize=PropertiesUtil.getIntValue("queue.prepare.size",1000);
    private int vertexInsertQueue=PropertiesUtil.getIntValue("vertex.insert.block.queue.size",10000);
    private int edgeInsertQueue=PropertiesUtil.getIntValue("edge.insert.block.queue.size",10000);


    private int consumerParallelism=PropertiesUtil.getIntValue("consumer_parallelism",1);
    private int batchReadFileSize=PropertiesUtil.getIntValue("batch.read-file-size",1000);
    private int batchSaveGraphSize=PropertiesUtil.getIntValue("batch.save-graph-size",1000);
    private int errCount=PropertiesUtil.getIntValue("error.count",30);

    private final String metricCsvDir = PropertiesUtil.getValue("metric.csv.dir",System.getenv("user.home"));
    private final int metricPeriodSecond = PropertiesUtil.getIntValue("metric.period.second",5);


    // ========================kafka 相关配置 begin======================================


    private Boolean kafkaEnableAutoCommit=PropertiesUtil.getBooleanValue("kafka.enable-auto-commit",false);

    // 模型结果发送的topic
    private String relationTopic=PropertiesUtil.getValue("kafka.relation-topic");


    private String kafkaGroupId=PropertiesUtil.getValue("kafka.group-id");

    private String kafkaAutoOffsetReset=PropertiesUtil.getValue("kafka.auto-offset-reset","earliest");

    private String kafkaBootstrapServers=PropertiesUtil.getValue("kafka.bootstrap-servers");

    private int kafkaMaxPollRecords=PropertiesUtil.getIntValue("kafka.max-poll-records",50);

    private int kafkaMaxPollInterval=PropertiesUtil.getIntValue("kafka.max-poll-interval",30000);

    private int kafkaSessionTimeoutMs=PropertiesUtil.getIntValue("kafka.session-timeout-ms",90000);

    private int kafkaMaxPartitionFetchBytes=PropertiesUtil.getIntValue("kafka.max-partition-fetch-bytes",104857600);

    private int kafkaHeartbeatIntervalMs=PropertiesUtil.getIntValue("kafka.heartbeat-interval-ms",3000);

    private int kafkaFetchMinBytes=PropertiesUtil.getIntValue("kafka.fetch-min-bytes",1);

    private String keyDeserializer=PropertiesUtil.getValue("kafka.key-deserializer","org.apache.kafka.common.serialization.ByteArrayDeserializer");
    private String valueDeserializer=PropertiesUtil.getValue("kafka.value-deserializer","org.apache.kafka.common.serialization.ByteArrayDeserializer");


    // ========================kafka 相关配置 end======================================


    public BaseConf() {
        Preconditions.checkArgument(StringUtils.isNoneEmpty(gstoreUrl),"gstoreUrl不能为空");
        Preconditions.checkArgument(StringUtils.isNoneEmpty(insertGraphType),"insertGraphType不能为空");
    }

    public Properties getKafkaProperties() {
        Properties properties = new Properties();
        properties.put("group.id", kafkaGroupId);
        properties.put("bootstrap.servers", kafkaBootstrapServers);
        properties.put("key.deserializer", this.getKeyDeserializer());
        properties.put("value.deserializer", this.getValueDeserializer());
        properties.put("auto.offset.reset", kafkaAutoOffsetReset);
        properties.put("enable.auto.commit", kafkaEnableAutoCommit);
        properties.put("max.poll.interval.ms", kafkaMaxPollInterval);
        properties.put("max.poll.records", kafkaMaxPollRecords);
        properties.put("session.timeout.ms", kafkaSessionTimeoutMs);
        properties.put("max.partition.fetch.bytes", kafkaMaxPartitionFetchBytes);
        properties.put("heartbeat.interval.ms", kafkaHeartbeatIntervalMs);
        properties.put("fetch.min.bytes", kafkaFetchMinBytes);
        return properties;

    }
}
