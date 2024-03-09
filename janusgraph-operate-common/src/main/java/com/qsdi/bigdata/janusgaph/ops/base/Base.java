/*
 * Copyright (c) 2022. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
 * Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
 * Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
 * Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */
package com.qsdi.bigdata.janusgaph.ops.base;

import com.qsdi.bigdata.graph.janusgraph.client.GraphClient;
import com.qsdi.bigdata.graph.janusgraph.client.GraphClientBuilder;
import com.qsdi.bigdata.janusgaph.ops.util.PropertiesUtil;


/**
 * Description
 *
 * @author lijie0203 2023/12/19 16:02
 */
public class Base {

    protected static GraphClient client;

    // 读文件行数的批次
    protected static final int BATCH_READ_SIZE = PropertiesUtil.getIntValue("batch.read.size",1000);

    //    入图的每个批次
    protected static final int BATCH_SAVE_SIZE = PropertiesUtil.getIntValue("batch.save_size",1000);

    protected static final int QUEUE_SIZE = PropertiesUtil.getIntValue("queue.size",10000);

    // 异常容错次数
    protected static final int ERROR_COUNT = PropertiesUtil.getIntValue("error.count",30);

    // 查询的线程数
    protected static final int C_PARALLELISM = PropertiesUtil.getIntValue("consumer_parallelism",50);

    protected static final String JANUS_SERVER_HOST = PropertiesUtil.getValue("janusgraph.server_host","192.168.1.140");

    protected static final int JANUS_SERVER_PORT = PropertiesUtil.getIntValue("janusgraph.server_port",9183);




    protected static final String GRAPH_NAME = "friendster";
//    protected static final String graphName = "qsdi_test_lj10";

    protected static final String VERTEX_LABEL = "person";

    protected static final String EDGE_LABEL = "friend";

    static {
        client = GraphClientBuilder.newBuilder()
                .serverHost(JANUS_SERVER_HOST)
//                .serverHost("localhost")
                .serverPort(JANUS_SERVER_PORT)
                .readTimeout(240)
//                .readTimeout(120)
                .build();
    }
}
