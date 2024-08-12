/*
 * Copyright (c) 2022. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
 * Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
 * Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
 * Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */
package com.qsdi.bigdata.janusgaph.ops.manager;

import com.qsdi.bigdata.graph.janusgraph.client.GraphClientBuilder;

/**
 * Description
 *
 * @author lijie0203 2023/12/19 15:52
 */
public class SchemaManager {
    public static void main(String[] args) {
        GraphClientBuilder.newBuilder()
                .serverHost("127.0.0.1")
                .serverPort(9183)
                .readTimeout(60)
                //            .setRpcMethodConfigs(rpcMethodConfigs)
                .build();
    }

}
