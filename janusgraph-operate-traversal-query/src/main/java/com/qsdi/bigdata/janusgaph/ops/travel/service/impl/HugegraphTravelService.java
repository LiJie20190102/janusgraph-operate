/*
 * Copyright (c) 2022. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
 * Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
 * Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
 * Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */
package com.qsdi.bigdata.janusgaph.ops.travel.service.impl;

import com.qsdi.bigdata.janusgaph.ops.travel.enums.GraphType;
import com.qsdi.bigdata.janusgaph.ops.travel.service.TravelService;
import com.qsdi.bigdata.multi.graph.api.struct.model.traverser.QsdiKneighborRequest;
import lombok.extern.slf4j.Slf4j;
import org.apache.hugegraph.driver.HugeClient;
import org.apache.hugegraph.driver.HugeClientBuilder;
import org.apache.hugegraph.structure.constant.Direction;
import org.apache.hugegraph.structure.graph.Path;
import org.apache.hugegraph.structure.traverser.Kneighbor;
import org.apache.hugegraph.structure.traverser.KneighborRequest;

import java.util.List;

/**
 * Description
 *
 * @author lijie0203 2024/2/1 21:49
 */
@Slf4j
public class HugegraphTravelService implements TravelService {


    private final HugeClient client;

    public HugegraphTravelService() {
        client =
//                new HugeClientBuilder("http://192.168.1.115:8066", "hugegraph")
                new HugeClientBuilder("http://192.168.1.110:8065", "hugegraph_test")
                        .configTimeout(10000000)
                        .build();
    }


    @Override
    public void kNeighbours(QsdiKneighborRequest kneighborRequest) {
        int maxDepth = kneighborRequest.getMaxDepth();

        KneighborRequest.Builder builder = KneighborRequest.builder().source(Long.parseLong(kneighborRequest.getSource().asObject().toString())).maxDepth(maxDepth).countOnly(false).withVertex(true).withPath(true).limit((int) kneighborRequest.getLimit());

        builder.step().degree(100000);
        KneighborRequest kneighborRequest1 = builder.build();
        Kneighbor kneighbor = client.traverser().kneighbor(kneighborRequest1);


//        log.info("{} maxDepth:{}, kNeighbours:{}", kneighborRequest.getSource(), maxDepth, kneighbor.ids());
    }

    @Override
    public void allShortestPath(String sourceId, String targetId, String maxDepth) {
        List<Path> paths = client.traverser().allShortestPaths(Long.parseLong(sourceId), Long.parseLong(targetId), Direction.BOTH, null, Integer.parseInt(maxDepth), 10000, 0,1000000);

//        log.info("source:{} target:{}, maxDepth{}, findShortestPath:{}", sourceId, targetId, maxDepth, paths);
    }


    @Override
    public GraphType getGraphType() {
        return GraphType.HUGE;
    }
}
