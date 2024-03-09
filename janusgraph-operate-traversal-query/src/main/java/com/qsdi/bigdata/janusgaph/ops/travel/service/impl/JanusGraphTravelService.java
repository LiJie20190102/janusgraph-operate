/*
 * Copyright (c) 2022. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
 * Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
 * Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
 * Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */
package com.qsdi.bigdata.janusgaph.ops.travel.service.impl;

import com.qsdi.bigdata.graph.janusgraph.entity.request.traversers.AdvancedKneighborRequest;
import com.qsdi.bigdata.graph.janusgraph.entity.request.traversers.CodeMapMemoryType;
import com.qsdi.bigdata.graph.janusgraph.entity.request.traversers.KeighborTraversalConfig;
import com.qsdi.bigdata.graph.janusgraph.entity.request.traversers.ShortestPathRequest;
import com.qsdi.bigdata.graph.janusgraph.entity.request.traversers.ShortestPathTraversalConfig;
import com.qsdi.bigdata.janusgaph.ops.base.Base;
import com.qsdi.bigdata.janusgaph.ops.travel.enums.GraphType;
import com.qsdi.bigdata.janusgaph.ops.travel.service.TravelService;
import com.qsdi.bigdata.multi.graph.api.struct.model.graph.QsdiPath;
import com.qsdi.bigdata.multi.graph.api.struct.model.traverser.QsdiEdgeStep;
import com.qsdi.bigdata.multi.graph.api.struct.model.traverser.QsdiKneighbor;
import com.qsdi.bigdata.multi.graph.api.struct.model.traverser.QsdiKneighborRequest;
import com.qsdi.bigdata.multi.graph.common.id.GraphID;
import com.qsdi.bigdata.multi.graph.common.id.IdGenerator;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * Description
 *
 * @author lijie0203 2024/2/1 21:49
 */
@Slf4j
public class JanusGraphTravelService extends Base implements TravelService {


    @Override
    public void kNeighbours(QsdiKneighborRequest kneighborRequest) {
        int maxDepth = kneighborRequest.getMaxDepth();


        QsdiEdgeStep edgeStep = QsdiEdgeStep.builder().degree(100000).build();

        kneighborRequest.setWithPath(true);
        kneighborRequest.setWithVertex(true);
        kneighborRequest.setCountOnly(false);
        kneighborRequest.setMaxDepth(maxDepth);
        kneighborRequest.setStep(edgeStep);

        KeighborTraversalConfig traversalConfig = KeighborTraversalConfig.builder().concurrentThreads(3).concurrentDepth(2).build();

        AdvancedKneighborRequest advancedKneighborRequest =
                AdvancedKneighborRequest.builder()
                        .qsdiKneighborRequest(kneighborRequest)
                        .traversalConfig(traversalConfig)
                        .build();

        QsdiKneighbor qsdiKneighbor = client.getTraverser().advancedKneighbours(GRAPH_NAME, advancedKneighborRequest);


//        log.info("{} maxDepth:{}, kNeighbours:{}", kneighborRequest.getSource(), maxDepth, qsdiKneighbor.getIds());
    }

    @Override
    public void allShortestPath(String sourceId, String targetId, String maxDepth) {
        GraphID sourceID = IdGenerator.of(sourceId);
        GraphID targetID = IdGenerator.of(targetId);

        ShortestPathTraversalConfig traversalConfig = ShortestPathTraversalConfig.builder().build();
        traversalConfig.setCodeMapMemoryType(CodeMapMemoryType.COMMON);
        ShortestPathRequest pathRequest =
                ShortestPathRequest.builder().traversalConfig(traversalConfig).sourceId(sourceID).targetId(targetID).degree(10000).skipDegree(0).capacity(1000000).maxDepth(Integer.parseInt(maxDepth)).build();

        List<QsdiPath> qsdiPaths = client.getTraverser().allShortestPaths(GRAPH_NAME, pathRequest);


//        log.info("source:{} target:{},maxDepth:{}, findShortestPath:{}", sourceId, targetId,maxDepth, qsdiPaths);
    }

    @Override
    public GraphType getGraphType() {
        return GraphType.JANUS;
    }
}
