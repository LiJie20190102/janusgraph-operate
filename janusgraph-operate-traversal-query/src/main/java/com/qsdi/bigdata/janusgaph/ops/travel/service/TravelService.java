/*
 * Copyright (c) 2022. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
 * Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
 * Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
 * Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */
package com.qsdi.bigdata.janusgaph.ops.travel.service;


import com.qsdi.bigdata.janusgaph.ops.travel.enums.GraphType;
import com.qsdi.bigdata.multi.graph.api.struct.model.traverser.QsdiKneighborRequest;

/**
 * Description
 *
 * @author lijie0203 2024/2/1 21:39
 */
public interface TravelService {
    void kNeighbours(QsdiKneighborRequest kneighborRequest);

    void allShortestPath(String sourceId, String targetId, String maxDepth);

    GraphType getGraphType();

}
