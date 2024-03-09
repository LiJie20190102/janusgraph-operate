/*
 * Copyright (c) 2022. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
 * Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
 * Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
 * Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */
package com.qsdi.bigdata.janusgaph.ops.travel.enums;

/**
 * Description
 *
 * @author lijie0203 2024/2/2 10:48
 */
public enum TaskType {

    MASSIVE_INSERTION("Massive Insertion", "MassiveInsertion"), SINGLE_INSERTION("Single Insertion", "SingleInsertion"), DELETION(
            "Delete Graph", "DeleteGraph"), FIND_NEIGHBOURS("Find Neighbours of All Nodes", "FindNeighbours"), FIND_ADJACENT_NODES(
            "Find Adjacent Nodes of All Edges", "FindAdjacent"), FIND_SHORTEST_PATH("Find Shortest Path", "FindShortest"), CLUSTERING(
            "Clustering", "Clustering");


    private final String longname;
    private final String filenamePrefix;



    private TaskType(String longName, String filenamePrefix)
    {
        this.longname = longName;
        this.filenamePrefix = filenamePrefix;
    }

}
