/*
 * Copyright (c) 2022. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
 * Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
 * Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
 * Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */
package com.qsdi.bigdata.janusgaph.ops.domain;

import com.qsdi.bigdata.graph.janusgraph.driver.GraphsManager;
import com.qsdi.bigdata.graph.janusgraph.entity.request.profile.CreateGraph;
import com.qsdi.bigdata.graph.janusgraph.entity.request.schema.BuildIndexRequest;
import com.qsdi.bigdata.janusgaph.ops.base.Base;
import com.qsdi.bigdata.multi.graph.api.struct.enums.QsdiCardinality;
import com.qsdi.bigdata.multi.graph.api.struct.enums.QsdiDataType;
import com.qsdi.bigdata.multi.graph.api.struct.enums.QsdiFrequency;
import com.qsdi.bigdata.multi.graph.api.struct.model.schema.QsdiEdgeLabel;
import com.qsdi.bigdata.multi.graph.api.struct.model.schema.QsdiPropertyKey;
import com.qsdi.bigdata.multi.graph.api.struct.model.schema.QsdiVertexLabel;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Description
 *
 * @author lijie0203 2023/12/19 16:01
 */
@Slf4j
public class CreateSchema extends Base {
    public static void main(String[] args) throws InterruptedException {
        List<QsdiPropertyKey> propertyKeys = client.getSchema().getPropertyKeys(GRAPH_NAME);
        System.out.println("propertyKeys=="+propertyKeys);
        List<QsdiVertexLabel> vertexLabels = client.getSchema().getVertexLabels(GRAPH_NAME);
        log.error("vertexLabels=="+vertexLabels);
        List<QsdiEdgeLabel> edgeLabels = client.getSchema().getEdgeLabels(GRAPH_NAME);
        log.error("aaaaadsd121=="+edgeLabels);

    }

    @Test
    public void truncateGraphSchema() {
        dropGraph();
        createGraph();

        //===================================

       addProperty();

        // v
        createVertexLabel();

        buildVertexIndex();

        // e
        createEdgeLabel();

        buildEdgeIndex();

        System.out.println("create schema ok===");
    }

    @Test
    public void dropGraph(){
        GraphsManager graphs = client.getGraphs();
        graphs.dropGraph(GRAPH_NAME);
        System.out.println("drop graph " + GRAPH_NAME + " successfully");
    }

    @Test
    public void createGraph(){
        GraphsManager graphs = client.getGraphs();
        CreateGraph createGraph = new CreateGraph();
        createGraph.setGraphName(GRAPH_NAME);
        Map<String, String> preference = new HashMap<>();
        preference.put("index.[x].elasticsearch.create.ext.number_of_shards", "9");
        preference.put("metrics.prefix", "org.janusgraph.[x]");
        preference.put("metrics.console.interval", "");
        preference.put("cache.db-cache", "false");
        preference.put("index.[x].hostname", "hadoop02,hadoop01,hadoop03");


//        preference.put("storage.hbase.ext.hbase.hconnection.threads.max", "512");


        createGraph.setPreference(preference);
        graphs.createGraph(createGraph);
        System.out.println("create graph " + graphs + " successfully");
    }


    @Test
    public void addProperty(){



        QsdiPropertyKey countPropertyKey =
                QsdiPropertyKey.builder()
                        .name("id")
                        .dataType(QsdiDataType.INT)
                        .nullable(true)
                        .cardinality(QsdiCardinality.SINGLE)
                        .build();


        QsdiPropertyKey propertyKey = client.getSchema().addPropertyKey(GRAPH_NAME, countPropertyKey);
        System.out.println("propertyKey=="+propertyKey);
    }

    @Test
    public void createVertexLabel(){

        Set<QsdiPropertyKey> addPeopleProperties = new HashSet<>();

        QsdiPropertyKey id = client.getSchema().getPropertyKey(GRAPH_NAME, "id");
        addPeopleProperties.add(id);

        QsdiVertexLabel addPeopleVertexLabel =
                QsdiVertexLabel.builder()
                        .checkExist(true)
                        .name(VERTEX_LABEL)
                        .properties(addPeopleProperties)
                        .build();


        QsdiVertexLabel qsdiVertexLabel = client.getSchema().addVertexLabel(GRAPH_NAME, addPeopleVertexLabel);
        System.out.println("vertexLabel=="+qsdiVertexLabel);
    }

    @Test
    public void buildVertexIndex() {
        Map<String, Object> indexConf = new HashMap<>();
        indexConf.put("id", "string");
        BuildIndexRequest buildIndexRequest =
                BuildIndexRequest.builder()
                        .labelName(VERTEX_LABEL)
                        .indexFieldType(indexConf)
                        .build();
        String s = this.client.getIndexManager().buildVertexIndex(GRAPH_NAME, buildIndexRequest);
        System.out.println("vertex index name " + s);
    }

    @Test
    public void createEdgeLabel(){


        QsdiEdgeLabel demoQsdiEdgeLabel =
                QsdiEdgeLabel.builder()
                        .frequency(QsdiFrequency.SINGLE)
                        .sourceLabel(VERTEX_LABEL)
                        .targetLabel(VERTEX_LABEL)
                        .name(EDGE_LABEL)
                        .build();

        QsdiEdgeLabel qsdiEdgeLabel = client.getSchema().addEdgeLabel(GRAPH_NAME, demoQsdiEdgeLabel);
        System.out.println(qsdiEdgeLabel);
    }


    @Test
    public void buildEdgeIndex() {
        Map<String, Object> indexConf = new HashMap<>();
        BuildIndexRequest buildIndexRequest =
                BuildIndexRequest.builder().labelName(EDGE_LABEL).indexFieldType(indexConf).build();
        String s = this.client.getIndexManager().buildEdgeIndex(GRAPH_NAME, buildIndexRequest);
        System.out.println("edge index name " + s);
    }












}
