/*
 * Copyright (c) 2022. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
 * Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
 * Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
 * Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */
package com.qsdi.bigdata.janusgaph.ops.domain;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import com.qsdi.bigdata.graph.janusgraph.entity.request.graph.AddEdgesRequest;
import com.qsdi.bigdata.graph.janusgraph.entity.request.graph.AddVerteicesRequest;
import com.qsdi.bigdata.janusgaph.ops.base.Base;
import com.qsdi.bigdata.janusgaph.ops.util.LineIterator;
import com.qsdi.bigdata.janusgaph.ops.util.PropertiesUtil;
import com.qsdi.bigdata.multi.graph.api.struct.model.graph.QsdiEdge;
import com.qsdi.bigdata.multi.graph.api.struct.model.graph.QsdiVertex;
import com.qsdi.bigdata.multi.graph.api.struct.model.graph.QsdiVertexForEdge;
import com.qsdi.bigdata.multi.graph.common.id.IdGenerator;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Description
 *
 * @author lijie0203 2023/12/19 15:54
 */
@Slf4j
public class InsertData extends Base {

    private static LinkedBlockingDeque<List<String>> vBlockingDeque = Queues.newLinkedBlockingDeque(QUEUE_SIZE);
    private static LinkedBlockingDeque<List<String>> eBlockingDeque = Queues.newLinkedBlockingDeque(QUEUE_SIZE);


    private static AtomicInteger saveCount = new AtomicInteger(0);

    private static final String V_FILE_NAME;

    private static final String E_FILE_DIRECTORY;

    static {

        if (isWinEnv()) {
            V_FILE_NAME = "src/main/data/com-friendster.vertex.txt";
            E_FILE_DIRECTORY = "src/main/data/splitdata";
        } else {
//            /usr/lib/hugegraph/apache-hugegraph-toolchain-incubating-1.0.0/apache-hugegraph-loader-incubating-1.0.0/data
//            Nodes: 65608366 Edges: 1806067135
            V_FILE_NAME = PropertiesUtil.getValue("v.data.file_name", "/usr/graph_data_test/com-friendster.vertex.txt");
            E_FILE_DIRECTORY = PropertiesUtil.getValue("e.data.file_directory", "/usr/graph_data_test/convert");

        }

    }


    public static void main(String[] args) {

//        int cParallelism = 15;
        ExecutorService cPool = Executors.newWorkStealingPool(C_PARALLELISM + 1);
//        LinkedBlockingDeque<List<String>> blockingDeque = Queues.newLinkedBlockingDeque(QUEUE_SIZE);


        try {
//            // v
            log.info("begin to inster v data");
            insertData(C_PARALLELISM, vBlockingDeque, cPool, "vertex");

//            // wait
            while (!vBlockingDeque.isEmpty()) ;

            log.info("end to inster v data");


            // e
            log.info("begin to inster e data");
            saveCount.set(0);
            insertData(C_PARALLELISM, eBlockingDeque, cPool, "edge");
            while (!vBlockingDeque.isEmpty()) ;
            log.info("end to inster e data");
        } finally {
            cPool.shutdownNow();
        }
    }

    private static void insertData(int cParallelism, LinkedBlockingDeque<List<String>> blockingDeque, ExecutorService cPool, String elementType) {
        CountDownLatch countDownLatch = new CountDownLatch(2 + cParallelism);
//        AtomicInteger producer = new AtomicInteger(pParallelism);
        AtomicBoolean error = new AtomicBoolean(false);

        if ("vertex".equals(elementType)) {
            producer(blockingDeque, cPool, error, countDownLatch);
            // 先预放1000
            while (blockingDeque.size() <= 1000) ;
            // 开始计时
            Stopwatch stopwatch = Stopwatch.createStarted();
            doInsert(cParallelism, blockingDeque, cPool, elementType, error, countDownLatch, stopwatch);
        } else {
            File file = new File(E_FILE_DIRECTORY);
            assert file.isDirectory();
            List<String> fileNames = Arrays.stream(Objects.requireNonNull(file.list())).map(x -> E_FILE_DIRECTORY + "/" + x).collect(Collectors.toList());

            CompletableFuture.runAsync(() -> {
                for (String fileName : fileNames) {
                    try (LineIterator<List<String>> lineIterator = new LineIterator<>(fileName, BATCH_READ_SIZE)) {
                        productData(blockingDeque, lineIterator);
                    } catch (FileNotFoundException e) {
//                        throw new RuntimeException(e);
                        log.warn("fileName {} not found", fileName);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }

            }, cPool).whenComplete((r, e) -> {
                if (e != null) {
                    error.compareAndSet(false, true);
                    log.error("producer error:", e);
                }
                countDownLatch.countDown();
            });


            // 先预放1000
            while (blockingDeque.size() <= 1000) ;

            // 开始计时
            Stopwatch stopwatch = Stopwatch.createStarted();
            doInsert(cParallelism, blockingDeque, cPool, elementType, error, countDownLatch, stopwatch);

        }
    }

    private static void producer(LinkedBlockingDeque<List<String>> blockingDeque, ExecutorService cPool, AtomicBoolean error, CountDownLatch countDownLatch) {
        CompletableFuture.runAsync(() -> {
            try (LineIterator<List<String>> lineIterator = new LineIterator<>(V_FILE_NAME, BATCH_READ_SIZE)) {
                productData(blockingDeque, lineIterator);
            } catch (Exception e) {
                throw new RuntimeException("producer v error:", e);
            }
        }, cPool).whenComplete((r, e) -> {
            if (e != null) {
                error.compareAndSet(false, true);
                log.error("producer error:", e);
            }
            countDownLatch.countDown();
        });
    }

    private static void doInsert(int cParallelism, LinkedBlockingDeque<List<String>> blockingDeque, ExecutorService cPool, String elementType, AtomicBoolean error, CountDownLatch countDownLatch, Stopwatch stopwatch) {
        try {

            // 开始统计性能
            computeRat(blockingDeque, cPool, elementType, stopwatch, error, countDownLatch);

            // consumer
            consumer(cParallelism, blockingDeque, cPool, elementType, stopwatch, error, countDownLatch);

            for (; ; ) {
                try {
                    boolean finish = countDownLatch.await(1, TimeUnit.SECONDS);
                    if (finish || error.get()) {
                        break;
                    }
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }
            log.info(elementType + " end, cost all " + stopwatch.stop().elapsed(TimeUnit.NANOSECONDS));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

//    private static void producer(LinkedBlockingDeque<List<String>> blockingDeque, ExecutorService cPool, LineIterator<List<String>> lineIterator, AtomicBoolean error, CountDownLatch countDownLatch) {
//        CompletableFuture.runAsync(() -> {
//            productData(blockingDeque, lineIterator);
//        }, cPool).whenComplete((r, e) -> {
//            if (e != null) {
//                error.compareAndSet(false, true);
//                log.error("producer error:", e);
//            }
//            countDownLatch.countDown();
//        });
//    }

    private static void consumer(int cParallelism, LinkedBlockingDeque<List<String>> blockingDeque, ExecutorService cPool, String elementType, Stopwatch stopwatch, AtomicBoolean error, CountDownLatch countDownLatch) {
        for (int i = 0; i < cParallelism; i++) {

            CompletableFuture.runAsync(() -> {
                //
                consumerData(blockingDeque, elementType, stopwatch);
            }, cPool).whenComplete((r, e) -> {
                if (e != null) {
                    error.compareAndSet(false, true);
                    log.error("comsumer error:", e);
                }
                countDownLatch.countDown();

            });
        }
    }

    private static void computeRat(LinkedBlockingDeque<List<String>> blockingDeque, ExecutorService cPool, String elementType, Stopwatch stopwatch, AtomicBoolean error, CountDownLatch countDownLatch) {
        CompletableFuture.runAsync(() -> {
            int oldCount = saveCount.get();
            while (!blockingDeque.isEmpty()) {
                int diffCount = saveCount.get() - oldCount;
                if (diffCount == 0) {
                    continue;
                }
                double rate = ((double) diffCount) / 60;
                double sumRate = ((double) saveCount.get()) / (stopwatch.elapsed(TimeUnit.SECONDS) + 1);
                log.info("{}===================rate is {}, and sumRate is {}", elementType, rate, sumRate);
                oldCount = saveCount.get();
                try {
                    TimeUnit.MINUTES.sleep(1);
                } catch (Exception e) {
                    log.error("InterruptedException:", e);
                    throw new RuntimeException(e);
                }
            }

        }, cPool).whenComplete((r, e) -> {
            if (e != null) {
                error.compareAndSet(false, true);
                log.error("rate error:", e);
            }
            countDownLatch.countDown();
        });
    }

    private static void productData(LinkedBlockingDeque<List<String>> blockingDeque, LineIterator<List<String>> lineIterator) {
        while (lineIterator.hasNext()) {
            List<String> next = lineIterator.next();
            // 单线程
            // todo 是否有阻塞队列
            while (!blockingDeque.offerLast(next)) {

            }
        }
    }

    private static void consumerData(LinkedBlockingDeque<List<String>> blockingDeque, String elementType, Stopwatch stopwatch) {
        boolean noData = false;
        boolean error = false;
        List<String> elements = null;
        for (int count = 0; ; count++) {
            try {
                if (!error) {
                    elements = blockingDeque.poll(10, TimeUnit.MILLISECONDS);
                }

                if (elements == null) {
                    // 10S没数据，认为此线程结束
                    if (noData && count >= 1000) {
                        break;
                    }

                    if (noData) {
                        continue;
                    }
                    noData = true;
                    count = 0;
                } else {
                    // 入图
                    int saveSize = saveData2graph(elements, elementType);
//                    if (elements.size() != saveSize) {
//                        log.warn(String.format("elements is %s, and save size is %s", elements.size(), saveSize));
//                    }
                    log.info(String.format("%s ---save [%s] end ,all save is [%s], all save =[%s]= now is all cost [%s]=====queue size is %s", Thread.currentThread().getName(), saveSize, saveCount.addAndGet(saveSize), elementType, stopwatch.elapsed(TimeUnit.NANOSECONDS), blockingDeque.size()));

                    if (noData) {
                        noData = false;
                    }
                    if (error) {
                        error = false;
                    }
                }
            } catch (Exception e) {
                log.error("get comsumer data error:", e);

                if (StringUtils.isNotEmpty(e.getMessage()) && e.getMessage().contains("write overflow")) {
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException ex) {
                        log.error(Thread.currentThread().getName() + ":InterruptedException:", e);
                    }
                    error = true;
                    continue;
                }

                if (error && count > ERROR_COUNT) {
                    log.error(Thread.currentThread().getName() + ":error count is {}", ERROR_COUNT);
                    throw new RuntimeException(e);
                }

                if (error) {
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException ex) {
                        log.error(Thread.currentThread().getName() + ":InterruptedException:", e);
                    }
                    continue;
                }
                error = true;
                count = 0;
            }
        }
    }

    public static Boolean isWinEnv() {
        String os = System.getProperty("os.name");
        return os != null && os.toLowerCase().startsWith("windows");
    }

    private static int saveData2graph(List<String> elements, String elementType) throws InterruptedException {

        List<List<String>> listList = Lists.partition(elements, BATCH_SAVE_SIZE);
//        TxConfig txConfig = TxConfig.builder().useQsdiIndexLogic(false).useQsdiIndexLogLogic(false).build();

        for (List<String> list : listList) {
            if ("vertex".equals(elementType)) {
                List<QsdiVertex> vertexList = list.stream().map(element -> {
                    QsdiVertex person = new QsdiVertex(VERTEX_LABEL);
                    person.property("id", element);
                    person.setId(IdGenerator.of(element));
                    return person;
                }).collect(Collectors.toList());

                AddVerteicesRequest addVerteicesRequest = AddVerteicesRequest.builder().vertices(vertexList).build();

                List<QsdiVertex> qsdiVertices = client.getGraph().addVertices(GRAPH_NAME, addVerteicesRequest);
                return qsdiVertices.size();
            } else if (("edge".equals(elementType))) {
                List<QsdiEdge> edgeList = list.stream().filter(x -> !x.startsWith("#") && StringUtils.isNotBlank(x)).map(element -> {
                    String[] split = element.split(",");

                    QsdiEdge edge = new QsdiEdge(EDGE_LABEL);
                    QsdiVertexForEdge sourceV = new QsdiVertexForEdge();
                    sourceV.setVertexId(IdGenerator.of(split[0]));
                    QsdiVertexForEdge targetV = new QsdiVertexForEdge();
                    targetV.setVertexId(IdGenerator.of(split[1]));

                    edge.setSourceV(sourceV);
                    edge.setTargetV(targetV);
                    return edge;
                }).collect(Collectors.toList());

                if (edgeList.isEmpty()) {
                    return 0;
                }

                AddEdgesRequest edgesRequest = AddEdgesRequest.builder().edges(edgeList).build();
                List<QsdiEdge> qsdiEdges = client.getGraph().addEdges(GRAPH_NAME, edgesRequest);
                return qsdiEdges.size();

            }
        }
        return 0;
    }


}
