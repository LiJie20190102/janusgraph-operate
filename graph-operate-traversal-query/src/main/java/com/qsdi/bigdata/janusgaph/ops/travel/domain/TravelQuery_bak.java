/*
 * Copyright (c) 2022. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
 * Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
 * Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
 * Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */
package com.qsdi.bigdata.janusgaph.ops.travel.domain;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Queues;
import com.qsdi.bigdata.janusgaph.ops.travel.enums.GraphType;
import com.qsdi.bigdata.janusgaph.ops.travel.enums.TaskType;
import com.qsdi.bigdata.janusgaph.ops.travel.service.TravelService;
import com.qsdi.bigdata.janusgaph.ops.travel.service.impl.JanusGraphTravelService;
import com.qsdi.bigdata.janusgaph.ops.util.LineIterator;
import com.qsdi.bigdata.multi.graph.api.struct.model.traverser.QsdiKneighborRequest;
import com.qsdi.bigdata.multi.graph.common.id.IdGenerator;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Description
 *
 * @author lijie0203 2024/2/1 21:40
 */
@Slf4j
public class TravelQuery_bak {

    protected static final String graphName = "friendster";

    // 读文件行数的批次
    private static final int BATCH_READ_SIZE = 10;

    private static  String V_READ_FILE_NAME =
            "/usr/graph_data_test/query_data/query_vertex.txt";

    // D:\bigdata\janusgraph-operate\travel-query\src\main\data\com-friendster.vertex.txt

    private static final String E_READ_FILE_NAME =
            "/usr/graph_data_test/query_data/edge";


    private static final long V_QUERY_COUNT = 5000L;

    private static final int QUEUE_SIZE = 10000;

    // 异常容错次数
    private static final int ERROR_COUNT = 1;

    // 查询的线程数
//  private static final int C_PARALLELISM = 800;
    private static final int C_PARALLELISM = 1;

    private static AtomicLong queryCurrentCount = new AtomicLong(0);

    private static AtomicLong errorCount = new AtomicLong(0);

    private static ArrayBlockingQueue<List<String>> blockingQueue = Queues.newArrayBlockingQueue(QUEUE_SIZE);


    private static ExecutorService cPool = Executors.newWorkStealingPool(C_PARALLELISM + 2);

    public static Boolean isWinEnv() {
        String os = System.getProperty("os.name");
        return os != null && os.toLowerCase().startsWith("windows");
    }


    public static void main(String[] args) {

        if (isWinEnv()) {
            V_READ_FILE_NAME = "D:\\bigdata\\janusgraph-operate\\travel-query\\src\\main\\data\\com-friendster.vertex.txt";
        } else {
//            /usr/lib/hugegraph/apache-hugegraph-toolchain-incubating-1.0.0/apache-hugegraph-loader-incubating-1.0.0/data
//            Nodes: 65608366 Edges: 1806067135
            V_READ_FILE_NAME = "/usr/graph_data_test/com-friendster.vertex.txt";
        }



        log.info("init janus client");
        TravelService janusGraphTravelService = new JanusGraphTravelService();

        log.info("janus==begin");
//        kneighborQuery(janusGraphTravelService, GraphType.JANUS);
        allShortestPathQuery(janusGraphTravelService, GraphType.JANUS);

        log.info("==janus==end");




//        log.info("init huge client");
//        HugegraphTravelService hugegraphTravelService = new HugegraphTravelService();
////        kneighborQuery(hugegraphTravelService, GraphType.HUGE);
//        allShortestPathQuery(hugegraphTravelService, GraphType.HUGE);
//
//        log.info("huge==end");


    }

    private static void allShortestPathQuery(TravelService janusGraphTravelService, GraphType graphType) {
        Map<String, String> queryParam = new HashMap<>();
//        queryParam.put("maxDepth", "1");
        queryParam.put("limit", "30");
//        log.info("================================allShortestPathQuery {}1 start", graphType);
//        travelQuery(janusGraphTravelService, TaskType.FIND_SHORTEST_PATH, queryParam);
//        while (!blockingQueue.isEmpty()) ;
//
//        log.info("================================allShortestPathQuery {}1 end", graphType);


//        log.info("================================allShortestPathQuery {}2 start", graphType);
//
//
//        queryParam.put("maxDepth", "2");
//        travelQuery(janusGraphTravelService, TaskType.FIND_SHORTEST_PATH, queryParam);
//        while (!blockingQueue.isEmpty()) ;
//
//        log.info("================================allShortestPathQuery {}2 end", graphType);

        log.info("================================allShortestPathQuery {}3 start", graphType);


        queryParam.put("maxDepth", "3");
        travelQuery(janusGraphTravelService, TaskType.FIND_SHORTEST_PATH, queryParam);
        while (!blockingQueue.isEmpty()) ;
        log.info("================================allShortestPathQuery {}3 end", graphType);

    }

    private static void kneighborQuery(TravelService travelService, GraphType graphType) {
        log.info("================================kneighborQuery {}1 start", graphType);
        Map<String, String> queryParam = new HashMap<>();
        queryParam.put("maxDepth", "1");
        queryParam.put("limit", "30");
        travelQuery(travelService, TaskType.FIND_NEIGHBOURS, queryParam);
        while (!blockingQueue.isEmpty()) ;

        log.info("================================kneighborQuery {}1 end", graphType);


        log.info("================================kneighborQuery {}2 start", graphType);

        queryParam.put("maxDepth", "2");
        travelQuery(travelService, TaskType.FIND_NEIGHBOURS, queryParam);
        while (!blockingQueue.isEmpty()) ;

        log.info("================================kneighborQuery {}2 end", graphType);


        log.info("================================kneighborQuery {}3 start", graphType);

        queryParam.put("maxDepth", "3");
        travelQuery(travelService, TaskType.FIND_NEIGHBOURS, queryParam);
        while (!blockingQueue.isEmpty()) ;
        log.info("================================kneighborQuery {}3 end", graphType);
    }

    private static void edgeAdjacentNodesQuery(TravelService service, Map<String, String> queryParam) {
        CountDownLatch countDownLatch = new CountDownLatch(2 + C_PARALLELISM);


        // 准备数据
        CompletableFuture.runAsync(
                        () -> {
                            getDataFromFile(TaskType.FIND_ADJACENT_NODES);
                        },
                        cPool)
                .whenComplete(
                        (r, e) -> {
                            if (e != null) {
                                log.error("producer error:", e);
                            }
                            countDownLatch.countDown();
                        });

        // 先预放1000
        while (blockingQueue.size() <= 1000) ;

        // 开始计时
        Stopwatch stopwatch = Stopwatch.createStarted();

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        // 开始统计性能
        computeRat(cPool, TaskType.FIND_ADJACENT_NODES, stopwatch, countDownLatch);


        queryData(service, queryParam, TaskType.FIND_ADJACENT_NODES, stopwatch, countDownLatch);
    }

    private static void findShortestPathQuery(TravelService service, Map<String, String> queryParam) {
        CountDownLatch countDownLatch = new CountDownLatch(2 + C_PARALLELISM);


        // 准备数据
        CompletableFuture.runAsync(
                        () -> {
                            getDataFromFile(TaskType.FIND_SHORTEST_PATH);
                        },
                        cPool)
                .whenComplete(
                        (r, e) -> {
                            if (e != null) {
                                log.error("producer error:", e);
                            }
                            countDownLatch.countDown();
                        });

        // 先预放1000
        while (blockingQueue.size() <= 1000) ;

        // 开始计时
        Stopwatch stopwatch = Stopwatch.createStarted();

        // 开始统计性能
        computeRat(cPool, TaskType.FIND_SHORTEST_PATH, stopwatch, countDownLatch);


        queryData(service, queryParam, TaskType.FIND_SHORTEST_PATH, stopwatch, countDownLatch);

    }

    private static void travelQuery(TravelService janusGraphTravelService, TaskType taskType, Map<String, String> queryParam) {
        CountDownLatch countDownLatch = new CountDownLatch(2 + C_PARALLELISM);


        // 准备数据
        CompletableFuture.runAsync(
                        () -> {
                            getDataFromFile(taskType);
                        },
                        cPool)
                .whenComplete(
                        (r, e) -> {
                            if (e != null) {
                                log.error("producer error:", e);
                            }
                            countDownLatch.countDown();
                        });

        // 先预放1000
//        while (blockingQueue.size() <= 100) ;

        // 开始计时
        Stopwatch stopwatch = Stopwatch.createStarted();

        // 开始统计性能
        computeRat(cPool, taskType, stopwatch, countDownLatch);

        queryData(janusGraphTravelService, queryParam, taskType, stopwatch, countDownLatch);

        long elapsed = stopwatch.elapsed(TimeUnit.MILLISECONDS);
        String s = queryParam.get("maxDepth");
        log.info("query end ,cost {},maxDepth is {}", elapsed, s);
    }


    private static void queryData(TravelService service, Map<String, String> queryParam, TaskType taskType, Stopwatch stopwatch,
                                  CountDownLatch countDownLatch) {
        for (int i = 0; i < C_PARALLELISM; i++) {
            int finalI = i;
            CompletableFuture.runAsync(
                            () -> {
                                log.info("aaa" + finalI);
                                consumerData(service, queryParam, taskType, stopwatch);
                            },
                            cPool)
                    .whenComplete(
                            (r, e) -> {
                                if (e != null) {
                                    //                error.compareAndSet(false, true);
                                    log.error("{} doQuery error:", taskType, e);
                                }
                                countDownLatch.countDown();
                            });
        }

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            log.error("InterruptedException:", e);
        }
    }

    private static void consumerData(TravelService service, Map<String, String> queryParam, TaskType taskType, Stopwatch stopwatch) {
        boolean noData = false;
        boolean error = false;
        List<String> elements = null;
        for (int count = 0; ; count++) {
            try {
                if (!error) {
                    elements = blockingQueue.poll(10, TimeUnit.MILLISECONDS);
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
                    query(service, queryParam, taskType, elements);
                    log.info("{} query {} cost {},all query {},queue is {}", taskType, elements.size(), stopwatch.elapsed(TimeUnit.MILLISECONDS), queryCurrentCount.addAndGet(elements.size()), blockingQueue.size());

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

    private static void query(TravelService service, Map<String, String> queryParam, TaskType taskType, List<String> elements) {
        String maxDepth = queryParam.get("maxDepth");
        String limit = queryParam.get("limit");
        String sourceElement = null;
        for (String element : elements) {
            if (TaskType.FIND_NEIGHBOURS == taskType) {
                QsdiKneighborRequest kneighborRequest =
                        QsdiKneighborRequest.builder().source(IdGenerator.of(element)).maxDepth(Integer.parseInt(maxDepth)).limit(Integer.parseInt(limit)).build();

                service.kNeighbours(kneighborRequest);
            } else if (TaskType.FIND_SHORTEST_PATH == taskType) {
                if (StringUtils.isEmpty(sourceElement)) {
                    sourceElement = element;
                    continue;
                }
                service.allShortestPath(sourceElement, element, maxDepth);
                sourceElement = element;
            }
        }
    }

    private static void getDataFromFile(TaskType taskType) {

        try (LineIterator<List<String>> lineIterator =
                     new LineIterator<>(V_READ_FILE_NAME, BATCH_READ_SIZE)) {
            productData(lineIterator);
        } catch (Exception e) {
            throw new RuntimeException("producer v error:", e);
        }
    }

    private static void productData(LineIterator<List<String>> lineIterator) {
        while (lineIterator.hasNext() && blockingQueue.size() < V_QUERY_COUNT) {
            List<String> next = lineIterator.next();
            boolean insertSuccess;
            do {

                try {
                    blockingQueue.put(next);
                    insertSuccess = true;
                } catch (InterruptedException e) {
                    insertSuccess = false;
                }

            } while (!insertSuccess);

        }
    }


    private static void computeRat(
            ExecutorService cPool,
            TaskType taskType, Stopwatch stopwatch,
            CountDownLatch countDownLatch) {
        CompletableFuture.runAsync(
                        () -> {
                            long oldCount = queryCurrentCount.get();
                            while (!blockingQueue.isEmpty()) {
                                long currrentCount = queryCurrentCount.get();
                                long diffCount = currrentCount - oldCount;
                                if (diffCount == 0) {
                                    continue;
                                }
                                double rate = ((double) diffCount) / 60;
                                double sumRate =
                                        ((double) currrentCount) / (stopwatch.elapsed(TimeUnit.SECONDS) + 1);
                                log.info(
                                        "{}=======rate is {}, and sumRate is {},queryCurrentCount is {},blockingDeque size is {}",
                                        taskType,
                                        rate,
                                        sumRate,
                                        currrentCount,
                                        blockingQueue.size());
                                oldCount = currrentCount;
                                try {
                                    TimeUnit.MINUTES.sleep(1);
                                } catch (Exception e) {
                                    log.error("InterruptedException:", e);
                                    throw new RuntimeException(e);
                                }
                            }
                            log.info("==blockingDeque size " + blockingQueue.size());
                        },
                        cPool)
                .whenComplete(
                        (r, e) -> {
                            if (e != null) {
                                log.error("rate error:", e);
                            }
                            countDownLatch.countDown();
                        });
    }
}
