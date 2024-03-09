/*
 * Copyright (c) 2022. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
 * Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
 * Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
 * Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */
package com.qsdi.bigdata.janusgaph.ops.travel.domain;

import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Queues;
import com.qsdi.bigdata.janusgaph.ops.base.Base;
import com.qsdi.bigdata.janusgaph.ops.travel.enums.TaskType;
import com.qsdi.bigdata.janusgaph.ops.travel.service.TravelService;
import com.qsdi.bigdata.janusgaph.ops.travel.service.impl.JanusGraphTravelService;
import com.qsdi.bigdata.janusgaph.ops.util.LineIterator;
import com.qsdi.bigdata.janusgaph.ops.util.PropertiesUtil;
import com.qsdi.bigdata.multi.graph.api.struct.model.traverser.QsdiKneighborRequest;
import com.qsdi.bigdata.multi.graph.common.id.IdGenerator;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Description
 *
 * @author lijie0203 2024/2/1 21:40
 */
@Slf4j
public class TravelQuery extends Base {

    private static final String FIND_NEIGHBOURS_FILE_NAME =
            PropertiesUtil.getValue("find.neighbours.file_name", "/usr/graph_data_test/query_data/query_vertex.txt");

    private static final String FIND_SHORTEST_PATH_FILE_NAME = PropertiesUtil.getValue("find.shortest_path.file_name", "/usr/graph_data_test/com-friendster.vertex.txt");

    private static final String METRIC_CSV_DIR = PropertiesUtil.getValue("metric.csv.dir", "/usr/graph_data_test/graph_test/janusgraph-travel-query/metric_csv");


    private static final long FIND_NEIGHBOURS_QUERY_COUNT = PropertiesUtil.getLongValue("find_neighbours.query_count",150000L);
    private static final long FIND_SHORTEST_PATH_QUERY_COUNT = PropertiesUtil.getLongValue("find_shortest_path.query_count",300000L);



    private static AtomicLong queryCurrentCount = new AtomicLong(0);

    private static ArrayBlockingQueue<List<String>> blockingQueue = Queues.newArrayBlockingQueue(QUEUE_SIZE);


    private static ExecutorService cPool = Executors.newWorkStealingPool(C_PARALLELISM + 2);


    // ===========================metrics=========================================
    private static final MetricRegistry metrics = new MetricRegistry();

    public static void main(String[] args) {


        log.info("init janus client");
        TravelService janusGraphTravelService = new JanusGraphTravelService();
        // 第一次
        log.info("janus=1=begin");
        allShortestPathQuery(janusGraphTravelService);
        kneighborQuery(janusGraphTravelService);

        log.info("==janus=1=end");
        // 第二次
        log.info("janus=2=begin");
        allShortestPathQuery(janusGraphTravelService);
        kneighborQuery(janusGraphTravelService);

        log.info("==janus=2=end");


//
//
//
//
//
//        log.info("init huge client");
//        HugegraphTravelService hugegraphTravelService = new HugegraphTravelService();
//        log.info("huge=1=begin");
//        allShortestPathQuery(hugegraphTravelService);
//        kneighborQuery(hugegraphTravelService);
//        log.info("huge=1=end");
//        log.info("huge=2=begin");
//        allShortestPathQuery(hugegraphTravelService);
//        kneighborQuery(hugegraphTravelService);
//        log.info("huge=2=end");
//
//        log.info("huge==end");


    }

    private static void allShortestPathQuery(TravelService travelService) {
        Map<String, String> queryParam = new HashMap<>();
        queryParam.put("limit", "100000");
//        queryParam.put("maxDepth", "1");
//        log.info("================================allShortestPathQuery {}1 start", graphType);
//        travelQuery(janusGraphTravelService, TaskType.FIND_SHORTEST_PATH, queryParam);
//        while (!blockingQueue.isEmpty()) ;
//
//        log.info("================================allShortestPathQuery {}1 end", graphType);


        log.info("================================allShortestPathQuery {}2 start", travelService.getGraphType());


        queryParam.put("maxDepth", "2");
        travelQuery(travelService, TaskType.FIND_SHORTEST_PATH, queryParam);
        while (!blockingQueue.isEmpty()) ;
        queryCurrentCount.set(0);


        log.info("================================allShortestPathQuery {}2 end", travelService.getGraphType());

        log.info("================================allShortestPathQuery {}3 start", travelService.getGraphType());


        queryParam.put("maxDepth", "3");
        travelQuery(travelService, TaskType.FIND_SHORTEST_PATH, queryParam);
        while (!blockingQueue.isEmpty()) ;
        queryCurrentCount.set(0);
        log.info("================================allShortestPathQuery {}3 end", travelService.getGraphType());

    }

    private static void kneighborQuery(TravelService travelService) {
        log.info("================================kneighborQuery {}1 start", travelService.getGraphType());
        Map<String, String> queryParam = new HashMap<>();
        queryParam.put("maxDepth", "1");
        queryParam.put("limit", "30");
        travelQuery(travelService, TaskType.FIND_NEIGHBOURS, queryParam);
        while (!blockingQueue.isEmpty()) ;
        queryCurrentCount.set(0);
        log.info("================================kneighborQuery {}1 end", travelService.getGraphType());


        log.info("================================kneighborQuery {}2 start", travelService.getGraphType());

        queryParam.put("maxDepth", "2");
        travelQuery(travelService, TaskType.FIND_NEIGHBOURS, queryParam);
        while (!blockingQueue.isEmpty()) ;
        queryCurrentCount.set(0);

        log.info("================================kneighborQuery {}2 end", travelService.getGraphType());


        log.info("================================kneighborQuery {}3 start", travelService.getGraphType());

        queryParam.put("maxDepth", "3");
        travelQuery(travelService, TaskType.FIND_NEIGHBOURS, queryParam);
        while (!blockingQueue.isEmpty()) ;
        queryCurrentCount.set(0);
        log.info("================================kneighborQuery {}3 end", travelService.getGraphType());
    }

//    private static void edgeAdjacentNodesQuery(TravelService service, Map<String, String> queryParam) {
//        CountDownLatch countDownLatch = new CountDownLatch(2 + C_PARALLELISM);
//
//
//        // 准备数据
//        CompletableFuture.runAsync(
//                        () -> {
//                            getDataFromFile(TaskType.FIND_ADJACENT_NODES);
//                        },
//                        cPool)
//                .whenComplete(
//                        (r, e) -> {
//                            if (e != null) {
//                                log.error("producer error:", e);
//                            }
//                            countDownLatch.countDown();
//                        });
//
//        // 先预放1000
//        while (blockingQueue.size() <= 1000) ;
//
//        // 开始计时
//        Stopwatch stopwatch = Stopwatch.createStarted();
//
//        try {
//            Thread.sleep(1000);
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        }
//        // 开始统计性能
//        computeRat(cPool, TaskType.FIND_ADJACENT_NODES, stopwatch, countDownLatch);
//
//
//        queryData(service, queryParam, TaskType.FIND_ADJACENT_NODES, stopwatch, countDownLatch);
//    }

//    private static void findShortestPathQuery(TravelService service, Map<String, String> queryParam) {
//        CountDownLatch countDownLatch = new CountDownLatch(2 + C_PARALLELISM);
//
//
//        // 准备数据
//        CompletableFuture.runAsync(
//                        () -> {
//                            getDataFromFile(TaskType.FIND_SHORTEST_PATH);
//                        },
//                        cPool)
//                .whenComplete(
//                        (r, e) -> {
//                            if (e != null) {
//                                log.error("producer error:", e);
//                            }
//                            countDownLatch.countDown();
//                        });
//
//        // 先预放1000
//        while (blockingQueue.size() <= 1000) ;
//
//        // 开始计时
//        Stopwatch stopwatch = Stopwatch.createStarted();
//
//        // 开始统计性能
//        computeRat(cPool, TaskType.FIND_SHORTEST_PATH, stopwatch, countDownLatch);
//
//
//        queryData(service, queryParam, TaskType.FIND_SHORTEST_PATH, stopwatch, countDownLatch);
//
//    }

    private static void travelQuery(TravelService travelService, TaskType taskType, Map<String, String> queryParam) {
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
        while (blockingQueue.size() <= 50) ;

        // 开始计时
        Stopwatch stopwatch = Stopwatch.createStarted();
        int maxDepth = Integer.parseInt(queryParam.get("maxDepth"));

        // 开始统计性能
        computeRat(cPool, taskType, stopwatch, countDownLatch);


        SimpleDateFormat sdf = new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss");
        Date date = new Date();
        String formattedDate = sdf.format(date);

        Timer timer = metrics.timer(travelService.getGraphType() + "_" + taskType + "_" + C_PARALLELISM + "_" + maxDepth + "_" + formattedDate);
        CsvReporter reporter = CsvReporter.forRegistry(metrics).formatFor(Locale.US)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.SECONDS)
                .build(new File(METRIC_CSV_DIR));
        reporter.start(30, TimeUnit.SECONDS);

        queryData(travelService, queryParam, taskType, stopwatch, countDownLatch, timer);

        long elapsed = stopwatch.elapsed(TimeUnit.MILLISECONDS);
        String s = queryParam.get("maxDepth");
        log.info("query end ,cost {},maxDepth is {}", elapsed, s);
    }


    private static void queryData(TravelService service, Map<String, String> queryParam, TaskType taskType, Stopwatch stopwatch,
                                  CountDownLatch countDownLatch, Timer timer) {
        for (int i = 0; i < C_PARALLELISM; i++) {
            int finalI = i;
            CompletableFuture.runAsync(
                            () -> {
                                log.info("aaa" + finalI);
                                consumerData(service, queryParam, taskType, stopwatch, timer);
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

    private static void consumerData(TravelService service, Map<String, String> queryParam, TaskType taskType, Stopwatch stopwatch, Timer timer) {
        boolean noData = false;
        boolean error = false;
        List<String> elements = null;
        String maxDepth = queryParam.get("maxDepth");
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
                    query(service, queryParam, taskType, elements, timer);
                    log.info("{},maxDepth:{}, query {} cost {},all query {},queue is {}", taskType, maxDepth, elements.size(), stopwatch.elapsed(TimeUnit.MILLISECONDS), queryCurrentCount.addAndGet(elements.size()), blockingQueue.size());

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

    private static void query(TravelService service, Map<String, String> queryParam, TaskType taskType, List<String> elements, Timer timer) {
        String maxDepth = queryParam.get("maxDepth");
        String limit = queryParam.get("limit");
        String sourceElement = null;
        for (String element : elements) {
            try (Timer.Context time = timer.time()) {
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
    }

    private static void getDataFromFile(TaskType taskType) {
        String fileName = null;
        if (taskType == TaskType.FIND_NEIGHBOURS) {
            fileName = FIND_NEIGHBOURS_FILE_NAME;
        } else if (taskType == TaskType.FIND_SHORTEST_PATH) {
            fileName = FIND_SHORTEST_PATH_FILE_NAME;
        }
        assert StringUtils.isNotEmpty(fileName);
        try (LineIterator<List<String>> lineIterator =
                     new LineIterator<>(fileName, BATCH_READ_SIZE)) {
            productData(lineIterator, taskType);
        } catch (Exception e) {
            throw new RuntimeException("producer v error:", e);
        }
    }

    private static void productData(LineIterator<List<String>> lineIterator, TaskType taskType) {
        long queueCount = 0;
        if (taskType == TaskType.FIND_NEIGHBOURS) {
            queueCount = FIND_NEIGHBOURS_QUERY_COUNT;
        } else if (taskType == TaskType.FIND_SHORTEST_PATH) {
            queueCount = FIND_SHORTEST_PATH_QUERY_COUNT;
        }

        while (lineIterator.hasNext() && queryCurrentCount.get() < queueCount) {
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
