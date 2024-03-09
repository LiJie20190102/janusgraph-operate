/*
 * Copyright (c) 2022. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
 * Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
 * Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
 * Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */
package com.qsdi.bigdata.janusgaph.ops.query;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Queues;
import com.qsdi.bigdata.janusgaph.ops.base.Base;
import com.qsdi.bigdata.janusgaph.ops.util.LineIterator;
import com.qsdi.bigdata.janusgaph.ops.util.PropertiesUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Description
 *
 * @author lijie0203 2024/1/2 11:01
 */
@Slf4j
public class QueryData extends Base {

    private static final String V_ORIGIN_FILE_NAME;


    private static final String V_READ_FILE_NAME;

    private static final String E_FILE_DIRECTORY;

    private static final long V_QUERY_COUNT = PropertiesUtil.getLongValue("v.query_count",20_000_000L);


    private static AtomicLong queryCurrentCount = new AtomicLong(0);

    private static AtomicLong errorCount = new AtomicLong(0);


    private static ArrayBlockingQueue<List<String>> blockingQueue = Queues.newArrayBlockingQueue(QUEUE_SIZE);
//    private static ArrayBlockingQueue<List<String>> eBlockingDeque = Queues.newArrayBlockingQueue(QUEUE_SIZE);

    private static ExecutorService cPool = Executors.newWorkStealingPool(C_PARALLELISM+2);


    static {

        if (isWinEnv()) {
            V_ORIGIN_FILE_NAME = "D:\\bigdata\\janusgraph-operate\\query\\src\\main\\data\\com-friendster.vertex.txt";
            V_READ_FILE_NAME = "D:\\bigdata\\janusgraph-operate\\query\\src\\main\\data\\query_vertex.txt";
            E_FILE_DIRECTORY = "D:\\bigdata\\janusgraph-operate\\query\\src\\main\\data";
        } else {
            V_ORIGIN_FILE_NAME = PropertiesUtil.getValue("v.origin_file_name", "/usr/graph_data_test/com-friendster.vertex.txt");
            V_READ_FILE_NAME = PropertiesUtil.getValue("v.read_file_name","/usr/graph_data_test/query_data/query_vertex.txt");
            E_FILE_DIRECTORY = PropertiesUtil.getValue("e.read_file_directory", "/usr/graph_data_test/query_data/edge");
        }

    }


    public static void main(String[] args) {

//
        // 点查询
        // 1、随机获取2000W顶点id
        // 2、调用api查询
        log.info("begin to query v====");
        queryVertex();
        log.info("end to query v====");

        queryCurrentCount.set(0);

        while (!blockingQueue.isEmpty()) ;

        log.info("begin to query v==2==");
        queryVertex();
        log.info("end to query v==2==");


        queryCurrentCount.set(0);
        while (!blockingQueue.isEmpty()) ;
//
//        // 边查询
//        // 1、随机获取5亿个边顶点
//        // 2、调用api查询
        log.info("begin to query e====");
        queryEdge();
        log.info("end to query e====");

        queryCurrentCount.set(0);
        while (!blockingQueue.isEmpty()) ;

        log.info("begin to query e=2==");
        queryEdge();
        log.info("end to query e==2==");


    }

    private static void queryEdge() {
        CountDownLatch countDownLatch = new CountDownLatch(2 + C_PARALLELISM);

        // 准备数据
        CompletableFuture.runAsync(() -> {
            File file = new File(E_FILE_DIRECTORY);
            if (file.exists()) {
                getDataFromFile(E_FILE_DIRECTORY, "edge");
            } else {
//                createReadFile("",E_READ_FILE_NAME,"edge");

                throw new RuntimeException(String.format("%s not found", E_FILE_DIRECTORY));
            }

        }, cPool).whenComplete((r, e) -> {
            if (e != null) {
//                error.compareAndSet(false, true);
                log.error("producer error:", e);
            }
            countDownLatch.countDown();
        });

        log.info("====1");


        // 先预放1000
        while (blockingQueue.size() <= 1000) ;
        log.info("====2");

        // 开始计时
        Stopwatch stopwatch = Stopwatch.createStarted();

        // 开始统计性能
        computeRat(cPool, "edge", stopwatch, countDownLatch);
        log.info("====3");

        // 查询操作
        queryData("edge", stopwatch, countDownLatch);
        long elapsed = stopwatch.elapsed(TimeUnit.NANOSECONDS);
        log.info("===error count is {}", errorCount.get());
        log.info("query edge end ,cost {}", elapsed);

    }

    private static void queryVertex() {
        CountDownLatch countDownLatch = new CountDownLatch(2 + C_PARALLELISM);

        // 准备数据
        CompletableFuture.runAsync(() -> {
            File file = new File(V_READ_FILE_NAME);
            if (file.exists()) {
                getDataFromFile(V_READ_FILE_NAME, "vertex");
            } else {
                createReadFile(V_ORIGIN_FILE_NAME, V_READ_FILE_NAME, "vertex");
            }

        }, cPool).whenComplete((r, e) -> {
            if (e != null) {
//                error.compareAndSet(false, true);
                log.error("producer error:", e);
            }
            countDownLatch.countDown();
        });


        // 先预放1000
        while (blockingQueue.size() <= 1000) ;

        // 开始计时
        Stopwatch stopwatch = Stopwatch.createStarted();

        // 开始统计性能
        computeRat(cPool, "vertex", stopwatch, countDownLatch);

        // 查询操作
        queryData("vertex", stopwatch, countDownLatch);
        long elapsed = stopwatch.elapsed(TimeUnit.NANOSECONDS);
        log.info("query vertex end ,cost {}", elapsed);

    }

    private static void createReadFile(String originFileName, String readFileName, String elementType) {
        if ("vertex".equals(elementType)) {
            try (BufferedReader reader = new BufferedReader(new FileReader(originFileName));
                 BufferedWriter writer = new BufferedWriter(new FileWriter(readFileName))) {
                log.info("create readFile {} begin", readFileName);
                long curnCount = 0L;
                while (curnCount < V_QUERY_COUNT) {
                    String currLine;

                    while ((currLine = reader.readLine()) != null && StringUtils.isNotBlank(currLine)) {
                        int randomNumber = new Random().nextInt(3);
                        for (int i = 0; i < randomNumber; i++) {
                            reader.readLine();
                        }
                        writer.write(currLine);
                        writer.newLine();
                        curnCount++;
                        if (curnCount == V_QUERY_COUNT) {
                            break;
                        }
                    }
                    if (curnCount == V_QUERY_COUNT) {
                        break;
                    }

                }
                log.info("create readFile {} end", readFileName);
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {

            // 已在server端完成
//            try (BufferedWriter writer = new BufferedWriter(new FileWriter(readFileName))) {
//                log.info("create readFile {} begin",readFileName);
//                long curnCount = 0L;
//                while (curnCount < E_QUERY_COUNT) {
//                    client.getGraph().listEdges(graphName)
//                }
//
//
//
//                log.info("create readFile {} begin",readFileName);
//            } catch (IOException e) {
//                throw new RuntimeException(e);
//            }


        }

    }

    private static void computeRat(ExecutorService cPool, String elementType, Stopwatch stopwatch, CountDownLatch countDownLatch) {
        CompletableFuture.runAsync(() -> {
            long oldCount = queryCurrentCount.get();
            while (!blockingQueue.isEmpty()) {
                long currrentCount = queryCurrentCount.get();
                long diffCount = currrentCount - oldCount;
                if (diffCount == 0) {
                    continue;
                }
                double rate = ((double) diffCount) / 60;
                double sumRate = ((double) currrentCount) / (stopwatch.elapsed(TimeUnit.SECONDS) + 1);
                log.info("{}=======rate is {}, and sumRate is {},queryCurrentCount is {},blockingDeque size is {}", elementType, rate, sumRate, currrentCount, blockingQueue.size());
                oldCount = currrentCount;
                try {
                    TimeUnit.MINUTES.sleep(1);
                } catch (Exception e) {
                    log.error("InterruptedException:", e);
                    throw new RuntimeException(e);
                }
            }
            log.info("==blockingDeque size " + blockingQueue.size());

        }, cPool).whenComplete((r, e) -> {
            if (e != null) {
                log.error("rate error:", e);
            }
            countDownLatch.countDown();
        });
    }

    private static void queryData(String elementType, Stopwatch stopwatch, CountDownLatch countDownLatch) {
        for (int i = 0; i < C_PARALLELISM; i++) {
            int finalI = i;
            CompletableFuture.runAsync(() -> {
                log.info("aaa" + finalI);
                consumerData(elementType, stopwatch);

            }, cPool).whenComplete((r, e) -> {
                if (e != null) {
//                error.compareAndSet(false, true);
                    log.error("doQuery error:", e);
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


    private static void consumerData(String elementType, Stopwatch stopwatch) {
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
//                    查询操作
                    doQuery(elements, elementType, stopwatch);

                    if (noData) {
                        noData = false;
                    }
                    if (error) {
                        error = false;
                    }
                }
            } catch (Exception e) {


                log.error("get comsumer data error:", e);

                if (StringUtils.isNotEmpty(e.getMessage()) && e.getMessage().contains("has no edges")) {
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException ex) {
                        log.error(Thread.currentThread().getName() + ":InterruptedException:", e);
                    }
                    errorCount.addAndGet(1);
                    continue;
                }

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

    private static void doQuery(List<String> elements, String elementType, Stopwatch stopwatch) {
        if ("vertex".equals(elementType)) {
//            System.out.println("elements =="+elements);
            for (String element : elements) {
                try {
                    client.getGraph().getVertices(GRAPH_NAME, element);
                } catch (Exception e) {
                    throw new RuntimeException(String.format("query v %s error.",element),e);
                }
            }

            long l = queryCurrentCount.addAndGet(elements.size());
//            log.info("{} queue is {},cost {},query count is {}", elementType, elements.size(), stopwatch.elapsed(TimeUnit.NANOSECONDS), l);
        } else {
            for (String element : elements) {
                try {
                    client.getGraph().getEdges(GRAPH_NAME, element);
                } catch (Exception e) {
                    throw new RuntimeException(String.format("query e %s error.",element),e);
                }
            }

            long l = queryCurrentCount.addAndGet(elements.size());
//            log.info("{} queue is {},cost {},query count is {}", elementType, elements.size(), stopwatch.elapsed(TimeUnit.NANOSECONDS), l);
        }
    }


    private static void getDataFromFile(String fileName, String elementType) {

        if ("vertex".equals(elementType)) {
            try (LineIterator<List<String>> lineIterator = new LineIterator<>(fileName, BATCH_READ_SIZE)) {
                productData(lineIterator);
            } catch (Exception e) {
                throw new RuntimeException("producer v error:", e);
            }
        } else {
            File file = new File(fileName);
            assert file.isDirectory();
            List<String> fileNames = Arrays.stream(Objects.requireNonNull(file.list())).map(x -> E_FILE_DIRECTORY + "/" + x).collect(Collectors.toList());

            for (String name : fileNames) {
                try (LineIterator<List<String>> lineIterator = new LineIterator<>(name, BATCH_READ_SIZE)) {
                    productData(lineIterator);
                } catch (FileNotFoundException e) {
                    throw new RuntimeException(e);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }

    }

    private static void productData(LineIterator<List<String>> lineIterator) {
        while (lineIterator.hasNext()) {
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

//    private static void productData(LinkedBlockingDeque<String> blockingDeque, String elementType) {
//        long currentCount = 0L;
//
//        String fileName = ("vertex".equals(elementType)) ? V_FILE_NAME : E_FILE_NAME;
//        long sumCount = ("vertex".equals(elementType)) ? V_QUERY_COUNT : E_QUERY_COUNT;
//
//        try (BufferedReader reader = new BufferedReader(new FileReader(fileName));
//             BufferedWriter writer = new BufferedWriter(new FileWriter(fileName))) {
//
//            String currLine;
//            while (currentCount < sumCount && (currLine = reader.readLine()) != null && StringUtils.isNotBlank(currLine)) {
//                writer.write(currLine);
//                while (!blockingDeque.offerLast(currLine)) ;
//                currentCount++;
//            }
//            writer.flush();
//        } catch (FileNotFoundException e) {
//            throw new RuntimeException(e);
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//
//    }


    public static Boolean isWinEnv() {
        String os = System.getProperty("os.name");
        return os != null && os.toLowerCase().startsWith("windows");
    }
}
