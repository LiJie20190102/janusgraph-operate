package com.qsdi.bigdata.graph.gstore.performance.test.job.job;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import com.qsdi.bigdata.graph.gstore.structure.graph.AddVerticesRequest;
import com.qsdi.bigdata.graph.gstore.structure.graph.GStoreBatchVertexRequest;
import com.qsdi.bigdata.janusgaph.ops.util.LineIterator;
import com.qsdi.bigdata.multi.graph.api.struct.model.graph.QsdiEdge;
import com.qsdi.bigdata.multi.graph.api.struct.model.graph.QsdiVertex;
import com.qsdi.bigdata.multi.graph.api.struct.model.graph.QsdiVertexForEdge;
import com.qsdi.bigdata.multi.graph.common.id.IdGenerator;
import com.qsdi.bigdata.multi.graph.common.util.CollectionUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;


/**
 * Description
 *
 * @author lijie0203 2024/7/15 17:26
 */
public class UpsertDataJob extends AbstractJob implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(UpsertDataJob.class);
    private static ArrayBlockingQueue<List<String>> vBlockingDeque = Queues.newArrayBlockingQueue(baseConf.getVertexInsertQueue());

    private static ArrayBlockingQueue<List<String>> eBlockingDeque = Queues.newArrayBlockingQueue(baseConf.getEdgeInsertQueue());


    private static AtomicLong saveCount = new AtomicLong(0);

    private static final String V_FILE_NAME;

    private static final String E_FILE_DIRECTORY;

    private static final String METRIC_CSV_DIR;

    // ===========================metrics=========================================
    private static final MetricRegistry metrics = new MetricRegistry();

    static {

        if (isWinEnv()) {
//            V_FILE_NAME = "D:\\code\\janusgraph-operate\\graph-operate-insert-gstore\\src\\main\\resources\\data\\vertex_data1000";
            V_FILE_NAME = "D:\\tmp\\gstore\\性能测试\\vertex_data1000W";
            E_FILE_DIRECTORY = "src/main/resources/data/edge_data";
            METRIC_CSV_DIR = "D:\\code\\janusgraph-operate\\graph-operate-insert-gstore";
        } else {
            V_FILE_NAME = baseConf.getVertexDataFile();
            E_FILE_DIRECTORY = baseConf.getEdgeDataFileDir();

            METRIC_CSV_DIR = baseConf.getMetricCsvDir();


        }

    }


    public static void main(String[] args) throws IOException {


//        int cParallelism = 15;
        ExecutorService cPool = Executors.newWorkStealingPool(baseConf.getConsumerParallelism() + 2);
//        LinkedBlockingDeque<List<String>> blockingDeque = Queues.newLinkedBlockingDeque(QUEUE_SIZE);


        try {
            if (baseConf.getVertexEnable()) {
//            // v
            LOGGER.info("begin to inster v data");
            insertData(baseConf.getConsumerParallelism(), vBlockingDeque, cPool, "vertex");

//            // wait
            while (!vBlockingDeque.isEmpty()) ;

            LOGGER.info("end to inster v data");
            }

            if (!baseConf.getEdgeEnable()) {
                return;
            }

//            // e
//            LOGGER.info("begin to inster e data");
//            saveCount.set(0);
//            insertData(baseConf.getConsumerParallelism(), eBlockingDeque, cPool, "edge");
//            while (!vBlockingDeque.isEmpty()) ;
//            LOGGER.info("end to inster e data");
        } finally {
            cPool.shutdownNow();
            gStoreClient.close();
        }
    }

    private static void insertData(int cParallelism, ArrayBlockingQueue<List<String>> blockingDeque, ExecutorService cPool, String elementType) {
        CountDownLatch countDownLatch = new CountDownLatch(2 + cParallelism);
//        AtomicInteger producer = new AtomicInteger(pParallelism);
        AtomicBoolean error = new AtomicBoolean(false);

        if (baseConf.getVertexEnable()&&"vertex".equals(elementType)) {
            producer(blockingDeque, cPool, error, countDownLatch);
            // 先预放1000
            while (blockingDeque.size() < baseConf.getQueuePrepareSize()) ;
            // 开始计时
            Stopwatch stopwatch = Stopwatch.createStarted();

            doInsert(cParallelism, blockingDeque, cPool, elementType, error, countDownLatch, stopwatch);
        } else {
            if (!baseConf.getEdgeEnable()) {
                return;
            }
            File file = new File(E_FILE_DIRECTORY);
            assert file.isDirectory();
            List<String> fileNames = Arrays.stream(Objects.requireNonNull(file.list())).map(x -> E_FILE_DIRECTORY + "/" + x).collect(Collectors.toList());

            CompletableFuture.runAsync(() -> {
                for (String fileName : fileNames) {
                    try (LineIterator<List<String>> lineIterator = new LineIterator<>(fileName, baseConf.getBatchReadFileSize())) {
                        productData(blockingDeque, lineIterator);
                    } catch (FileNotFoundException e) {
//                        throw new RuntimeException(e);
                        LOGGER.warn("fileName {} not found", fileName);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }

            }, cPool).whenComplete((r, e) -> {
                if (e != null) {
                    error.compareAndSet(false, true);
                    LOGGER.error("producer error:", e);
                }
                countDownLatch.countDown();
            });


            // 先预放1000
            while (blockingDeque.size() <= baseConf.getQueuePrepareSize()) ;

            // 开始计时
            Stopwatch stopwatch = Stopwatch.createStarted();
            doInsert(cParallelism, blockingDeque, cPool, elementType, error, countDownLatch, stopwatch);

        }
    }

    private static void producer(ArrayBlockingQueue<List<String>> blockingDeque, ExecutorService cPool, AtomicBoolean error, CountDownLatch countDownLatch) {
        CompletableFuture.runAsync(() -> {
            try (LineIterator<List<String>> lineIterator = new LineIterator<>(V_FILE_NAME, baseConf.getBatchReadFileSize())) {
                productData(blockingDeque, lineIterator);
            } catch (Exception e) {
                throw new RuntimeException("producer v error:", e);
            }
        }, cPool).whenComplete((r, e) -> {
            if (e != null) {
                error.compareAndSet(false, true);
                LOGGER.error("producer error:", e);
            }
            countDownLatch.countDown();
        });
    }

    private static void doInsert(int cParallelism, ArrayBlockingQueue<List<String>> blockingDeque, ExecutorService cPool, String elementType, AtomicBoolean error, CountDownLatch countDownLatch, Stopwatch stopwatch) {
        try {

            // 开始统计性能
            computeRat(blockingDeque, cPool, elementType, stopwatch, error, countDownLatch);

            // metrics 设置
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss");
            Date date = new Date();
            String formattedDate = sdf.format(date);
            Timer timer = metrics.timer(baseConf.getInsertGraphType()+"_"+elementType+"_"+ formattedDate);
            CsvReporter reporter = CsvReporter.forRegistry(metrics).formatFor(Locale.US)
                    .convertRatesTo(TimeUnit.SECONDS)
                    .convertDurationsTo(TimeUnit.SECONDS)
                    .build(new File(METRIC_CSV_DIR));
            reporter.start(baseConf.getMetricPeriodSecond(), TimeUnit.SECONDS);

            // consumer
            consumer(cParallelism, blockingDeque, cPool, elementType, stopwatch, error, countDownLatch,timer);

            for (; ; ) {
                try {
                    boolean finish = countDownLatch.await(1, TimeUnit.SECONDS);
                    if (finish || error.get()) {
                        break;
                    }
                } catch (Exception e) {
                    LOGGER.error(e.getMessage(), e);
                }
            }
            LOGGER.info(elementType + " end, cost all " + stopwatch.stop().elapsed(TimeUnit.MILLISECONDS));
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

    private static void consumer(int cParallelism, ArrayBlockingQueue<List<String>> blockingDeque, ExecutorService cPool, String elementType, Stopwatch stopwatch, AtomicBoolean error, CountDownLatch countDownLatch, Timer timer) {
        for (int i = 0; i < cParallelism; i++) {

            CompletableFuture.runAsync(() -> {
                //
                consumerData(blockingDeque, elementType, stopwatch,timer);
            }, cPool).whenComplete((r, e) -> {
                if (e != null) {
                    error.compareAndSet(false, true);
                    LOGGER.error("comsumer error:", e);
                }
                countDownLatch.countDown();

            });
        }
    }

    private static void computeRat(ArrayBlockingQueue<List<String>> blockingDeque, ExecutorService cPool, String elementType, Stopwatch stopwatch, AtomicBoolean error, CountDownLatch countDownLatch) {
        CompletableFuture.runAsync(() -> {
            long oldCount = saveCount.get();
            while (!blockingDeque.isEmpty()) {
                long currrentCount = saveCount.get();

                long diffCount = currrentCount - oldCount;
                if (diffCount == 0) {
                    continue;
                }
                double rate = ((double) diffCount) / 60;
                double sumRate = ((double) currrentCount) / (stopwatch.elapsed(TimeUnit.SECONDS) + 1);
                LOGGER.info("{}===================rate is {}, and sumRate is {},saveCount is{}", elementType, rate, sumRate,currrentCount);
                oldCount = currrentCount;
                try {
                    TimeUnit.MINUTES.sleep(1);
                } catch (Exception e) {
                    LOGGER.error("InterruptedException:", e);
                    throw new RuntimeException(e);
                }
            }

        }, cPool).whenComplete((r, e) -> {
            if (e != null) {
                error.compareAndSet(false, true);
                LOGGER.error("rate error:", e);
            }
            countDownLatch.countDown();
        });
    }

    private static void productData(ArrayBlockingQueue<List<String>> blockingDeque, LineIterator<List<String>> lineIterator) {

        while (lineIterator.hasNext()) {
            List<String> next = lineIterator.next();
            boolean insertSuccess;
            do {

                try {
                    blockingDeque.put(next);
                    insertSuccess = true;
                } catch (InterruptedException e) {
                    insertSuccess = false;
                }

            } while (!insertSuccess);

        }

    }

    private static void consumerData(ArrayBlockingQueue<List<String>> blockingDeque, String elementType, Stopwatch stopwatch, Timer timer) {
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
                    int saveSize = saveData2graph(elements, elementType,timer);
//                    if (elements.size() != saveSize) {
//                        log.warn(String.format("elements is %s, and save size is %s", elements.size(), saveSize));
//                    }
                    LOGGER.info(String.format("%s ---save [%s] end ,all save is [%s], all save =[%s]= now is all cost [%s]=====queue size is %s", Thread.currentThread().getName(), saveSize, saveCount.addAndGet(saveSize), elementType, stopwatch.elapsed(TimeUnit.MILLISECONDS), blockingDeque.size()));

                    if (noData) {
                        noData = false;
                    }
                    if (error) {
                        error = false;
                    }
                }
            } catch (Exception e) {
                LOGGER.error("get comsumer data error:", e);

                if (StringUtils.isNotEmpty(e.getMessage()) && e.getMessage().contains("write overflow")) {
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException ex) {
                        LOGGER.error(Thread.currentThread().getName() + ":InterruptedException:", e);
                    }
                    error = true;
                    continue;
                }

                if (error && count > baseConf.getErrCount()) {
                    LOGGER.error(Thread.currentThread().getName() + ":error count is {}", baseConf.getErrCount());
                    throw new RuntimeException(e);
                }

                if (error) {
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException ex) {
                        LOGGER.error(Thread.currentThread().getName() + ":InterruptedException:", e);
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

    private static int saveData2graph(List<String> elements, String elementType, Timer timer) throws InterruptedException {

        List<List<String>> listList = Lists.partition(elements, baseConf.getBatchSaveGraphSize());
//        TxConfig txConfig = TxConfig.builder().useQsdiIndexLogic(false).useQsdiIndexLogLogic(false).build();

        int batchSumCount = 0;
        for (List<String> list : listList) {
            try (Timer.Context time = timer.time()) {
                if (baseConf.getVertexEnable() && "vertex".equals(elementType)) {
                    List<QsdiVertex> vertexList = list.stream().map(element -> {

                        if ("relation-center".equalsIgnoreCase(baseConf.getDataSource())) {
                            QsdiVertex person = new QsdiVertex(baseConf.getEdgeLabel());
                            person.property("id", element);
                            person.setId(IdGenerator.of(element));
                            return person;
                        } else {
                            JSONObject jsonObject = JSON.parseObject(element);
                            Object id = jsonObject.get("id");
                            Object properties = jsonObject.get("properties");
                            QsdiVertex qsdiVertex = new QsdiVertex("real_name_archive_test");
                            qsdiVertex.setId(IdGenerator.of(id));
                            qsdiVertex.setProperties((Map<String, Object>) properties);
                            return qsdiVertex;
                        }
                    }).collect(Collectors.toList());

                    if (CollectionUtil.isEmpty(vertexList)) {
                        return batchSumCount;
                    }

                    if ("insert".equalsIgnoreCase(baseConf.getInsertGraphType())) {
                        AddVerticesRequest verticesRequest = AddVerticesRequest.builder().vertices(vertexList).build();
                        List<QsdiVertex> qsdiVertices = null;
                        qsdiVertices = gStoreClient.getGraph().addVertices(baseConf.getGraphName(), verticesRequest);

                        batchSumCount += qsdiVertices.size();
                    } else if ("update".equalsIgnoreCase(baseConf.getInsertGraphType())) {
                        GStoreBatchVertexRequest gStoreBatchVertexRequest =
                                GStoreBatchVertexRequest.createBuilder()
//                                    .updatingStrategies(gstoreUpdateStrategies)
                                        .vertices(vertexList)
                                        .build();

                        List<QsdiVertex> qsdiVertices = gStoreClient.getGraph().upsertVertices(baseConf.getGraphName(), gStoreBatchVertexRequest);
                        batchSumCount += qsdiVertices.size();
                    }


                } else if (baseConf.getEdgeEnable() && "edge".equals(elementType)) {
                    List<QsdiEdge> edgeList = list.stream().filter(x -> !x.startsWith(":") && StringUtils.isNotBlank(x)).map(element -> {
                        String[] split = element.split(",");

                        QsdiEdge edge = new QsdiEdge(baseConf.getEdgeLabel());
                        QsdiVertexForEdge sourceV = new QsdiVertexForEdge();
                        sourceV.setVertexId(IdGenerator.of(split[0]));
                        QsdiVertexForEdge targetV = new QsdiVertexForEdge();
                        targetV.setVertexId(IdGenerator.of(split[1]));

                        edge.setSourceV(sourceV);
                        edge.setTargetV(targetV);
                        return edge;
                    }).collect(Collectors.toList());

                    if (edgeList.isEmpty()) {
                        return batchSumCount;
                    }

//                AddEdgesRequest edgesRequest = AddEdgesRequest.builder().edges(edgeList).build();
//                List<QsdiEdge> qsdiEdges = client.getGraph().addEdges(GRAPH_NAME, edgesRequest);
//                return qsdiEdges.size();

                }
            }
        }
        return batchSumCount;
    }


    @Override
    public void close() throws Exception {
        gStoreClient.close();
    }


}
