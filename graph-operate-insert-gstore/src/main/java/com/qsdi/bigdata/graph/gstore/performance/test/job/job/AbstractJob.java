package com.qsdi.bigdata.graph.gstore.performance.test.job.job;

import com.qsdi.bigdata.graph.gstore.driver.GStoreClient;
import com.qsdi.bigdata.graph.gstore.driver.GStoreClientBuilder;
import com.qsdi.bigdata.graph.gstore.performance.test.job.conf.BaseConf;

/**
 * Description
 *
 * @author lijie0203 2024/7/15 18:19
 */
public abstract class AbstractJob implements Job{
    protected static GStoreClient gStoreClient;
    protected static BaseConf baseConf=new BaseConf();

    static {
        GStoreClientBuilder GStoreClientBuilder = new GStoreClientBuilder(baseConf.getGstoreUrl());
        // 单位s
        GStoreClientBuilder.configTimeout(baseConf.getGstoreConnectTimeOut());
        gStoreClient = new GStoreClient(GStoreClientBuilder);
    }
}
