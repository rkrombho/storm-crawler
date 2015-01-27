package com.digitalpebble.storm.crawler.util;

import org.apache.storm.guava.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

/**
 * Enno Shioji
 */
public class ConcurrentUtils {
    private ConcurrentUtils(){}
    
    public static ScheduledExecutorService defaultScheduledExecutorService(String name, int threadNum){
        ThreadFactory tf = new ThreadFactoryBuilder().setNameFormat(name + "-%s").build();
        return new ScheduledThreadPoolExecutor(threadNum, tf, new ThreadPoolExecutor.CallerRunsPolicy());
    }

}
