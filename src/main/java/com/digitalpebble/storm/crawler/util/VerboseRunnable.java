package com.digitalpebble.storm.crawler.util;

import org.apache.storm.guava.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Enno Shioji
 */
public abstract class VerboseRunnable implements Runnable{
    private static final Logger log = LoggerFactory.getLogger(VerboseRunnable.class);

    @Override
    public void run() {
        try{
            doRun();
        }catch (Exception e){
            log.error("Exception:", e);
            throw Throwables.propagate(e);
        }
    }

    protected abstract void doRun() throws Exception;
}
