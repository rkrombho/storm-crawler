package com.digitalpebble.storm.crawler.protocol.http;

import backtype.storm.Config;
import com.digitalpebble.storm.crawler.protocol.Protocol;
import com.digitalpebble.storm.crawler.protocol.ProtocolResponse;
import com.digitalpebble.storm.crawler.util.VerboseRunnable;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.AsyncHttpClientConfig;
import crawlercommons.robots.BaseRobotRules;
import org.apache.storm.guava.util.concurrent.Futures;
import org.apache.storm.guava.util.concurrent.ListenableFuture;
import org.apache.storm.guava.util.concurrent.MoreExecutors;
import org.apache.storm.guava.util.concurrent.SettableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Enno Shioji
 */
public class AsyncHttpProtocol implements Protocol {
    private static final Logger log = LoggerFactory.getLogger(AsyncHttpProtocol.class);
    private AsyncHttpClient client;
    private HttpRobotRulesParser robots;


    @Override
    public void configure(Config conf) {
        // TODO implement config
        AsyncHttpClientConfig config = new AsyncHttpClientConfig.Builder().build();
        this.client = new AsyncHttpClient(config);

        this.robots = new HttpRobotRulesParser();
    }

    @Override
    public ListenableFuture<ProtocolResponse> getProtocolOutput(String url, Map<String, String[]> metadata) throws Exception {
        // TODO implement metadata
        final SettableFuture<ProtocolResponse> ret = SettableFuture.create();
        client.prepareGet(url).execute().addListener(new VerboseRunnable() {
            @Override
            protected void doRun() {
                try {
                    ProtocolResponse resp = ret.get();
                    ret.set(resp);
                } catch (Exception e) {
                    ret.setException(e);
                }
            }
        }, MoreExecutors.sameThreadExecutor());
        return ret;
    }

    @Override
    public ListenableFuture<BaseRobotRules> getRobotRules(String url) {
        return this.robots.getRobotRulesSet(this, url);
    }
}
