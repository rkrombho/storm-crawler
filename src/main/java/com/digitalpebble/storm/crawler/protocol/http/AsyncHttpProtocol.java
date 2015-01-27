package com.digitalpebble.storm.crawler.protocol.http;

import backtype.storm.Config;
import com.digitalpebble.storm.crawler.protocol.Protocol;
import com.digitalpebble.storm.crawler.protocol.ProtocolResponse;
import com.digitalpebble.storm.crawler.util.VerboseRunnable;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.AsyncHttpClientConfig;
import com.ning.http.client.FluentCaseInsensitiveStringsMap;
import com.ning.http.client.Response;
import crawlercommons.robots.BaseRobotRules;
import org.apache.storm.guava.base.Function;
import org.apache.storm.guava.collect.Maps;
import org.apache.storm.guava.util.concurrent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

/**
 * Enno Shioji
 */
public class AsyncHttpProtocol implements Protocol {
    private static final Logger log = LoggerFactory.getLogger(AsyncHttpProtocol.class);
    private Semaphore maxConnections;
    private AsyncHttpClient client;
    private HttpRobotRulesParser robots;
    private ExecutorService workers;



    @Override
    public void configure(Config conf) {
        // TODO implement config
        AsyncHttpClientConfig config = new AsyncHttpClientConfig.Builder().build();
        this.client = new AsyncHttpClient(config);

        this.robots = new HttpRobotRulesParser(conf);
        workers = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat(this.getClass().getSimpleName() + "-worker-%s").build());
        this.maxConnections = new Semaphore(3);

    }

    @Override
    public ListenableFuture<ProtocolResponse> getProtocolOutput(String url, final Map<String, String[]> metadata) throws Exception {
        // TODO implement metadata
        final HashMap<String, String[]> knownMetadata = new HashMap<String, String[]>(metadata);
        final SettableFuture<ProtocolResponse> ret = SettableFuture.create();

        this.maxConnections.acquire();
        final com.ning.http.client.ListenableFuture<Response> httpResp = client.prepareGet(url).execute();

        httpResp.addListener(new VerboseRunnable() {
            @Override
            protected void doRun() {
                try {
                    Response response = httpResp.get();
                    int code = response.getStatusCode();
                    byte[] content = response.getResponseBodyAsBytes();
                    knownMetadata.putAll(convert(response.getHeaders()));

                    ret.set(new ProtocolResponse(content, code, knownMetadata));
                }catch (Exception e){
                    ret.setException(e);
                }finally {
                    AsyncHttpProtocol.this.maxConnections.release();
                }
            }
        }, workers);
        return ret;
    }

    private Map<String, String[]> convert(final FluentCaseInsensitiveStringsMap headers) {
        Map<String, String[]> ret =  Maps.asMap(headers.keySet(), new Function<String, String[]>() {
            @Override
            public String[] apply(String key) {
                List<String> values = headers.get(key);
                return values.toArray(new String[values.size()]);
            }
        });

        return new HashMap<String, String[]>(ret);

    }

    @Override
    public ListenableFuture<BaseRobotRules> getRobotRules(String url) {
        return this.robots.getRobotRulesSet(this, url);
    }

}
