/**
 * Licensed to DigitalPebble Ltd under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * DigitalPebble licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.digitalpebble.storm.crawler.bolt;

import backtype.storm.Config;
import backtype.storm.metric.api.MultiCountMetric;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import com.digitalpebble.storm.crawler.filtering.URLFilterUtil;
import com.digitalpebble.storm.crawler.filtering.URLFilters;
import com.digitalpebble.storm.crawler.persistence.Status;
import com.digitalpebble.storm.crawler.protocol.HttpHeaders;
import com.digitalpebble.storm.crawler.protocol.Protocol;
import com.digitalpebble.storm.crawler.protocol.ProtocolFactory;
import com.digitalpebble.storm.crawler.protocol.ProtocolResponse;
import com.digitalpebble.storm.crawler.util.*;
import crawlercommons.robots.BaseRobotRules;
import crawlercommons.url.PaidLevelDomain;
import org.apache.commons.lang.StringUtils;
import org.apache.storm.guava.cache.Cache;
import org.apache.storm.guava.cache.CacheBuilder;
import org.apache.storm.guava.collect.ImmutableMap;
import org.apache.storm.guava.util.concurrent.ListenableFuture;
import org.apache.storm.guava.util.concurrent.MoreExecutors;
import org.apache.storm.guava.util.concurrent.RateLimiter;
import org.apache.storm.guava.util.concurrent.ThreadFactoryBuilder;
import org.elasticsearch.common.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.*;

/**
 * A multithreaded, queue-based fetcher adapted from Apache Nutch. Enforces the
 * politeness and handles the fetching threads itself.
 */

public class NonBlockingFetcherBolt extends BaseRichBolt {
    private static final String STATUS_STREAM = com.digitalpebble.storm.crawler.Constants.StatusStreamName;


    public static final Logger LOG = LoggerFactory.getLogger(NonBlockingFetcherBolt.class);

    private Config conf;

    private OutputCollector _collector;

    private MultiCountMetric eventCounter;

    private ProtocolFactory protocolFactory;

    private int taskIndex = -1;

    private URLFilterUtil parentURLFilter;

    private URLFilters urlFilters;

    private boolean allowRedirs;

    private MetadataTransfer metadataTransfer;

    private ScheduledExecutorService dispatcher;

    private Executor workers;

    private ConcurrentMap<String, RateLimiter> lastFetched;

    /**
     * This class described the item to be fetched.
     */
    private static class FetchItem {

        String queueID;
        String url;
        URL u;
        Tuple t;

        public FetchItem(String url, URL u, Tuple t, String queueID) {
            this.url = url;
            this.u = u;
            this.queueID = queueID;
            this.t = t;
        }

        /**
         * Create an item. Queue id will be created based on
         * <code>queueMode</code> argument, either as a protocol + hostname
         * pair, protocol + IP address pair or protocol+domain pair.
         */

        public static FetchItem create(Tuple t, String queueMode) {

            String url = t.getStringByField("url");

            String queueID;
            URL u = null;
            try {
                u = new URL(url.toString());
            } catch (Exception e) {
                LOG.warn("Cannot parse url: {}", url, e);
                return null;
            }
            final String proto = u.getProtocol().toLowerCase();
            String key = null;
            // reuse any key that might have been given
            // be it the hostname, domain or IP
            if (t.contains("key")) {
                key = t.getStringByField("key");
            }
            if (StringUtils.isNotBlank(key)) {
                queueID = proto + "://" + key.toLowerCase();
                return new FetchItem(url, u, t, queueID);
            }

            if (QUEUE_MODE_IP.equalsIgnoreCase(queueMode)) {
                try {
                    final InetAddress addr = InetAddress.getByName(u.getHost());
                    key = addr.getHostAddress();
                } catch (final UnknownHostException e) {
                    // unable to resolve it, so don't fall back to host name
                    LOG.warn("Unable to resolve: {}, skipping.", u.getHost());
                    return null;
                }
            } else if (QUEUE_MODE_DOMAIN
                    .equalsIgnoreCase(queueMode)) {
                key = PaidLevelDomain.getPLD(u);
                if (key == null) {
                    LOG.warn(
                            "Unknown domain for url: {}, using URL string as key",
                            url);
                    key = u.toExternalForm();
                }
            } else {
                key = u.getHost();
                if (key == null) {
                    LOG.warn(
                            "Unknown host for url: {}, using URL string as key",
                            url);
                    key = u.toExternalForm();
                }
            }
            queueID = proto + "://" + key.toLowerCase();
            return new FetchItem(url, u, t, queueID);
        }

    }

    public static final String QUEUE_MODE_HOST = "byHost";
    public static final String QUEUE_MODE_DOMAIN = "byDomain";
    public static final String QUEUE_MODE_IP = "byIP";

    private void handleRedirect(Tuple t, String sourceUrl, String newUrl,
                                Map<String, String[]> sourceMetadata) {

        // build an absolute URL
        URL sURL;
        try {
            sURL = new URL(sourceUrl);
            URL tmpURL = URLUtil.resolveURL(sURL, newUrl);
            newUrl = tmpURL.toExternalForm();
        } catch (MalformedURLException e) {
            LOG.debug("MalformedURLException on {} or {}", sourceUrl, newUrl);
            return;
        }

        // apply URL filters
        if (this.urlFilters != null) {
            newUrl = this.urlFilters.filter(newUrl);
        }

        // filtered
        if (newUrl == null) {
            return;
        }

        // check that within domain or hostname
        if (parentURLFilter != null) {
            parentURLFilter.setSourceURL(sURL);
            if (!parentURLFilter.filter(newUrl))
                return;
        }

        Map<String, String[]> metadata = metadataTransfer
                .getMetaForOutlink(sourceMetadata);

        // TODO check that hasn't exceeded max number of redirections
        emitAndAck(t, STATUS_STREAM, newUrl, new HashMap<String, String[]>(metadata), Status.DISCOVERED);
    }

    private void checkConfiguration() {

        // ensure that a value has been set for the agent name and that that
        // agent name is the first value in the agents we advertise for robot
        // rules parsing
        String agentName = (String) getConf().get("http.agent.name");
        if (agentName == null || agentName.trim().length() == 0) {
            String message = "Fetcher: No agents listed in 'http.agent.name'"
                    + " property.";
            LOG.error(message);
            throw new IllegalArgumentException(message);
        }
    }

    private Config getConf() {
        return this.conf;
    }

    private long maxCrawlDelay;
    private String queueMode;
    private long crawlDelay;
    private long minCrawlDelay;

    @Override
    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {

        _collector = collector;
        this.conf = new Config();
        this.conf.putAll(stormConf);
        this.maxCrawlDelay = ConfUtils.getInt(conf,
                "fetcher.max.crawl.delay", 30) * 1000;

        queueMode = ConfUtils.getString(conf, "fetcher.queue.mode",
                QUEUE_MODE_HOST);
        // check that the mode is known
        if (!queueMode.equals(QUEUE_MODE_IP)
                && !queueMode.equals(QUEUE_MODE_DOMAIN)
                && !queueMode.equals(QUEUE_MODE_HOST)) {
            LOG.error("Unknown partition mode : {} - forcing to byHost",
                    queueMode);
            queueMode = QUEUE_MODE_HOST;
        }
        LOG.info("Using queue mode : {}", queueMode);

        this.crawlDelay = (long) (ConfUtils.getFloat(conf,
                "fetcher.server.delay", 1.0f) * 1000);
        this.minCrawlDelay = (long) (ConfUtils.getFloat(conf,
                "fetcher.server.min.delay", 0.0f) * 1000);


        checkConfiguration();

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long start = System.currentTimeMillis();
        LOG.info("[Fetcher #{}] : starting at {}", taskIndex, sdf.format(start));

        protocolFactory = new ProtocolFactory(conf);

        dispatcher = ConcurrentUtils
                .defaultScheduledExecutorService(
                        this.getClass().getSimpleName() + "-dispatcher",
                        Runtime.getRuntime().availableProcessors()
                );

        workers = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat(this.getClass().getSimpleName() + "-worker-%s").build());

        Cache<String, RateLimiter> lastFetched = CacheBuilder.newBuilder().expireAfterAccess(maxCrawlDelay, TimeUnit.SECONDS).build();
        this.lastFetched = lastFetched.asMap();

        this.taskIndex = context.getThisTaskIndex();

        this.parentURLFilter = new URLFilterUtil(conf);

        String urlconfigfile = ConfUtils.getString(conf,
                "urlfilters.config.file", "urlfilters.json");

        if (urlconfigfile != null)
            try {
                urlFilters = new URLFilters(urlconfigfile);
            } catch (IOException e) {
                LOG.error("Exception caught while loading the URLFilters");
                throw new RuntimeException(
                        "Exception caught while loading the URLFilters", e);
            }

        allowRedirs = ConfUtils.getBoolean(stormConf,
                com.digitalpebble.storm.crawler.Constants.AllowRedirParamName,
                true);

        metadataTransfer = new MetadataTransfer(stormConf);

        this.eventCounter = context.registerMetric("fetcher_counter",
                new MultiCountMetric(), 10);

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("url", "content", "metadata"));
        declarer.declareStream(
                com.digitalpebble.storm.crawler.Constants.StatusStreamName,
                new Fields("url", "metadata", "status"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        return conf;
    }

    @Override
    public void execute(Tuple input) {
        if (!input.contains("url") || Strings.isEmpty(input.getStringByField("url"))) {
            LOG.info("[Fetcher #{}] Missing field url in tuple {}", taskIndex, input);
            // ignore silently
            synchronized (_collector) {
                _collector.ack(input);
            }
            return;
        }


        FetchItem fetchItem = FetchItem.create(input, QUEUE_MODE_DOMAIN);
        dispatcher.execute(new Dispatch(fetchItem));
    }


    private class Dispatch extends VerboseRunnable {
        private final FetchItem fit;
        private final Map<String, String[]> metadata;


        public Dispatch(FetchItem fit) {
            this.fit = fit;

            Map<String, String[]> metadata = null;
            if (fit.t.contains("metadata")) {
                metadata = (Map<String, String[]>) fit.t
                        .getValueByField("metadata");
            }
            this.metadata = metadata == null ? Collections.<String, String[]>emptyMap() : metadata;
        }

        @Override
        protected void doRun() throws Exception {
            Protocol protocol = NonBlockingFetcherBolt.this.protocolFactory.getProtocol(fit.u);
            if (protocol == null) {
                throw new RuntimeException("No protocol implementation found for " + fit.url);
            }
            final ListenableFuture<BaseRobotRules> rulesFuture = protocol.getRobotRules(fit.url);

            rulesFuture.addListener(new VerboseRunnable() {
                @Override
                protected void doRun() throws Exception {
                    BaseRobotRules rules = rulesFuture.get();
                    if (!rules.isAllowed(fit.u.toString())) {
                        LOG.info("Denied by robots.txt: {}", fit.url);
                        markCount("denied-by-robots");
                        emitAndAck(fit.t, STATUS_STREAM, fit.url, new HashMap<String, String[]>(metadata), Status.ERROR);
                    } else {
                        if (rules.getCrawlDelay() > 0 && rules.getCrawlDelay() > maxCrawlDelay && maxCrawlDelay >= 0) {
                            LOG.info("Crawl-Delay for {} too long ({}), skipping",
                                    fit.url, rules.getCrawlDelay());
                            emitAndAck(fit.t, STATUS_STREAM, fit.url, metadata, Status.ERROR);
                            return;
                        }

                        if (rules.getCrawlDelay() > 0) {
                            long crawlDelay = rules.getCrawlDelay();
                            crawlDelay = crawlDelay < 0 ? 0 : crawlDelay;
                            LOG.info(
                                    "Crawl delay for queue: {}  is set to {} as per robots.txt. url: {}",
                                    fit.queueID, crawlDelay, fit.url);
                            RateLimiter limiter = RateLimiter.create(1.0 / crawlDelay);
                            NonBlockingFetcherBolt.this.lastFetched.putIfAbsent(fit.queueID, limiter);
                        }
                        dispatcher.execute(new Fetch(fit));

                    }
                }
            }, workers);

        }

    }

    private void markCount(String counterName) {
        synchronized (eventCounter) {
            eventCounter.scope(counterName).incr();
        }
    }

    private void emitAndAck(Tuple anchor, String streamID, Object... toEmit) {

        synchronized (_collector) {
            Values vals = new Values(toEmit);
            if (anchor == null) {
                _collector.emit(streamID, vals);
            } else {
                _collector.emit(streamID, Arrays.asList(anchor), vals);
                _collector.ack(anchor);
            }
        }
    }

    private class Fetch extends VerboseRunnable {
        private final FetchItem fit;
        private final Map<String, String[]> metadata = ImmutableMap.of();

        public Fetch(FetchItem fit) {
            this.fit = fit;
        }

        @Override
        protected void doRun() throws Exception {
            RateLimiter limiter = lastFetched.get(fit.queueID);
            if (limiter != null && !limiter.tryAcquire()) {
                // Cannot acquire permission, must wait
                // 1 sec delay is to prevent CPU hogging
                dispatcher.schedule(this, 1, TimeUnit.SECONDS);
                return;
            }

            Protocol protocol = protocolFactory.getProtocol(fit.u);
            final ListenableFuture<ProtocolResponse> responseFuture = protocol.getProtocolOutput(
                    fit.url, metadata);

            responseFuture.addListener(new VerboseRunnable() {
                @Override
                protected void doRun() {
                    try {
                        ProtocolResponse response = responseFuture.get();
                        LOG.info("[Fetcher #{}] Fetched {} with status {}",
                                taskIndex, fit.url, response.getStatusCode());


                        response.getMetadata().put(
                                "fetch.statusCode",
                                new String[]{Integer.toString(response
                                        .getStatusCode())});

                        // update the stats
                        // eventStats.scope("KB downloaded").update((long)
                        // content.length / 1024l);
                        // eventStats.scope("# pages").update(1);

                        // passes the input metadata if any to the response one
                        for (Entry<String, String[]> entry : metadata.entrySet()) {
                            response.getMetadata().put(entry.getKey(),
                                    entry.getValue());
                        }

                        // determine the status based on the status code
                        Status status = Status.fromHTTPCode(response
                                .getStatusCode());

                        // if the status is OK emit on default stream
                        if (status.equals(Status.FETCHED)) {
                            emitAndAck(fit.t, Utils.DEFAULT_STREAM_ID, fit.url, response.getContent(), new HashMap<String, String[]>(metadata));
                            markCount("fetched");
                        } else if (status.equals(Status.REDIRECTION)) {
                            emitAndAck(fit.t, STATUS_STREAM, fit.url, metadata, status);
                            markCount("redirection");
                            // find the URL it redirects to
                            String[] redirection = response.getMetadata().get(
                                    HttpHeaders.LOCATION);

                            if (allowRedirs && redirection.length != 0
                                    && redirection[0] != null) {
                                handleRedirect(fit.t, fit.url, redirection[0],
                                        metadata);
                            }

                        }
                        // error
                        else {
                            emitAndAck(fit.t, STATUS_STREAM, fit.url, response.getMetadata(), status);
                            markCount("error-http-status-" + response.getStatusCode());
                        }
                    } catch (Exception e) {
                        LOG.error("Exception while fetching {}", fit.url, e);

                        // add the reason of the failure in the metadata
                        metadata.put("fetch.exception",
                                new String[]{e.getMessage()});

                        // send to status stream
                        emitAndAck(fit.t, STATUS_STREAM, fit.url, metadata, Status.FETCH_ERROR);
                        markCount("error-exception-" + e.getClass().getSimpleName());
                    }
                }
            }, workers);
        }
    }
}
