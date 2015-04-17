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

package com.digitalpebble.storm.crawler.protocol.httpclient;

import java.io.IOException;
import java.util.Locale;

import org.apache.commons.lang.StringUtils;
import org.apache.http.Header;
import org.apache.http.HeaderIterator;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.DefaultProxyRoutePlanner;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;

import com.digitalpebble.storm.crawler.Metadata;
import com.digitalpebble.storm.crawler.protocol.AbstractHttpProtocol;
import com.digitalpebble.storm.crawler.protocol.HttpRobotRulesParser;
import com.digitalpebble.storm.crawler.protocol.ProtocolResponse;
import com.digitalpebble.storm.crawler.protocol.RobotRulesParser;
import com.digitalpebble.storm.crawler.util.ConfUtils;

import crawlercommons.robots.BaseRobotRules;

/**
 * Uses Apache httpclient to handle http and https
 **/

public class HttpProtocol extends AbstractHttpProtocol implements
        ResponseHandler<ProtocolResponse> {

    private static final org.slf4j.Logger LOG = LoggerFactory
            .getLogger(HttpProtocol.class);

    private final static PoolingHttpClientConnectionManager CONNECTION_MANAGER = new PoolingHttpClientConnectionManager();
    static {
        // Increase max total connection to 200
        CONNECTION_MANAGER.setMaxTotal(200);
        // Increase default max connection per route to 20
        CONNECTION_MANAGER.setDefaultMaxPerRoute(20);
    }

    private String userAgent;

    private com.digitalpebble.storm.crawler.protocol.HttpRobotRulesParser robots;

    /**
     * TODO record response time in the meta data, see property
     * http.store.responsetime.
     */
    private boolean responseTime = true;

    // TODO
    private int maxContent;

    /** The network timeout in millisecond */
    private int timeout = 30000;

    private boolean skipRobots = false;

    /** The proxy hostname. */
    private String proxyHost = null;

    /** The proxy port. */
    private int proxyPort = 8080;

    /** Indicates if a proxy is used */
    private boolean useProxy = false;

    @Override
    public ProtocolResponse getProtocolOutput(String url, Metadata md)
            throws Exception {

        HttpClientBuilder builder = HttpClients.custom()
                .setUserAgent(userAgent)
                .setConnectionManager(CONNECTION_MANAGER);

        // use a proxy?
        if (useProxy) {
            HttpHost proxy = new HttpHost(proxyHost, proxyPort);
            DefaultProxyRoutePlanner routePlanner = new DefaultProxyRoutePlanner(
                    proxy);
            builder.setRoutePlanner(routePlanner);
        }

        LOG.debug("HTTP connection manager stats "
                + CONNECTION_MANAGER.getTotalStats().toString());

        CloseableHttpClient httpclient = builder.build();

        RequestConfig requestConfig = RequestConfig.custom()
                .setSocketTimeout(timeout).setConnectTimeout(timeout).build();

        HttpGet httpget = new HttpGet(url);

        if (md != null) {
            String ifModifiedSince = md.getFirstValue("cachedLastModified");
            if (StringUtils.isNotBlank(ifModifiedSince)) {
                httpget.addHeader("If-Modified-Since", ifModifiedSince);
            }

            String ifNoneMatch = md.getFirstValue("cachedEtag");
            if (StringUtils.isNotBlank(ifNoneMatch)) {
                httpget.addHeader("If-None-Match", ifNoneMatch);
            }
        }

        httpget.setConfig(requestConfig);

        // no need to release the connection explicitly as this is handled
        // automatically
        return httpclient.execute(httpget, this);
    }

    @Override
    public ProtocolResponse handleResponse(HttpResponse response)
            throws ClientProtocolException, IOException {
        int status = response.getStatusLine().getStatusCode();
        Metadata metadata = new Metadata();
        HeaderIterator iter = response.headerIterator();
        while (iter.hasNext()) {
            Header header = iter.nextHeader();
            metadata.addValue(header.getName().toLowerCase(Locale.ROOT),
                    header.getValue());
        }
        // TODO find a way of limiting by maxContent
        byte[] bytes = EntityUtils.toByteArray(response.getEntity());
        return new ProtocolResponse(bytes, status, metadata);
    }

    @Override
    public BaseRobotRules getRobotRules(String url) {
        if (this.skipRobots)
            return RobotRulesParser.EMPTY_RULES;
        return robots.getRobotRulesSet(this, url);
    }

    @Override
    public void configure(final Config conf) {
        this.timeout = ConfUtils.getInt(conf, "http.timeout", 10000);

        this.proxyHost = ConfUtils.getString(conf, "http.proxy.host", null);
        this.proxyPort = ConfUtils.getInt(conf, "http.proxy.port", 8080);
        this.useProxy = (proxyHost != null && proxyHost.length() > 0);

        this.maxContent = ConfUtils.getInt(conf, "http.content.limit",
                64 * 1024);
        this.userAgent = getAgentString(
                ConfUtils.getString(conf, "http.agent.name"),
                ConfUtils.getString(conf, "http.agent.version"),
                ConfUtils.getString(conf, "http.agent.description"),
                ConfUtils.getString(conf, "http.agent.url"),
                ConfUtils.getString(conf, "http.agent.email"));

        this.responseTime = ConfUtils.getBoolean(conf,
                "http.store.responsetime", true);

        this.skipRobots = ConfUtils.getBoolean(conf, "http.skip.robots", false);

        robots = new HttpRobotRulesParser(conf);
    }

    public static void main(String args[]) throws Exception {
        HttpProtocol protocol = new HttpProtocol();

        String url = args[0];
        Config conf = ConfUtils.loadConf(args[1]);

        protocol.configure(conf);

        if (!protocol.skipRobots) {
            BaseRobotRules rules = protocol.getRobotRules(url);
            System.out.println("is allowed : " + rules.isAllowed(url));
        }

        Metadata md = new Metadata();
        ProtocolResponse response = protocol.getProtocolOutput(url, md);
        System.out.println(url);
        System.out.println(response.getMetadata());
        System.out.println(response.getStatusCode());
        System.out.println(response.getContent().length);
    }

}
