package com.openvraas.services.healthchecker;

import java.io.IOException;
import java.nio.CharBuffer;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.AuthSchemes;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.nio.IOControl;
import org.apache.http.nio.client.methods.AsyncCharConsumer;
import org.apache.http.nio.client.methods.HttpAsyncMethods;
import org.apache.http.nio.protocol.HttpAsyncRequestProducer;
import org.apache.http.protocol.HttpContext;

public class Tester {

    private String url;
    private String healthCheckPath;
    private String returnType;
    private String expectedReturn;
    private int defaultTimeout = 5000;
    private boolean isOk = false;

    public Tester connect(String url) {
        this.url = url;
        return this;
    }

    public Tester withHealthCheckPath(String healthCheckPath) {
        this.healthCheckPath = healthCheckPath;
        return this;
    }

    public Tester withReturn(String returnType, String expectedReturn) {
        this.returnType = returnType;
        this.expectedReturn = expectedReturn;
        return this;
    }

    public boolean run() throws RuntimeException, InterruptedException, ExecutionException {
        if (url==null||healthCheckPath==null||returnType==null||expectedReturn==null) {
            return false;
        }
        CloseableHttpAsyncClient httpclient = HttpAsyncClients.createDefault();
        try {
            httpclient.start();
            final CountDownLatch latch = new CountDownLatch(1);
            HttpGet request = new HttpGet(url);
            HttpAsyncRequestProducer producer = HttpAsyncMethods.create(request);
            RequestConfig defaultRequestConfig = RequestConfig.custom()
                    .setCookieSpec(CookieSpecs.BEST_MATCH)
                    .setExpectContinueEnabled(true)
                    .setStaleConnectionCheckEnabled(true)
                    .setTargetPreferredAuthSchemes(Arrays.asList(AuthSchemes.NTLM, AuthSchemes.DIGEST))
                    .setProxyPreferredAuthSchemes(Arrays.asList(AuthSchemes.BASIC))
                    .build();
            RequestConfig requestConfig = RequestConfig.copy(defaultRequestConfig)
                    .setSocketTimeout(defaultTimeout)
                    .setConnectTimeout(defaultTimeout)
                    .setConnectionRequestTimeout(defaultTimeout)
                    .build();
            request.setConfig(requestConfig);
            AsyncCharConsumer<HttpResponse> consumer = new AsyncCharConsumer<HttpResponse>() {

                HttpResponse response;

                @Override
                protected void onCharReceived(CharBuffer buf, IOControl iocontrol)
                        throws IOException {
                }

                @Override
                protected HttpResponse buildResult(HttpContext context)
                        throws Exception {
                    return this.response;
                }

                @Override
                protected void onResponseReceived(HttpResponse response)
                        throws HttpException, IOException {
                    this.response = response;
                }

            };
            httpclient.execute(producer, consumer, new FutureCallback<HttpResponse>() {

                @Override
                public void cancelled() {
                    latch.countDown();
                }

                @Override
                public void completed(HttpResponse response) {
                    latch.countDown();

                    isOk = true;

                }

                @Override
                public void failed(Exception e) {
                    latch.countDown();

                }

            });
            latch.await();
        } finally {
            try {
                httpclient.close();
            } catch (IOException ignore) {
            }
        }
        return isOk;
    }

}
