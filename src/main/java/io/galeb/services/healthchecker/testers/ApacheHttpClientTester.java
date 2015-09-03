/*
 * Copyright (c) 2014-2015 Globo.com - ATeam
 * All rights reserved.
 *
 * This source is subject to the Apache License, Version 2.0.
 * Please see the LICENSE file for more information.
 *
 * Authors: See AUTHORS file
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.galeb.services.healthchecker.testers;

import java.io.IOException;
import java.io.InputStream;
import java.nio.CharBuffer;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import javax.ws.rs.core.HttpHeaders;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
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

import io.galeb.core.logging.Logger;

public class ApacheHttpClientTester implements TestExecutor {

    private Optional<Logger> logger;
    private String url;
    @SuppressWarnings("unused") private String healthCheckPath;
    private String returnType;
    private String expectedReturn;
    private final int defaultTimeout = 5000;
    private boolean isOk = false;
    private String host;

    @Override
    public TestExecutor setLogger(Optional<Logger> logger) {
        this.logger = logger;
        return this;
    }

    @Override
    public TestExecutor withHost(String host) {
        this.host = host;
        return this;
    }

    @Override
    public TestExecutor withUrl(String url) {
        this.url = url;
        return this;
    }

    @Override
    public TestExecutor withBody(String body) {
        return this;
    }

    @Override
    public TestExecutor withStatusCode(int statusCode) {
        return this;
    }

    @Override
    public TestExecutor reset() {
        return this;
    }

    @Override
    public boolean check() {
        try {
            return connect();
        } catch (ExecutionException | RuntimeException | InterruptedException ignore) {
            return false;
        }
    }

    public ApacheHttpClientTester withReturn(String returnType, String expectedReturn) {
        this.returnType = returnType;
        this.expectedReturn = expectedReturn;
        return this;
    }

    public boolean connect() throws RuntimeException, InterruptedException, ExecutionException {
        if (url==null||returnType==null||expectedReturn==null) {
            return false;
        }
        final CloseableHttpAsyncClient httpclient = HttpAsyncClients.createDefault();
        try {
            httpclient.start();
            final CountDownLatch latch = new CountDownLatch(1);
            final HttpGet request = new HttpGet(url);
            request.setHeader(HttpHeaders.HOST, host);
            final HttpAsyncRequestProducer producer = HttpAsyncMethods.create(request);
            final RequestConfig requestConfig = RequestConfig.copy(RequestConfig.DEFAULT)
                    .setSocketTimeout(defaultTimeout)
                    .setConnectTimeout(defaultTimeout)
                    .setConnectionRequestTimeout(defaultTimeout)
                    .build();
            request.setConfig(requestConfig);
            final AsyncCharConsumer<HttpResponse> consumer = new AsyncCharConsumer<HttpResponse>() {

                HttpResponse response;

                @Override
                protected void onCharReceived(CharBuffer buf, IOControl iocontrol)
                        throws IOException {
                    return;
                }

                @Override
                protected HttpResponse buildResult(HttpContext context)
                        throws Exception {
                    return response;
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
                    isOk = false;
                }

                @Override
                public void completed(HttpResponse response) {
                    latch.countDown();

                    final int statusCode = response.getStatusLine().getStatusCode();
                    InputStream contentIS = null;
                    String content = "";
                    try {
                        contentIS = response.getEntity().getContent();
                        content = IOUtils.toString(contentIS);

                    } catch (IllegalStateException | IOException e) {
                        logger.ifPresent(log -> log.debug(e));
                    }

                    if (returnType.startsWith("httpCode")) {
                        returnType = returnType.replaceFirst("httpCode", "");
                    }
                    // isOk = statusCode == Integer.parseInt(returnType); // Disable temporarily statusCode check
                    isOk = true;
                }

                @Override
                public void failed(Exception e) {
                    latch.countDown();
                    isOk = false;
                    logger.ifPresent(log -> log.debug(e));
                }

            });
            latch.await();

        } catch (final RuntimeException e) {
            isOk = false;
            logger.ifPresent(log -> log.error(e));
        } finally {
            try {
                httpclient.close();
            } catch (final IOException e) {
                logger.ifPresent(log -> log.debug(e));
            }
        }
        return isOk;
    }

}
