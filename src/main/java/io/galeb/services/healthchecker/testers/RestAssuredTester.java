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

import static com.jayway.restassured.RestAssured.with;
import static com.jayway.restassured.config.HttpClientConfig.httpClientConfig;
import static io.galeb.services.healthchecker.HealthChecker.HEALTHCHECKER_USERAGENT;
import static org.hamcrest.Matchers.containsString;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import com.jayway.restassured.response.Header;
import com.jayway.restassured.response.ValidatableResponse;
import com.jayway.restassured.specification.RequestSpecification;
import io.galeb.core.json.JsonObject;
import io.galeb.core.model.Backend;
import io.galeb.core.model.Entity;
import org.apache.http.HttpHeaders;
import org.apache.http.client.params.ClientPNames;
import org.apache.http.params.CoreConnectionPNames;

import com.jayway.restassured.config.HttpClientConfig;
import com.jayway.restassured.config.RedirectConfig;
import com.jayway.restassured.config.RestAssuredConfig;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.cache.Cache;

public class RestAssuredTester implements TestExecutor {

    private static final Logger LOGGER = LogManager.getLogger();

    private String url = null;
    private String host = null;
    private int statusCode = 0;
    private String body = null;
    private boolean followRedirects = false;
    private HttpClientConfig httpClientConfig = null;
    private int connectionTimeout = 5000;
    private Entity entity = null;
    private Cache<String, String> cache;

    @Override
    public TestExecutor setCache(final Cache<String, String> cache) {
        this.cache = cache;
        return this;
    }

    @Override
    public TestExecutor setEntity(Entity entity) {
        this.entity = entity;
        return this;
    }

    @Override
    public TestExecutor withUrl(String url) {
        this.url = url;
        return this;
    }

    @Override
    public TestExecutor withHost(String host) {
        this.host = host;
        return this;
    }

    @Override
    public TestExecutor withStatusCode(int statusCode) {
        this.statusCode = statusCode;
        return this;
    }

    @Override
    public TestExecutor withBody(String body) {
        this.body = body;
        return this;
    }

    @Override
    public TestExecutor followRedirects(Boolean follow) {
        if (follow != null) {
            followRedirects = follow;
        }
        return this;
    }

    @Override
    public TestExecutor setConnectionTimeOut(Integer timeout) {
        if (timeout != null) {
            connectionTimeout = timeout;
            final Map<String, Object> conf = new HashMap<>();
            conf.put(ClientPNames.CONN_MANAGER_TIMEOUT, Long.valueOf(timeout));
            conf.put(CoreConnectionPNames.CONNECTION_TIMEOUT, timeout);
            conf.put(CoreConnectionPNames.SO_TIMEOUT, timeout);
            conf.put(CoreConnectionPNames.STALE_CONNECTION_CHECK, true);
            conf.put("CONNECTION_MANAGER_TIMEOUT", timeout);
            httpClientConfig = httpClientConfig().withParams(conf);
        }
        return this;
    }

    @Override
    public TestExecutor reset() {
        url = null;
        host = null;
        statusCode = 0;
        body = null;
        httpClientConfig = null;
        return this;
    }

    @Override
    public void check() {
        RequestSpecification request;
        ValidatableResponse response = null;
        RedirectConfig redirectConfig = RestAssuredConfig.config().getRedirectConfig().followRedirects(followRedirects);
        RestAssuredConfig restAssuredConfig = RestAssuredConfig.config().redirect(redirectConfig);

        if (httpClientConfig == null) {
            setConnectionTimeOut(connectionTimeout);
        }

        restAssuredConfig.httpClient(httpClientConfig);

        request = with().config(restAssuredConfig);
        if (host != null && !"".equals(host)) {
            Header headerHost = new Header(HttpHeaders.HOST, host);
            request.header(headerHost);
        }
        Header userAgent = new Header(HttpHeaders.USER_AGENT, HEALTHCHECKER_USERAGENT);
        request.header(userAgent);

        final ExecutorService executor = Executors.newWorkStealingPool(1);
        Future<ValidatableResponse> future = null;

        try {
            future = executor.submit(new Task(request, url));
        } catch (RejectedExecutionException e) {
            LOGGER.debug(e);
        }

        try {
            if (future != null) {
                response = future.get(connectionTimeout, TimeUnit.MILLISECONDS);
            } else {
                LOGGER.warn(url+" >>> NOT RUN - Task problem");
            }
        } catch (Exception e) {
            if (future != null) {
                future.cancel(true);
            }
            String tempMessage = e.getMessage();
            if (tempMessage == null) {
                tempMessage = "Connection Timeout ("+connectionTimeout+" ms)";
            }
            final String message = tempMessage;
            LOGGER.warn(url+" >>> Backend FAIL ("+message+")");
        } finally {
            executor.shutdownNow();
        }
        if (response == null) {
            notifyHealthOnCheck(false);
            return;
        }
        if (statusCode > 0) {
            try {
                response.statusCode(statusCode);
                LOGGER.debug(url+" > STATUS CODE MATCH ("+statusCode+")");
            } catch (AssertionError e) {
                LOGGER.warn(url+" >>> STATUS CODE NOT MATCH ("+statusCode+")");
                notifyHealthOnCheck(false);
                return;
            }
        }
        if (body != null && !"".equals(body)) {
            try {
                response.body(containsString(body));
                LOGGER.debug(url+" > BODY MATCH ("+body+")");
            } catch (AssertionError e) {
                LOGGER.warn(url+" >>> BODY NOT MATCH ("+body+")");
                notifyHealthOnCheck(false);
                return;
            }
        }
        notifyHealthOnCheck(true);
    }

    class Task implements Callable<ValidatableResponse> {

        private final RequestSpecification request;
        private final String url;

        public Task(final RequestSpecification request, String url) {
            this.request = request;
            this.url = url;
        }

        @Override
        public ValidatableResponse call() throws Exception {
            return request.get(url).then();
        }
    }

    private void notifyHealthOnCheck(boolean isOk) {
        if (entity instanceof Backend) {
            Backend backend = (Backend)entity;
            Backend.Health lastHealth = backend.getHealth();
            LOGGER.debug("Last Health " + entity.compoundId() + " is "+ lastHealth.toString());
            if (isOk) {
                backend.setHealth(Backend.Health.HEALTHY);
            } else {
                backend.setHealth(Backend.Health.DEAD);
            }
            if (backend.getHealth()!=lastHealth) {
                LOGGER.debug("New Health " + entity.compoundId() + " is "+ backend.getHealth().toString());
                String hostWithPort = backend.getId();
                if (isOk) {
                    LOGGER.info(hostWithPort+" is OK");
                } else {
                    LOGGER.warn(hostWithPort+" is FAILED");
                }
                cache.replace(entity.compoundId(), JsonObject.toJsonString(backend));
            }
        } else {
            LOGGER.warn("Entity is NOT Backend");
        }
    }

}
