package io.galeb.services.healthchecker.testers;

import static com.jayway.restassured.RestAssured.with;
import static com.jayway.restassured.config.HttpClientConfig.httpClientConfig;
import static org.hamcrest.Matchers.containsString;

import java.util.Optional;

import org.apache.http.HttpHeaders;
import org.apache.http.client.params.ClientPNames;
import org.apache.http.params.CoreConnectionPNames;

import com.jayway.restassured.config.HttpClientConfig;
import com.jayway.restassured.config.RedirectConfig;
import com.jayway.restassured.config.RestAssuredConfig;
import com.jayway.restassured.response.Header;
import com.jayway.restassured.response.ValidatableResponse;
import com.jayway.restassured.specification.RequestSpecification;

import io.galeb.core.logging.Logger;

@SuppressWarnings("deprecation")
public class RestAssuredTester implements TestExecutor {

    private Optional<Logger> logger = Optional.empty();

    private String url = null;
    private String host = null;
    private int statusCode = 0;
    private String body = null;
    private boolean followRedirects = false;
    private HttpClientConfig httpClientConfig = null;

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
    public TestExecutor connectTimeOut(Integer timeout) {
        if (timeout != null) {
            httpClientConfig = httpClientConfig()
                    .setParam(ClientPNames.CONN_MANAGER_TIMEOUT, Long.valueOf(timeout))
                    .setParam(CoreConnectionPNames.CONNECTION_TIMEOUT, timeout)
                    .setParam(CoreConnectionPNames.SO_TIMEOUT, timeout)
                    .setParam(CoreConnectionPNames.STALE_CONNECTION_CHECK, true);
        }
        return this;
    }

    @Override
    public TestExecutor setLogger(Optional<Logger> logger) {
        this.logger = logger;
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
    public synchronized boolean check() {
        RequestSpecification request;
        ValidatableResponse response;
        RedirectConfig redirectConfig = RestAssuredConfig.config().getRedirectConfig().followRedirects(followRedirects);
        RestAssuredConfig restAssuredConfig = RestAssuredConfig.config().redirect(redirectConfig);

        if (httpClientConfig == null) {
            httpClientConfig = httpClientConfig()
                    .setParam(ClientPNames.CONN_MANAGER_TIMEOUT, 5000L)
                    .setParam(CoreConnectionPNames.CONNECTION_TIMEOUT, 5000)
                    .setParam(CoreConnectionPNames.SO_TIMEOUT, 5000)
                    .setParam(CoreConnectionPNames.STALE_CONNECTION_CHECK, true);
        }

        restAssuredConfig.httpClient(httpClientConfig);

        request = with().config(restAssuredConfig);
        if (host != null && !"".equals(host)) {
            Header headerHost = new Header(HttpHeaders.HOST, host);
            request.header(headerHost);
        }

        try {
            response = request.get(url).then();
            if (statusCode > 0) {
                response.statusCode(statusCode);
                logger.ifPresent(log -> log.debug(url+" > STATUS CODE MATCH ("+statusCode+")"));
            } else {
                logger.ifPresent(log -> log.warn(url+" >>> STATUS CODE NOT MATCH ("+statusCode+")"));
            }
            if (body != null && !"".equals(body)) {
                response.body(containsString(body));
                logger.ifPresent(log -> log.debug(url+" > BODY MATCH ("+body+")"));
            } else {
                logger.ifPresent(log -> log.warn(url+" >>> BODY NOT MATCH ("+body+")"));
            }
        } catch (Exception e) {
            return false;
        }

        return true;
    }

}
