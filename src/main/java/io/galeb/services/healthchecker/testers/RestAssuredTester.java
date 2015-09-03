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

    private final int defaultTimeout = 2000;

    private Optional<Logger> logger = Optional.empty();
    private HttpClientConfig httpClientConfig;

    private String url = null;
    private String host = null;
    private int statusCode = 0;
    private String body = null;

    public RestAssuredTester() {
        httpClientConfig = httpClientConfig()
                .setParam(ClientPNames.CONN_MANAGER_TIMEOUT, defaultTimeout)       // HttpConnectionManager connection return time
                .setParam(CoreConnectionPNames.CONNECTION_TIMEOUT, defaultTimeout) // Remote host connection time
                .setParam(CoreConnectionPNames.SO_TIMEOUT, defaultTimeout)         // Remote host response time
                .setParam(CoreConnectionPNames.STALE_CONNECTION_CHECK, true);      // Determines whether stale connection check is to be used
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
        return this;
    }

    @Override
    public synchronized boolean check() {
        RequestSpecification request;
        ValidatableResponse response;
        RedirectConfig redirectConfig = RestAssuredConfig.config().getRedirectConfig().followRedirects(false);
        RestAssuredConfig restAssuredConfig = RestAssuredConfig.config().redirect(redirectConfig);
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
