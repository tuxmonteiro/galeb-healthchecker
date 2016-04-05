package io.galeb.services.healthchecker.testers;

import io.galeb.core.model.Entity;

import javax.cache.Cache;

public interface TestExecutor {

    TestExecutor withUrl(String fullPath);

    TestExecutor withHost(String host);

    TestExecutor withStatusCode(int statusCode);

    TestExecutor withBody(String body);

    default TestExecutor setConnectionTimeOut(Integer timeout) {
        return this;
    };

    default TestExecutor followRedirects(Boolean follow) {
        return this;
    };

    default TestExecutor reset() { return this; }

    void check();

    default TestExecutor setEntity(Entity entity) { return this; }

    default TestExecutor setCache(Cache<String, String> cache) { return this; }

}
