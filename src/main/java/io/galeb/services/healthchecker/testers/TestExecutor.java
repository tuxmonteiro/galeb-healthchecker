package io.galeb.services.healthchecker.testers;

import java.util.Optional;

import io.galeb.core.logging.Logger;
import io.galeb.core.model.*;
import io.galeb.services.healthchecker.sched.*;

public interface TestExecutor {

    default TestExecutor withJob(HealthCheckJob healthCheckJob) {
        return this;
    }

    TestExecutor withUrl(String fullPath);

    TestExecutor withHost(String host);

    TestExecutor withStatusCode(int statusCode);

    TestExecutor withBody(String body);

    public default TestExecutor setConnectionTimeOut(Integer timeout) {
        return this;
    };

    public default TestExecutor followRedirects(Boolean follow) {
        return this;
    };

    TestExecutor setLogger(Optional<Logger>  logger);

    TestExecutor reset();

    boolean check();

    TestExecutor setEntity(Entity entity);

    Entity getEntity();

}
