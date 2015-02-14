package com.openvraas.services.healthchecker.sched;

import java.util.List;
import java.util.concurrent.ExecutionException;

import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import com.openvraas.core.controller.EntityController.Action;
import com.openvraas.core.logging.Logger;
import com.openvraas.core.model.Backend;
import com.openvraas.core.model.Backend.Health;
import com.openvraas.core.model.Farm;
import com.openvraas.hazelcast.IEventBus;
import com.openvraas.services.healthchecker.Tester;

@DisallowConcurrentExecution
public class HealthCheckJob implements Job {

    private Tester tester;
    private Logger logger;
    private Farm farm;
    private IEventBus eventbus;

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {

        final JobDataMap jobDataMap = context.getJobDetail().getJobDataMap();

        if (tester==null) {
            tester = (Tester) jobDataMap.get("tester");
        }
        if (logger==null) {
            logger = (Logger) jobDataMap.get("logger");
        }
        if (farm==null) {
            farm = (Farm) jobDataMap.get("farm");
        }
        if (eventbus==null) {
            eventbus = (IEventBus) jobDataMap.get("eventbus");
        }

        List<Backend> backends = farm.getBackends();
        for (Backend backend : backends) {
            String url = backend.getId();
            String returnType = "string";
            Backend.Health lastHealth = backend.getHealth();

            String healthCheckPath = (String) backend.getProperties().get("hcPath");
            if (healthCheckPath==null) {
                healthCheckPath = "/";
            }

            String fullUrl = url + healthCheckPath;

            String expectedReturn = (String) backend.getProperties().get("hcExpectedReturn");
            if (expectedReturn==null) {
                returnType = "httpCode200";
                expectedReturn="OK";
            }

            boolean isOk = false;
            try {
                isOk = tester.withUrl(fullUrl)
                             .withHealthCheckPath(healthCheckPath)
                             .withReturn(returnType, expectedReturn)
                             .connect();

            } catch (RuntimeException | InterruptedException | ExecutionException e) {
                logger.debug(e);
            }

            if (isOk) {
                backend.setHealth(Health.HEALTHY);
                loggerDebug(url+" is OK");
            } else {
                backend.setHealth(Health.DEADY);
                loggerDebug(url+" FAIL");
            }
            if (backend.getHealth()!=lastHealth) {
                eventbus.publishEntity(backend, Backend.class.getSimpleName().toLowerCase(), Action.CHANGE);
            }

        }

        loggerDebug("Job HealthCheck done.");

    }

    private void loggerDebug(String message) {
        if (logger!=null) {
            logger.debug(message);
        }
    }

}
