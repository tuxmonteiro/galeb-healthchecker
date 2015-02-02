package com.openvraas.services.healthchecker.sched;

import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import com.openvraas.core.logging.Logger;

@DisallowConcurrentExecution
public class HealthCheckJob implements Job {

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {

        JobDataMap jobDataMap = context.getJobDetail().getJobDataMap();

        Logger logger = (Logger) jobDataMap.get("logger");

        if (logger!=null) {
            logger.debug("Hello!  HelloJob is executing.");
        }
    }

}
