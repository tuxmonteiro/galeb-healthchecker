package com.openvraas.services.healthchecker;

import static org.quartz.JobBuilder.*;
import static org.quartz.TriggerBuilder.*;
import static org.quartz.SimpleScheduleBuilder.*;

import java.util.UUID;

import javax.annotation.PostConstruct;

import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobListener;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.impl.StdSchedulerFactory;

import com.openvraas.services.AbstractService;
import com.openvraas.services.healthchecker.sched.HealthCheckJob;

public class HealthChecker extends AbstractService implements JobListener {

    private static final String PROP_HEALTHCHECKER_PREFIX   = "com.openvraas.healthcheck.";

    private static final String PROP_HEALTHCHECKER_INTERVAL = PROP_HEALTHCHECKER_PREFIX+"interval";

    private Scheduler scheduler;

    static {
        if (System.getProperty(PROP_HEALTHCHECKER_INTERVAL)==null) {
            System.setProperty(PROP_HEALTHCHECKER_INTERVAL, "10");
        }
    }

    @PostConstruct
    protected void init() {

        super.prelaunch();

        setupScheduler();

        startHealthCheckJob();

        onLog("DEBUG", "ready");

    }

    private void setupScheduler() {
        try {
            scheduler = new StdSchedulerFactory().getScheduler();
            scheduler.getListenerManager().addJobListener(this);

            scheduler.start();
        } catch (SchedulerException e) {
            logger.error(e);
        }
    }

    private void startHealthCheckJob() {
        try {
            if (scheduler.isStarted()) {

                int interval = Integer.parseInt(System.getProperty(PROP_HEALTHCHECKER_INTERVAL));
                Trigger trigger = newTrigger().withIdentity(UUID.randomUUID().toString())
                                              .startNow()
                                              .withSchedule(simpleSchedule().withIntervalInSeconds(interval).repeatForever())
                                              .build();

                JobDataMap jobdataMap = new JobDataMap();
                jobdataMap.put("tester", new Tester());
                jobdataMap.put("farm", farm);
                jobdataMap.put("logger", logger);
                jobdataMap.put("eventbus", eventbus);

                JobDetail healthCheckJob = newJob(HealthCheckJob.class).withIdentity(HealthCheckJob.class.getName())
                                                                       .setJobData(jobdataMap)
                                                                       .build();


                scheduler.scheduleJob(healthCheckJob, trigger);

            }
        } catch (SchedulerException e) {
            logger.error(e);
        }
    }

    @Override
    public String getName() {
        return this.toString();
    }

    @Override
    public void jobToBeExecuted(JobExecutionContext context) {
        logger.debug(context.getJobDetail().getKey().getName()+" to be executed");
    }

    @Override
    public void jobExecutionVetoed(JobExecutionContext context) {
        logger.debug(context.getJobDetail().getKey().getName()+" vetoed");
    }

    @Override
    public void jobWasExecuted(JobExecutionContext context,
            JobExecutionException jobException) {
        logger.debug(context.getJobDetail().getKey().getName()+" was executed");
    }

}
