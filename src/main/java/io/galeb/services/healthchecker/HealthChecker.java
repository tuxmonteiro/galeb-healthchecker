package io.galeb.services.healthchecker;

import static org.quartz.JobBuilder.newJob;
import static org.quartz.SimpleScheduleBuilder.simpleSchedule;
import static org.quartz.TriggerBuilder.newTrigger;
import io.galeb.core.controller.EntityController.Action;
import io.galeb.core.json.JsonObject;
import io.galeb.core.services.AbstractService;
import io.galeb.services.healthchecker.sched.HealthCheckJob;

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

public class HealthChecker extends AbstractService implements JobListener {

    private static final String PROP_HEALTHCHECKER_PREFIX   = "io.galeb.healthcheck.";

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

        logger.debug(String.format("%s ready", toString()));
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
        return toString();
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

    @Override
    public void handleController(JsonObject json, Action action) {
        // future
    }

}
