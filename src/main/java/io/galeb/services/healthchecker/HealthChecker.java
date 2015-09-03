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

package io.galeb.services.healthchecker;

import static org.quartz.JobBuilder.newJob;
import static org.quartz.SimpleScheduleBuilder.simpleSchedule;
import static org.quartz.TriggerBuilder.newTrigger;

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

import io.galeb.core.services.AbstractService;
import io.galeb.services.healthchecker.sched.HealthCheckJob;
import io.galeb.services.healthchecker.testers.RestAssuredTester;

public class HealthChecker extends AbstractService implements JobListener {

    private static final String PROP_HEALTHCHECKER_PREFIX     = HealthChecker.class.getPackage().getName()+".";

    private static final String PROP_HEALTHCHECKER_INTERVAL   = PROP_HEALTHCHECKER_PREFIX+"interval";

    public static final String PROP_HEALTHCHECKER_DEF_PATH    = PROP_HEALTHCHECKER_PREFIX+"defpath";

    public static final String PROP_HEALTHCHECKER_DEF_BODY    = PROP_HEALTHCHECKER_PREFIX+"defBody";

    public static final String PROP_HEALTHCHECKER_DEF_STATUS  = PROP_HEALTHCHECKER_PREFIX+"defstatus";

    public static final String PROP_HEALTHCHECKER_DEF_TIMEOUT = PROP_HEALTHCHECKER_PREFIX+"defTimeOut";

    public static final String PROP_HEALTHCHECKER_DEF_FOLLOW  = PROP_HEALTHCHECKER_PREFIX+"defFollowRedirects";

    public static final String TESTER_NAME = "tester";

    private Scheduler scheduler;

    static {
        if (System.getProperty(PROP_HEALTHCHECKER_INTERVAL)==null) {
            System.setProperty(PROP_HEALTHCHECKER_INTERVAL, "10");
        }
    }

    @PostConstruct
    public void init() {
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
                jobdataMap.put(TESTER_NAME, new RestAssuredTester());
                jobdataMap.put(AbstractService.FARM, farm);
                jobdataMap.put(AbstractService.LOGGER, logger);
                jobdataMap.put(AbstractService.DISTRIBUTEDMAP, distributedMap);

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

}
