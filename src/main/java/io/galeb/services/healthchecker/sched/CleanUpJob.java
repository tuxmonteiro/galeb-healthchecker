/*
 *  Galeb - Load Balance as a Service Plataform
 *
 *  Copyright (C) 2014-2016 Globo.com
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.galeb.services.healthchecker.sched;

import io.galeb.core.logging.Logger;
import io.galeb.core.services.AbstractService;
import io.galeb.services.healthchecker.HealthChecker;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Future;

@DisallowConcurrentExecution
public class CleanUpJob implements Job {

    private Map<String, Future> futureMap;

    private Optional<Logger> logger = Optional.empty();

    @SuppressWarnings("unchecked")
    public void init(JobDataMap jobDataMap) {
        if (!logger.isPresent()) {
            logger = Optional.ofNullable((Logger) jobDataMap.get(AbstractService.LOGGER));
        }
        if (futureMap == null) {
            futureMap = (Map<String, Future>) jobDataMap.get(HealthChecker.FUTURE_MAP);
        }
    }

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        init(context.getJobDetail().getJobDataMap());
        long start = System.currentTimeMillis();
        Set<Map.Entry<String, Future>> entries = futureMap.entrySet();
        int initialSize = entries.size();
        entries.stream()
                .filter(entry -> entry.getValue() == null || entry.getValue().isDone() || entry.getValue().isCancelled())
                .map(Map.Entry::getKey).forEach(futureMap::remove);
        int totalRemoved = initialSize - entries.size();
        long end = System.currentTimeMillis();
        logger.ifPresent(log -> log.info(CleanUpJob.class.getSimpleName() + ": removed " +
            totalRemoved + (totalRemoved > 1 ? " entries" : " entry") + " (" + (end - start) + " ms)"));
    }
}
