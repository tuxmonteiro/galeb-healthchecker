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

package io.galeb.services.healthchecker.sched;

import static io.galeb.core.model.BackendPool.PROP_HEALTHCHECK_CODE;
import static io.galeb.core.model.BackendPool.PROP_HEALTHCHECK_HOST;
import static io.galeb.core.model.BackendPool.PROP_HEALTHCHECK_PATH;
import static io.galeb.core.model.BackendPool.PROP_HEALTHCHECK_RETURN;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import io.galeb.core.cluster.DistributedMap;
import io.galeb.core.logging.Logger;
import io.galeb.core.model.Backend;
import io.galeb.core.model.Backend.Health;
import io.galeb.core.model.BackendPool;
import io.galeb.core.model.Entity;
import io.galeb.core.model.Farm;
import io.galeb.core.model.Rule;
import io.galeb.core.model.collections.BackendPoolCollection;
import io.galeb.core.services.AbstractService;
import io.galeb.services.healthchecker.HealthChecker;
import io.galeb.services.healthchecker.testers.TestExecutor;

@DisallowConcurrentExecution
public class HealthCheckJob implements Job {

    private TestExecutor tester;
    private Optional<Logger> logger = Optional.empty();
    private Farm farm;
    private DistributedMap<String, Entity> distributedMap;

    @SuppressWarnings("unchecked")
    private void init(final JobDataMap jobDataMap) {
        if (!logger.isPresent()) {
            logger = Optional.ofNullable((Logger) jobDataMap.get(AbstractService.LOGGER));
        }
        if (tester==null) {
            tester = (TestExecutor) jobDataMap.get(HealthChecker.TESTER_NAME);
            tester.setLogger(logger);
        }
        if (farm==null) {
            farm = (Farm) jobDataMap.get(AbstractService.FARM);
        }
        if (distributedMap==null) {
            distributedMap = (DistributedMap<String, Entity>) jobDataMap.get(AbstractService.DISTRIBUTEDMAP);
        }
    }

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {

        init(context.getJobDetail().getJobDataMap());

        logger.ifPresent(log -> log.info("=== " + this.getClass().getSimpleName() + " ==="));

        final BackendPoolCollection backendPoolCollection = (BackendPoolCollection) farm.getCollection(BackendPool.class);
        backendPoolCollection.stream().forEach(pool -> {
            checkBackendPool(pool, getProperties(pool));
        });

        logger.ifPresent(log -> log.debug("Job HealthCheck done."));

    }

    private void notifyHealthOnCheck(Entity entity, Health lastHealth, boolean isOk) {
        if (entity instanceof Backend) {
            Backend backend = (Backend)entity;
            if (isOk) {
                backend.setHealth(Health.HEALTHY);
            } else {
                backend.setHealth(Health.DEADY);
            }
            if (backend.getHealth()!=lastHealth) {
                String hostWithPort = backend.getId();
                if (isOk) {
                    logger.ifPresent(log -> log.info(hostWithPort+" is OK"));
                } else {
                    logger.ifPresent(log -> log.warn(hostWithPort+" is FAILED"));
                }
                distributedMap.getMap(Backend.class.getName()).put(hostWithPort, backend);
            }
        }
    }

    private Map<String, Object> getProperties(final Entity pool) {
        final Map<String, Object> properties = new HashMap<>(pool.getProperties());
        final String hcBody = Optional.ofNullable((String) properties.get(PROP_HEALTHCHECK_RETURN))
                                        .orElse(System.getProperty(HealthChecker.PROP_HEALTHCHECKER_DEF_BODY, ""));
        final String hcPath = Optional.ofNullable((String) properties.get(PROP_HEALTHCHECK_PATH))
                                        .orElse(System.getProperty(HealthChecker.PROP_HEALTHCHECKER_DEF_PATH, "/"));
        final String hcHost = Optional.ofNullable((String) properties.get(PROP_HEALTHCHECK_HOST))
                                        .orElse(getHost(pool));
        final String hcStatusCode = Optional.ofNullable((String) properties.get(PROP_HEALTHCHECK_CODE))
                                        .orElse("0");
        properties.put(PROP_HEALTHCHECK_RETURN, hcBody);
        properties.put(PROP_HEALTHCHECK_PATH, hcPath);
        properties.put(PROP_HEALTHCHECK_HOST, hcHost);
        try {
            properties.put(PROP_HEALTHCHECK_CODE, Integer.parseInt(hcStatusCode));
        } catch (Exception e) {
            properties.put(PROP_HEALTHCHECK_CODE, 0);
        }
        return Collections.unmodifiableMap(properties);
    }

    private void checkBackendPool(final Entity pool, final Map<String, Object> properties) {

        final String hcBody = (String) properties.get(PROP_HEALTHCHECK_RETURN);
        final String hcPath = (String) properties.get(PROP_HEALTHCHECK_PATH);
        final String hcHost = (String) properties.get(PROP_HEALTHCHECK_HOST);
        final int statusCode = (int) properties.get(PROP_HEALTHCHECK_CODE);

        farm.getCollection(Backend.class).stream()
                .filter(backend -> backend != null && pool.getId().equals(backend.getParentId()))
                .forEach(backend ->
        {
            if (backend instanceof Backend) {
                final String hostWithPort = backend.getId();
                final Backend.Health lastHealth = ((Backend) backend).getHealth();
                final String fullPath = hostWithPort+hcPath;
                boolean isOk = tester.reset()
                                     .withUrl(fullPath)
                                     .withHost(hcHost)
                                     .withStatusCode(statusCode)
                                     .withBody(hcBody)
                                     .check();
                notifyHealthOnCheck(backend, lastHealth, isOk);
            }
        });
    }

    private String getHost(Entity pool) {
        Optional<Entity> rule = farm.getCollection(Rule.class).stream()
                .filter(r -> pool.getId().equalsIgnoreCase((String) r.getProperty(Rule.PROP_TARGET_ID)))
                .findAny();
        return rule.isPresent() ? rule.get().getParentId() : "";
    }

}
