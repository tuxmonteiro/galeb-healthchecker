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
import static io.galeb.services.healthchecker.HealthChecker.PROP_HEALTHCHECKER_FOLLOW_REDIR;
import static io.galeb.services.healthchecker.HealthChecker.PROP_HEALTHCHECKER_CONN_TIMEOUT;
import static io.galeb.services.healthchecker.HealthChecker.PROP_HEALTHCHECKER_THREADS;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import io.galeb.core.jcache.CacheFactory;
import io.galeb.core.json.JsonObject;
import io.galeb.services.healthchecker.testers.RestAssuredTester;
import io.galeb.services.healthchecker.testers.TestExecutor;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import io.galeb.core.logging.Logger;
import io.galeb.core.model.Backend;
import io.galeb.core.model.Backend.Health;
import io.galeb.core.model.BackendPool;
import io.galeb.core.model.Entity;
import io.galeb.core.model.Rule;
import io.galeb.core.services.AbstractService;
import io.galeb.services.healthchecker.HealthChecker;

import javax.cache.Cache;

@DisallowConcurrentExecution
public class HealthCheckJob implements Job {

    private static Integer threads = Integer.parseInt(System.getProperty(PROP_HEALTHCHECKER_THREADS,
            String.valueOf(Runtime.getRuntime().availableProcessors())));

    private Optional<Logger> logger = Optional.empty();
    private CacheFactory cacheFactory;
    private final ExecutorService executor = Executors.newWorkStealingPool(threads);
    private final Map<String, Future<Boolean>> backendMapProcess = new ConcurrentHashMap<>();

    @SuppressWarnings("unchecked")
    private void init(final JobDataMap jobDataMap) {
        if (!logger.isPresent()) {
            logger = Optional.ofNullable((Logger) jobDataMap.get(AbstractService.LOGGER));
        }
        if (cacheFactory==null) {
            cacheFactory = (CacheFactory) jobDataMap.get(AbstractService.CACHEFACTORY);
        }
    }

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {

        init(context.getJobDetail().getJobDataMap());

        logger.ifPresent(log -> log.info("=== " + this.getClass().getSimpleName() + " ==="));

        Cache<String, String> pools = cacheFactory.getCache(BackendPool.class.getName());
        Cache<String, String> backends = cacheFactory.getCache(Backend.class.getName());
        Stream<Cache.Entry<String, String>> streamOfBackendPools = StreamSupport.stream(pools.spliterator(), true);

        streamOfBackendPools.parallel().forEach(entry -> {
            BackendPool backendPool = (BackendPool) JsonObject.fromJson(entry.getValue(), BackendPool.class);
            Stream<Cache.Entry<String, String>> streamOfBackends = StreamSupport.stream(backends.spliterator(), false);
            streamOfBackends.map(entry2 -> (Backend) JsonObject.fromJson(entry2.getValue(), Backend.class))
                    .filter(b -> b.getParentId().equals(backendPool.getId()))
                    .forEach(backendPool::addBackend);
            checkBackendPool(backendPool, getProperties(backendPool));
        });

        logger.ifPresent(log -> log.debug("Job HealthCheck done."));

    }

    private void notifyHealthOnCheck(Entity entity, boolean isOk) {
        if (entity instanceof Backend) {
            Backend backend = (Backend)entity;
            Health lastHealth = backend.getHealth();
            logger.ifPresent(log -> log.debug("Last Health " + entity.compoundId() + " is "+ lastHealth.toString()));
            if (isOk) {
                backend.setHealth(Health.HEALTHY);
            } else {
                backend.setHealth(Health.DEADY);
            }
            if (backend.getHealth()!=lastHealth) {
                logger.ifPresent(log -> log.debug("New Health " + entity.compoundId() + " is "+ backend.getHealth().toString()));
                String hostWithPort = backend.getId();
                if (isOk) {
                    logger.ifPresent(log -> log.info(hostWithPort+" is OK"));
                } else {
                    logger.ifPresent(log -> log.warn(hostWithPort+" is FAILED"));
                }
                Cache<String, String> cache = cacheFactory.getCache(Backend.class.getName());
                cache.replace(entity.compoundId(), JsonObject.toJsonString(backend));
            }
        } else {
            logger.ifPresent(log -> log.warn("Entity is NOT Backend"));
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

    private void checkBackendPool(final BackendPool pool, final Map<String, Object> properties) {

        final String hcBody = (String) properties.get(PROP_HEALTHCHECK_RETURN);
        final String hcPath = (String) properties.get(PROP_HEALTHCHECK_PATH);
        final String hcHost = (String) properties.get(PROP_HEALTHCHECK_HOST);
        final int statusCode = (int) properties.get(PROP_HEALTHCHECK_CODE);
        final AtomicBoolean isOk = new AtomicBoolean(false);

        pool.getBackends().stream().forEach(backend ->
        {
            String connTimeOut = System.getProperty(PROP_HEALTHCHECKER_CONN_TIMEOUT);
            String followRedirects = System.getProperty(PROP_HEALTHCHECKER_FOLLOW_REDIR);

            if (backend != null) {
                final String hostWithPort = backend.getId();
                final String fullPath = hostWithPort+hcPath;
                try {
                    final HealthCheckJob job = this;
                    final String futureKey = backend.compoundId();
                    Future<Boolean> future = backendMapProcess.get(futureKey);
                    if (future == null || future.isDone() || future.isCancelled()) {
                        logger.ifPresent(log -> log.info("Processing " + futureKey));
                        future = executor.submit(new Callable<Boolean>() {
                            @Override
                            public Boolean call() throws Exception {
                                return new RestAssuredTester()
                                        .reset()
                                        .withJob(job)
                                        .withUrl(fullPath)
                                        .withHost(hcHost)
                                        .withStatusCode(statusCode)
                                        .withBody(hcBody)
                                        .setConnectionTimeOut(connTimeOut != null ?
                                                Integer.parseInt(connTimeOut) : null)
                                        .followRedirects(followRedirects != null ?
                                                Boolean.parseBoolean(followRedirects) : null)
                                        .setLogger(logger)
                                        .setEntity(backend)
                                        .check();
                            }
                        });
                        backendMapProcess.put(futureKey, future);
                    }
                } catch (Exception e) {
                    logger.ifPresent(log -> log.error(hostWithPort+": "+e.getMessage()));
                    e.printStackTrace();
                }
            }
        });
    }

    private String getHost(Entity pool) {
        Cache<String, String> rules = cacheFactory.getCache(Rule.class.getName());
        Stream<Cache.Entry<String, String>> streamOfRules = StreamSupport.stream(rules.spliterator(), false);

        Optional<Rule> rule = streamOfRules.map(entry -> (Rule) JsonObject.fromJson(entry.getValue(), Rule.class))
                .filter(r -> pool.getId().equalsIgnoreCase((String) r.getProperty(Rule.PROP_TARGET_ID)))
                .findAny();

        return rule.isPresent() ? rule.get().getParentId() : "";
    }

    public void done(TestExecutor tester) {
        try {
            Entity entity = tester.getEntity();
            String futureKey = entity.compoundId();

            Boolean checkResult = backendMapProcess.get(futureKey).get();
            notifyHealthOnCheck(entity, checkResult);
            logger.ifPresent(log -> log.debug("Done " + futureKey + " with result: " + checkResult.toString()));
            backendMapProcess.remove(futureKey);
        } catch (InterruptedException|ExecutionException e) {
            logger.ifPresent(log -> log.error(e.getMessage()));
        }
    }

}
