/**
 * Copyright 2012 Nikita Koksharov
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.corundumstudio.socketio.scheduler;

import com.google.common.base.Preconditions;
import com.zipwhip.pools.PoolUtil;
import com.zipwhip.pools.PoolableObjectFactoryBase;
import org.apache.commons.pool.ObjectPool;
import org.apache.commons.pool.PoolUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.*;

public class CancelableScheduler {

    private static final Logger LOGGER = LoggerFactory.getLogger(CancelableScheduler.class);

    private static final ObjectPool OBJECT_POOL = PoolUtils.erodingPool(PoolUtil.getPool(new PoolableObjectFactoryBase() {
        @Override
        public Object makeObject() throws Exception {
            return new ScheduledRunnable();
        }
    }));

    private final Map<SchedulerKey, Future<?>> scheduledFutures = new ConcurrentHashMap<SchedulerKey, Future<?>>();
    private final ScheduledExecutorService executorService;

    public CancelableScheduler(int threadPoolSize) {
        executorService = Executors.newScheduledThreadPool(threadPoolSize);
    }

    public void cancel(SchedulerKey key) {
        Future<?> future = scheduledFutures.remove(key);

        if (future != null) {
            future.cancel(false);
        }
    }

    public void schedule(Runnable runnable, long delay, TimeUnit unit) {
        executorService.schedule(runnable, delay, unit);
    }

    public void schedule(final SchedulerKey key, final Runnable runnable, long delay, TimeUnit unit) {
        Runnable scheduledRunnable = getRunnable(key, runnable);
        Future<?> future = executorService.schedule(scheduledRunnable, delay, unit);

        scheduledFutures.put(key, future);
    }

    private Runnable getRunnable(final SchedulerKey key, final Runnable runnable) {
        ScheduledRunnable result;

        try {
            result = (ScheduledRunnable) OBJECT_POOL.borrowObject();
        } catch (Exception e) {
            result = new ScheduledRunnable();
        }

        result.setKey(key);
        result.setRunnable(runnable);
        result.setScheduledFutures(scheduledFutures);

        return result;
    }

    public void shutdown() {
        executorService.shutdownNow();
    }

    private static void releaseScheduledRunnable(ScheduledRunnable scheduledRunnable) {
        try {
            OBJECT_POOL.returnObject(scheduledRunnable);
        } catch (Exception e) {
            LOGGER.error("Failed to returnObject!", e);
        }
    }

    private static class ScheduledRunnable implements Runnable {

        private Map<SchedulerKey, Future<?>> scheduledFutures;
        private Runnable runnable;
        private SchedulerKey key;

        @Override
        public void run() {
            try {
                try {
                    runnable.run();
                } finally {
                    scheduledFutures.remove(key);
                }
            } finally {
                releaseScheduledRunnable(this);
            }
        }

        public Map<SchedulerKey, Future<?>> getScheduledFutures() {
            return scheduledFutures;
        }

        public void setScheduledFutures(Map<SchedulerKey, Future<?>> scheduledFutures) {
            this.scheduledFutures = Preconditions.checkNotNull(scheduledFutures);
        }

        public Runnable getRunnable() {
            return runnable;
        }

        public void setRunnable(Runnable runnable) {
            this.runnable = Preconditions.checkNotNull(runnable);
        }

        public SchedulerKey getKey() {
            return key;
        }

        public void setKey(SchedulerKey key) {
            this.key = Preconditions.checkNotNull(key);
        }
    }

}
