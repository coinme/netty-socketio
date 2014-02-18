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

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class CancelableScheduler {

    private final Map<SchedulerKey, Future<?>> scheduledFutures = new TreeMap<SchedulerKey, Future<?>>();
    private final ScheduledExecutorService executorService;

    public CancelableScheduler(int threadPoolSize) {
        executorService = Executors.newScheduledThreadPool(threadPoolSize);
    }

    public synchronized void cancel(SchedulerKey key) {
        Future<?> future = scheduledFutures.remove(key);
        if (future != null) {
            future.cancel(false);
        }
    }

    public void schedule(Runnable runnable, long delay, TimeUnit unit) {
        executorService.schedule(runnable, delay, unit);
    }

    // TODO: If a cancel comes in at the same time as a schedule, this object will fail. It is not thread safe.
    public synchronized void schedule(final SchedulerKey key, final Runnable runnable, long delay, TimeUnit unit) {
        // We only allow one scheduled task per key (otherwise you lose the ability to cancel)
        if (scheduledFutures.containsKey(key)) {
            Future future = scheduledFutures.remove(key);
            if (future != null) {
                future.cancel(false);
            }
        }

        Future<?> future = executorService.schedule(new Runnable() {
            @Override
            public void run() {
                try {
                    runnable.run();
                } finally {
                    scheduledFutures.remove(key);
                }
            }
        }, delay, unit);
        scheduledFutures.put(key, future);
    }

    public void shutdown() {
        executorService.shutdownNow();
    }

}
