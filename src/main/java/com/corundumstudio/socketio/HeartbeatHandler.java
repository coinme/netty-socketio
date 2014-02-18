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
package com.corundumstudio.socketio;

import com.corundumstudio.socketio.parser.Packet;
import com.corundumstudio.socketio.parser.PacketType;
import com.corundumstudio.socketio.scheduler.CancelableScheduler;
import com.corundumstudio.socketio.scheduler.SchedulerKey;
import com.corundumstudio.socketio.scheduler.SchedulerKey.Type;
import com.corundumstudio.socketio.transport.BaseClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class HeartbeatHandler implements Disconnectable {

    private final Logger LOGGER = LoggerFactory.getLogger(getClass());

    private final CancelableScheduler scheduler;
    private final Configuration configuration;

    private final Map<String, SchedulerKey> schedulerKeyMap = new ConcurrentHashMap<String, SchedulerKey>();
    private final Map<String, Runnable> heartbeatRunnableMap = new ConcurrentHashMap<String, Runnable>();
    private final Map<String, Runnable> disconnectRunnableMap = new ConcurrentHashMap<String, Runnable>();

    public HeartbeatHandler(Configuration configuration, CancelableScheduler scheduler) {
        this.configuration = configuration;
        this.scheduler = scheduler;
    }

    public void onHeartbeat(final BaseClient client) {
        if (!configuration.isHeartbeatsEnabled()) {
            return;
        }

        // cancel disconnect timeout
        cancelTimeout(Type.CLOSE_TIMEOUT, client);

        // refresh heartbeat and disconnect timers
        scheduleHeartbeatAndDisconnectTimeouts(client);
    }

    @Override
    public void onDisconnect(BaseClient client) {
        cancelTimeout(Type.HEARBEAT_TIMEOUT, client);
        cancelTimeout(Type.CLOSE_TIMEOUT, client);
    }

    private void cancelTimeout(Type type, final BaseClient client) {
        SchedulerKey timeoutKey = getSchedulerKey(type, client.getSessionId());

        synchronized (timeoutKey) {
            scheduler.cancel(timeoutKey);
        }
    }

    private void scheduleHeartbeatAndDisconnectTimeouts(final BaseClient client) {
        SchedulerKey heartbeatTimeoutKey = getSchedulerKey(Type.HEARBEAT_TIMEOUT, client.getSessionId());

        synchronized (heartbeatTimeoutKey) {
            // cancel previous heartbeat just in case
            scheduler.cancel(heartbeatTimeoutKey);
            // schedule new heartbeat
            scheduler.schedule(heartbeatTimeoutKey, getHeartbeatRunnable(Type.HEARBEAT_TIMEOUT, client), configuration.getHeartbeatInterval(), TimeUnit.SECONDS);
        }
    }

    private Runnable getHeartbeatRunnable(Type heartbeatType, final BaseClient client) {
        String key = getKey(heartbeatType, client.getSessionId());
        if (heartbeatRunnableMap.containsKey(key)) {
            return heartbeatRunnableMap.get(key);
        }

        final Runnable disconnectRunnable = getDisconnectRunnable(client);

        Runnable runnable = new Runnable() {
            public void run() {
                synchronized (client) {
                    cancelTimeout(Type.CLOSE_TIMEOUT, client);

                    scheduler.schedule(getSchedulerKey(Type.CLOSE_TIMEOUT, client.getSessionId()), disconnectRunnable, configuration.getHeartbeatTimeout(), TimeUnit.SECONDS);

                    client.send(new Packet(PacketType.HEARTBEAT));
                }
            }
        };
        heartbeatRunnableMap.put(key, runnable);

        return runnable;
    }

    private Runnable getDisconnectRunnable(final BaseClient client) {
        String key = getKey(Type.CLOSE_TIMEOUT, client.getSessionId());
        if (disconnectRunnableMap.containsKey(key)) {
            return disconnectRunnableMap.get(key);
        }

        Runnable runnable = new Runnable() {
            public void run() {
                synchronized (client) {
                    client.disconnect();
                    LOGGER.warn("Client with sessionId: {} disconnected due to heartbeat timeout", client.getSessionId());
                }
            }
        };

        disconnectRunnableMap.put(key, runnable);

        return runnable;
    }

    private SchedulerKey getSchedulerKey(Type type, String sessionId) {
        String key = getKey(type, sessionId);
        if (schedulerKeyMap.containsKey(key)) {
            return schedulerKeyMap.get(key);
        }

        SchedulerKey schedulerKey = new SchedulerKey(type, sessionId);
        schedulerKeyMap.put(key, schedulerKey);

        return schedulerKey;
    }

    private String getKey(Type heartbeatType, String sessionId) {
        return heartbeatType + ":" + sessionId;
    }
}
