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

    private final Logger log = LoggerFactory.getLogger(getClass());

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

        final SchedulerKey heartbeatTimeoutKey = getHeartbeatTimeoutKey(Type.HEARBEAT_TIMEOUT, client.getSessionId());
        // cancel heartbeat check because the client answered
        synchronized (heartbeatTimeoutKey) {
            scheduler.cancel(heartbeatTimeoutKey);
            scheduler.schedule(getHeartbeatRunnable(Type.HEARBEAT_TIMEOUT, client), configuration.getHeartbeatInterval(), TimeUnit.SECONDS);
        }
    }

    @Override
    public void onDisconnect(BaseClient client) {
        SchedulerKey schedulerKey = getHeartbeatTimeoutKey(Type.HEARBEAT_TIMEOUT, client.getSessionId());

        if (schedulerKey == null) {
            log.error("Couldn't cancel schedule for null client!");
            return;
        }

        synchronized (schedulerKey) {
            scheduler.cancel(getHeartbeatTimeoutKey(Type.HEARBEAT_TIMEOUT, client.getSessionId()));
        }
    }

    private void scheduleClientHeartbeatCheck(final BaseClient client, SchedulerKey heartbeatTimeoutKey) {
        // cancel previous heartbeat check
        synchronized (heartbeatTimeoutKey) {
            scheduler.cancel(heartbeatTimeoutKey);
            scheduler.schedule(heartbeatTimeoutKey, getDisconnectRunnable(Type.HEARBEAT_TIMEOUT, client), configuration.getHeartbeatTimeout(), TimeUnit.SECONDS);
        }
    }

    private Runnable getHeartbeatRunnable(Type heartbeatType, final BaseClient client) {
        String key = getMapKey(heartbeatType, client.getSessionId());
        if (heartbeatRunnableMap.containsKey(key)) {
            return heartbeatRunnableMap.get(key);
        }

        Runnable runnable = new Runnable() {
            public void run() {
                client.send(new Packet(PacketType.HEARTBEAT));
                scheduleClientHeartbeatCheck(client, getHeartbeatTimeoutKey(Type.HEARBEAT_TIMEOUT, client.getSessionId()));
            }
        };
        heartbeatRunnableMap.put(key, runnable);

        return runnable;
    }

    private Runnable getDisconnectRunnable(Type heartbeatType, final BaseClient client) {
        String key = getMapKey(heartbeatType, client.getSessionId());
        if (disconnectRunnableMap.containsKey(key)) {
            return disconnectRunnableMap.get(key);
        }

        Runnable runnable = new Runnable() {
            public void run() {
                client.disconnect();
                log.debug("Client with sessionId: {} disconnected due to heartbeat timeout", client.getSessionId());
            }
        };

        disconnectRunnableMap.put(key, runnable);

        return runnable;
    }

    private SchedulerKey getHeartbeatTimeoutKey(Type heartbeatType, String sessionId) {
        String key = getMapKey(heartbeatType, sessionId);
        if (schedulerKeyMap.containsKey(key)) {
            return schedulerKeyMap.get(key);
        }

        SchedulerKey schedulerKey = new SchedulerKey(heartbeatType, sessionId);
        schedulerKeyMap.put(key, schedulerKey);

        return schedulerKey;
    }

    private String getMapKey(Type heartbeatType, String sessionId) {
        return "heartbeat:" + heartbeatType + ":" + sessionId;
    }

}
