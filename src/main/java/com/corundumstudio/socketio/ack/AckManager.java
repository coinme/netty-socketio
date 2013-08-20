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
package com.corundumstudio.socketio.ack;

import com.corundumstudio.socketio.AckCallback;
import com.corundumstudio.socketio.Disconnectable;
import com.corundumstudio.socketio.SocketIOClient;
import com.corundumstudio.socketio.parser.Packet;
import com.corundumstudio.socketio.scheduler.CancelableScheduler;
import com.corundumstudio.socketio.scheduler.SchedulerKey;
import com.corundumstudio.socketio.scheduler.SchedulerKey.Type;
import com.corundumstudio.socketio.transport.BaseClient;
import com.google.common.base.Preconditions;
import com.zipwhip.pools.PoolUtil;
import com.zipwhip.pools.PoolableObjectFactoryBase;
import org.apache.commons.pool.ObjectPool;
import org.apache.commons.pool.PoolUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class AckManager implements Disconnectable {

    private static final Logger LOGGER = LoggerFactory.getLogger(AckManager.class);

    protected static final ObjectPool OBJECT_POOL = PoolUtils.erodingPool(PoolUtil.getPool(new PoolableObjectFactoryBase() {
        public Object makeObject() throws Exception {
            return new ScheduledRunnable();
        }
    }));

    private final Map<String, AckEntry> ackEntries = new ConcurrentHashMap<String, AckEntry>();

    private final CancelableScheduler scheduler;

    public AckManager(CancelableScheduler scheduler) {
        super();
        this.scheduler = scheduler;
    }

    public void initAckIndex(String sessionId, long index) {
        AckEntry ackEntry = getAckEntry(sessionId);
        ackEntry.initAckIndex(index);
    }

    private AckEntry getAckEntry(String sessionId) {
        AckEntry ackEntry = ackEntries.get(sessionId);
        if (ackEntry == null) {
            ackEntry = new AckEntry();
            AckEntry oldAckEntry = ackEntries.put(sessionId, ackEntry);
            if (oldAckEntry != null) {
                ackEntry = oldAckEntry;
            }
        }
        return ackEntry;
    }

    public void onAck(SocketIOClient client, Packet packet) {
        SchedulerKey key = new SchedulerKey(Type.ACK_TIMEOUT, client.getSessionId());
        scheduler.cancel(key);

        AckCallback callback = removeCallback(ackEntries, client.getSessionId(), packet.getAckId());
        if (callback != null) {
            Object param = null;
            if (!packet.getArgs().isEmpty()) {
                param = packet.getArgs().get(0);
            }
            callback.onSuccess(param);
        }
    }

    private static AckCallback removeCallback(Map<String, AckEntry> ackEntries, String sessionId, long index) {
        AckEntry ackEntry = ackEntries.get(sessionId);
        // may be null if client disconnected
        // before timeout occurs
        if (ackEntry != null) {
            return ackEntry.removeCallback(index);
        }
        return null;
    }

    public AckCallback<?> getCallback(String sessionId, long index) {
        AckEntry ackEntry = getAckEntry(sessionId);
        return ackEntry.getAckCallback(index);
    }

    public long registerAck(String sessionId, AckCallback callback) {
        AckEntry ackEntry = getAckEntry(sessionId);
        ackEntry.initAckIndex(0);
        long index = ackEntry.addAckCallback(callback);

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("AckCallback registered with id: {}", index);
        }

        scheduleTimeout(index, sessionId, callback);

        return index;
    }

    private void scheduleTimeout(final long index, final String sessionId, final AckCallback callback) {
        if (callback.getTimeout() == -1) {
            return;
        }
        // TODO: consider adding ObjectPooling to AckSchedulerKey
        SchedulerKey key = new AckSchedulerKey(Type.ACK_TIMEOUT, sessionId, index);
        ScheduledRunnable runnable = borrowRunnable(callback, key.getSessionId(), index);

        try {
            scheduler.schedule(key, runnable, callback.getTimeout(), TimeUnit.SECONDS);
        } catch (Exception e) {
            releaseScheduledRunnable(runnable);
        }
    }

    private static void releaseScheduledRunnable(ScheduledRunnable runnable) {
        try {
            OBJECT_POOL.returnObject(runnable);
        } catch (Exception e) {
            LOGGER.error("Failed to return runnable", e);
        }
    }

    private ScheduledRunnable borrowRunnable(AckCallback callback, String sessionId, long index) {
        ScheduledRunnable runnable;

        try {
            runnable = (ScheduledRunnable) OBJECT_POOL.borrowObject();
        } catch (Exception e) {
            runnable = new ScheduledRunnable();
        }

        runnable.setAckEntries(ackEntries);
        runnable.setCallback(callback);
        runnable.setIndex(index);
        runnable.setSessionId(sessionId);

        return runnable;
    }

    @Override
    public void onDisconnect(BaseClient client) {
        ackEntries.remove(client.getSessionId());
    }

    private static class ScheduledRunnable implements Runnable {

        private String sessionId;
        private long index;
        private AckCallback callback;
        private Map<String, AckEntry> ackEntries;

        @Override
        public void run() {
            try {
                try {
                    removeCallback(ackEntries, sessionId, index);
                } finally {
                    callback.onTimeout();
                }
            } finally {
                releaseScheduledRunnable(this);
            }
        }

        public Map<String, AckEntry> getAckEntries() {
            return ackEntries;
        }

        public void setAckEntries(Map<String, AckEntry> ackEntries) {
            this.ackEntries = Preconditions.checkNotNull(ackEntries);
        }

        public String getSessionId() {
            return sessionId;
        }

        public void setSessionId(String sessionId) {
            this.sessionId = Preconditions.checkNotNull(sessionId);
        }

        public long getIndex() {
            return index;
        }

        public void setIndex(long index) {
            this.index = index;
        }

        public AckCallback getCallback() {
            return callback;
        }

        public void setCallback(AckCallback callback) {
            this.callback = Preconditions.checkNotNull(callback);
        }
    }

    private static class AckEntry {

        final Map<Long, AckCallback<?>> ackCallbacks = new ConcurrentHashMap<Long, AckCallback<?>>();
        final AtomicLong ackIndex = new AtomicLong(-1);

        public long addAckCallback(AckCallback<?> callback) {
            long index = ackIndex.incrementAndGet();
            ackCallbacks.put(index, callback);
            return index;
        }

        public Set<Long> getAckIndexes() {
            return ackCallbacks.keySet();
        }

        public AckCallback<?> getAckCallback(long index) {
            return ackCallbacks.get(index);
        }

        public AckCallback<?> removeCallback(long index) {
            return ackCallbacks.remove(index);
        }

        public void initAckIndex(long index) {
            ackIndex.compareAndSet(-1, index);
        }

    }
}