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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class AckManager implements Disconnectable {

    private final Logger log = LoggerFactory.getLogger(getClass());

    class AckEntry {

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
        // TODO: ObjectPool here.

        if (log.isTraceEnabled()) {
            log.trace("Ack received for client: " + client.getSessionId());
        }

        SchedulerKey key = new AckSchedulerKey(Type.ACK_TIMEOUT, client.getSessionId(), packet.getAckId());
        scheduler.cancel(key);

        AckCallback callback = removeCallback(client.getSessionId(), packet.getAckId());
        if (callback != null) {
            Object param = null;
            if (!packet.getArgs().isEmpty()) {
                param = packet.getArgs().get(0);
            }
            callback.onSuccess(param);
        }
    }

    private AckCallback removeCallback(String sessionId, long index) {
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

        if (log.isDebugEnabled()) {
            log.debug("AckCallback registered with id: {}", index);
        }

        scheduleTimeout(index, sessionId, callback);

        return index;
    }

    private void scheduleTimeout(final long index, final String sessionId, final AckCallback callback) {
        if (callback.getTimeout() == -1) {
            return;
        }
        SchedulerKey key = new AckSchedulerKey(Type.ACK_TIMEOUT, sessionId, index);
        scheduler.schedule(key, new Runnable() {
            @Override
            public void run() {
                AckCallback c = removeCallback(sessionId, index);

                if (c != null) {
                    if (c != callback) {
                        throw new IllegalStateException("The callback1 for timeout did not match callback2. This would be a crisscrossed callback.onTimeout() call.");
                    }

                    c.onTimeout();
                }
            }
        }, callback.getTimeout(), TimeUnit.SECONDS);
    }

    @Override
    public void onDisconnect(BaseClient client) {
        AckEntry e = ackEntries.remove(client.getSessionId());

        if (e == null) {
            return;
        }

        Set<Long> indexes = e.getAckIndexes();

        if (indexes == null) {
            return;
        }

        synchronized (indexes) {
            for (Long index : indexes) {
                SchedulerKey key =  new AckSchedulerKey(Type.ACK_TIMEOUT, client.getSessionId(), index);

                scheduler.cancel(key);
            }
        }
    }
}