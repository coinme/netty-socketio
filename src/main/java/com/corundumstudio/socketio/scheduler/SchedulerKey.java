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

public class SchedulerKey implements Comparable<SchedulerKey> {

    public enum Type {POLLING, HEARBEAT_TIMEOUT, CLOSE_TIMEOUT, AUTHORIZE, ACK_TIMEOUT};

    private final Type type;
    private final String sessionId;

    public SchedulerKey(Type type, String sessionId) {
        this.type = type;
        this.sessionId = sessionId;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((sessionId == null) ? 0 : sessionId.hashCode());
        result = prime * result + ((type == null) ? 0 : type.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        SchedulerKey other = (SchedulerKey) obj;
        if (sessionId == null) {
            if (other.sessionId != null)
                return false;
        } else if (!sessionId.equals(other.sessionId))
            return false;
        if (type != other.type)
            return false;
        return true;
    }

    @Override
    public int compareTo(SchedulerKey o1) {
        SchedulerKey o2 = this;

        if ((o1 == null) && (o2 == null)) {
            return 0;
        } else if ((o1 == null) && (o2 != null)) {
            return -1;
        } else if ((o1 != null) && (o2 == null)) {
            return 1;
        }

        int left = o1.hashCode();
        int right = o2.hashCode();

        if (left < right) {
            return -1;
        } else if (left > right) {
            return 1;
        } else {
            return 0;
        }
    }


}
