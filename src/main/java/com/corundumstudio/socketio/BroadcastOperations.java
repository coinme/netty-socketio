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

import com.corundumstudio.socketio.misc.IterableCollection;
import com.corundumstudio.socketio.parser.Packet;
import io.netty.channel.ChannelFuture;

import java.util.Collection;

public class BroadcastOperations implements ClientOperations {

    private final Iterable<SocketIOClient> clients;

    public BroadcastOperations(Iterable<SocketIOClient> clients) {
        super();
        this.clients = clients;
    }

    public Collection<SocketIOClient> getClients() {
        return new IterableCollection<SocketIOClient>(clients);
    }

    @Override
    public ChannelFuture sendMessage(String message) {
        for (SocketIOClient client : clients) {
            client.sendMessage(message);
        }

        return null; // TODO: should we fix this?
    }

    public <T> void sendMessage(String message, BroadcastAckCallback<T> ackCallback) {
        for (SocketIOClient client : clients) {
            client.sendMessage(message, ackCallback.createClientCallback(client));
        }
        ackCallback.loopFinished();
    }

    @Override
    public ChannelFuture sendJsonObject(Object object) {
        for (SocketIOClient client : clients) {
            client.sendJsonObject(object);
        }

        return null; // TODO: should we fix this?
    }

    public <T> void sendJsonObject(Object object, BroadcastAckCallback<T> ackCallback) {
        for (SocketIOClient client : clients) {
            client.sendJsonObject(object, ackCallback.createClientCallback(client));
        }
        ackCallback.loopFinished();
    }

    @Override
    public ChannelFuture send(Packet packet) {
        for (SocketIOClient client : clients) {
            client.send(packet);
        }

        return null; // TODO: should we fix this?
    }

    public <T> void send(Packet packet, BroadcastAckCallback<T> ackCallback) {
        for (SocketIOClient client : clients) {
            client.send(packet, ackCallback.createClientCallback(client));
        }
        ackCallback.loopFinished();
    }

    @Override
    public ChannelFuture disconnect() {
        for (SocketIOClient client : clients) {
            client.disconnect();
        }

        return null; // TODO: should we fix this?
    }

    @Override
    public ChannelFuture sendEvent(String name, Object data) {
        for (SocketIOClient client : clients) {
            client.sendEvent(name, data);
        }

        return null; // TODO: should we fix this?
    }

    public <T> void sendEvent(String name, Object data, BroadcastAckCallback<T> ackCallback) {
        for (SocketIOClient client : clients) {
            client.sendEvent(name, data, ackCallback.createClientCallback(client));
        }
        ackCallback.loopFinished();
    }

}
