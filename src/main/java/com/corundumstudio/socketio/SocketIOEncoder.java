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

import com.corundumstudio.socketio.messages.*;
import com.corundumstudio.socketio.messages.HttpMessage;
import com.corundumstudio.socketio.parser.Encoder;
import com.corundumstudio.socketio.parser.Packet;
import com.corundumstudio.socketio.transport.BaseClient;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

import static io.netty.handler.codec.http.HttpHeaders.Names.*;
import static io.netty.handler.codec.http.HttpHeaders.Values.KEEP_ALIVE;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

@Sharable
public class SocketIOEncoder extends ChannelOutboundHandlerAdapter implements Disconnectable {

    class XHRClientEntry {

        // works faster than locking
        final AtomicReference<Channel> lastChannel = new AtomicReference<Channel>();
        final Queue<Packet> packets = new ConcurrentLinkedQueue<Packet>();

        public void addPacket(Packet packet) {
            packets.add(packet);
        }

        public Queue<Packet> getPackets() {
            return packets;
        }

        /**
         * We can write to channel only once.
         *
         * @param channel
         * @return true - can write
         */
        public boolean writeOnce(Channel channel) {
            Channel prevVal = lastChannel.get();
            return !channel.equals(prevVal)
                            && lastChannel.compareAndSet(prevVal, channel);
        }

    }

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final ConcurrentMap<String, XHRClientEntry> sessionId2ActiveChannelId = new ConcurrentHashMap<String, XHRClientEntry>();
    private final Encoder encoder;

    public SocketIOEncoder(Encoder encoder) {
        this.encoder = encoder;
    }

    private XHRClientEntry getXHRClientEntry(String sessionId) {
        XHRClientEntry clientEntry = sessionId2ActiveChannelId.get(sessionId);
        if (clientEntry == null) {
            clientEntry = new XHRClientEntry();
            XHRClientEntry old = sessionId2ActiveChannelId.putIfAbsent(sessionId, clientEntry);
            if (old != null) {
                clientEntry = old;
            }
        }
        return clientEntry;
    }

    private void write(HttpMessage xhrMessage, Packet packet,
            ChannelHandlerContext ctx, ByteBuf out, ChannelPromise promise) throws IOException {
        XHRClientEntry clientEntry = getXHRClientEntry(xhrMessage.getSessionId());
        if (packet != null) {
            clientEntry.addPacket(packet);
        }

        Channel channel = ctx.channel();
        if (!channel.isActive() || clientEntry.getPackets().isEmpty()
                    || !clientEntry.writeOnce(channel)) {
            out.release();
            return;
        }

        encoder.encodePackets(clientEntry.getPackets(), out, ctx.alloc());
        sendMessage(xhrMessage, channel, out, promise);
    }

    private void sendMessage(HttpMessage msg, Channel channel, ByteBuf out, ChannelPromise promise) {
        HttpResponse res = createHttpResponse(msg.getOrigin(), out);
        channel.write(res);

        if (log.isTraceEnabled()) {
            log.trace("Out message: {} - sessionId: {}",
                        out.toString(CharsetUtil.UTF_8), msg.getSessionId());
        }
        if (out.isReadable()) {
            channel.write(out, promise);
        } else {
            out.release();
        }

        ChannelFuture f = channel.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
        f.addListener(ChannelFutureListener.CLOSE);
    }

    private HttpResponse createHttpResponse(String origin, ByteBuf message) {
        HttpResponse res = new DefaultHttpResponse(HTTP_1_1, HttpResponseStatus.OK);

        HttpHeaders.addHeader(res, CONTENT_TYPE, "text/plain; charset=UTF-8");
        HttpHeaders.addHeader(res, CONNECTION, KEEP_ALIVE);
        if (origin != null) {
            HttpHeaders.addHeader(res, ACCESS_CONTROL_ALLOW_ORIGIN, origin);
            HttpHeaders.addHeader(res, ACCESS_CONTROL_ALLOW_CREDENTIALS, "true");
        }
        HttpHeaders.setContentLength(res, message.readableBytes());

        return res;
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (!(msg instanceof BaseMessage)) {
            super.write(ctx, msg, promise);
            return;
        }

        ByteBuf out = encoder.allocateBuffer(ctx.alloc());

        if (msg instanceof AuthorizeMessage) {
            handle((AuthorizeMessage) msg, ctx.channel(), out, promise);
        }

        if (msg instanceof XHRNewChannelMessage) {
            write((XHRNewChannelMessage) msg, null, ctx, out, promise);
        }
        if (msg instanceof XHRPacketMessage) {
            XHRPacketMessage m = (XHRPacketMessage) msg;
            write(m, m.getPacket(), ctx, out, promise);
        }
        if (msg instanceof XHROutMessage) {
            sendMessage((XHROutMessage) msg, ctx.channel(), out, promise);
        }
        if (msg instanceof XHRErrorMessage) {
            XHRErrorMessage xhrErrorMessage = (XHRErrorMessage) msg;
            encoder.encodePacket(xhrErrorMessage.getPacket(), out);
            sendMessage(xhrErrorMessage, ctx.channel(), out, promise);
        }

        if (msg instanceof WebSocketPacketMessage) {
            handle((WebSocketPacketMessage) msg, ctx.channel(), out, promise);
        }
        if (msg instanceof WebsocketErrorMessage) {
            handle((WebsocketErrorMessage) msg, ctx.channel(), out, promise);
        }
    }

    private void handle(AuthorizeMessage authMsg, Channel channel, ByteBuf out, ChannelPromise promise) throws IOException {
        String message = authMsg.getMsg();
        if (authMsg.getJsonpParam() != null) {
            encoder.encodeJsonP(authMsg.getJsonpParam(), message, out);
        } else {
            out.writeBytes(message.getBytes());
        }
        sendMessage(authMsg, channel, out, promise);
    }

    private void handle(WebSocketPacketMessage webSocketPacketMessage, Channel channel, ByteBuf out, ChannelPromise promise) throws IOException {
        encoder.encodePacket(webSocketPacketMessage.getPacket(), out);
        WebSocketFrame res = new TextWebSocketFrame(out);
        log.trace("Out message: {} sessionId: {}",
                        out.toString(CharsetUtil.UTF_8), webSocketPacketMessage.getSessionId());
        channel.writeAndFlush(res, promise);
        if (!out.isReadable()) {
            out.release();
        }
    }

    private void handle(WebsocketErrorMessage websocketErrorMessage, Channel channel, ByteBuf out, ChannelPromise promise) throws IOException {
        encoder.encodePacket(websocketErrorMessage.getPacket(), out);
        TextWebSocketFrame frame = new TextWebSocketFrame(out);
        channel.writeAndFlush(frame, promise);
    }

    @Override
    public void onDisconnect(BaseClient client) {
        sessionId2ActiveChannelId.remove(client.getSessionId());
    }

}
