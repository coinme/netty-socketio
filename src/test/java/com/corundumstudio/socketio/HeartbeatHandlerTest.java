package com.corundumstudio.socketio;

import com.corundumstudio.socketio.parser.Packet;
import com.corundumstudio.socketio.scheduler.CancelableScheduler;
import com.corundumstudio.socketio.transport.BaseClient;
import io.netty.channel.ChannelFuture;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Created by IntelliJ IDEA.
 * User: Russ
 * Date: 2/6/14
 * Time: 2:04 PM
 */
public class HeartbeatHandlerTest {

    private static Configuration configuration;
    private static MockClient client;
    private static HeartbeatHandler handler;

    @Test
    public void testHeartbeat() throws InterruptedException {
        configuration = new Configuration();
        configuration.setHeartbeatInterval(1);
        configuration.setHeartbeatTimeout(2);

        handler = new HeartbeatHandler(configuration, new CancelableScheduler(2));

        CountDownLatch connectedLatch = new CountDownLatch(1);

        client = new MockClient(UUID.randomUUID().toString(), connectedLatch);

        for (int i = 0; i < 10; i++) {
            if (client.latch.getCount() < 1) {
                fail("Disconnected too soon!");
            }

            // simulate client heartbeat
            handler.onHeartbeat(client);

            // sleep for heartbeat duration
            Thread.sleep(1500);
        }

        assertEquals("Didn't disconnect in time!", true, client.latch.await(2, TimeUnit.SECONDS));
    }

    public static class MockClient extends BaseClient {

        private CountDownLatch latch;

        public MockClient(String sessionId, CountDownLatch latch) {
            super(sessionId, null, null, null);

            this.latch = latch;
        }

        @Override
        public void disconnect() {
            this.latch.countDown();
        }

        @Override
        public ChannelFuture send(Packet packet) {
            return null;
        }
    }

}
