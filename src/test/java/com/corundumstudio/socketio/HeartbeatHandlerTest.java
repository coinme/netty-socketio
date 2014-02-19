package com.corundumstudio.socketio;

import com.corundumstudio.socketio.parser.Packet;
import com.corundumstudio.socketio.scheduler.CancelableScheduler;
import com.corundumstudio.socketio.transport.BaseClient;
import io.netty.channel.ChannelFuture;
import org.junit.Test;

import java.util.Random;
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
    private Random random = new Random();

    @Test
    public void testHeartbeat() throws InterruptedException {
        configuration = new Configuration();
        configuration.setHeartbeatTimeout(1);
        configuration.setCloseTimeout(2);

        handler = new HeartbeatHandler(configuration, new CancelableScheduler(2));

        CountDownLatch connectedLatch = new CountDownLatch(1);

        client = new MockClient(UUID.randomUUID().toString(), connectedLatch);

        for (int i = 0; i < 10; i++) {
            // sleep for heartbeat interval
            Thread.sleep(1500);

            ensureConnected();

            // simulate client heartbeat
            handler.onHeartbeat(client);
        }

        assertEquals("Didn't disconnect in time!", true, client.latch.await(4, TimeUnit.SECONDS));
    }

    @Test
    public void testHeartbeatWithRandomHeartbeats() throws InterruptedException {
        configuration = new Configuration();
        configuration.setHeartbeatTimeout(1);
        configuration.setCloseTimeout(2);

        handler = new HeartbeatHandler(configuration, new CancelableScheduler(2));

        CountDownLatch connectedLatch = new CountDownLatch(1);

        client = new MockClient(UUID.randomUUID().toString(), connectedLatch);

        for (int i = 0; i < 10; i++) {
            // sleep for heartbeat interval
            Thread.sleep(750);

            if (random.nextBoolean()) {
                handler.onHeartbeat(client);
            }

            Thread.sleep(750);

            ensureConnected();

            // simulate client heartbeat
            handler.onHeartbeat(client);

            if (random.nextBoolean()) {
                handler.onHeartbeat(client);
            }
        }

        assertEquals("Didn't disconnect in time!", true, client.latch.await(4, TimeUnit.SECONDS));
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

    private void ensureConnected() {
        if (client.latch.getCount() < 1) {
            fail("Disconnected too soon!");
        }
    }
}
