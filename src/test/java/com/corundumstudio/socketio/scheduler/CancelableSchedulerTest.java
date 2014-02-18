package com.corundumstudio.socketio.scheduler;

import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by IntelliJ IDEA.
 * User: Russ
 * Date: 2/17/14
 * Time: 4:18 PM
 */
public class CancelableSchedulerTest {

    CancelableScheduler scheduler = new CancelableScheduler(4);

    @Test
    public void testNormalSchedule() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);

        scheduler.schedule(new LatchRunnable(latch), 50, TimeUnit.MILLISECONDS);

        assertTrue("Didn't fire in time!", latch.await(100, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testScheduleAndCancel() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        String sessionId = UUID.randomUUID().toString();
        SchedulerKey key = new SchedulerKey(SchedulerKey.Type.HEARBEAT_TIMEOUT, sessionId);

        // schedule it
        scheduler.schedule(key, new LatchRunnable(latch), 50, TimeUnit.MILLISECONDS);

        scheduler.cancel(key);

        assertFalse("Should have been cancelled!", latch.await(100, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testScheduleMultipleAndCancelOnce() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        String sessionId = UUID.randomUUID().toString();
        SchedulerKey key = new SchedulerKey(SchedulerKey.Type.HEARBEAT_TIMEOUT, sessionId);

        // schedule it
        scheduler.schedule(key, new LatchRunnable(latch), 50, TimeUnit.MILLISECONDS);
        scheduler.schedule(key, new LatchRunnable(latch), 51, TimeUnit.MILLISECONDS);
        scheduler.schedule(key, new LatchRunnable(latch), 52, TimeUnit.MILLISECONDS);
        scheduler.schedule(key, new LatchRunnable(latch), 53, TimeUnit.MILLISECONDS);
        scheduler.schedule(key, new LatchRunnable(latch), 54, TimeUnit.MILLISECONDS);
        scheduler.schedule(key, new LatchRunnable(latch), 55, TimeUnit.MILLISECONDS);

        scheduler.cancel(key);

        assertFalse("Should have been cancelled!", latch.await(500, TimeUnit.MILLISECONDS));
    }

    private class LatchRunnable implements Runnable {

        final CountDownLatch latch;

        public LatchRunnable(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void run() {
            latch.countDown();
        }
    }
}
