package com.skytix.schedulerclient;

import org.apache.mesos.v1.scheduler.Protos;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class SchedulerTest {

    @Test
    public void testSchedulerConnectsAndCloses() throws Exception {
        final AtomicBoolean subscribed = new AtomicBoolean(false);

        final Scheduler scheduler = createScheduler(UUID.randomUUID().toString(), new BaseSchedulerEventHandler() {

            @Override
            public void handleEvent(Protos.Event aEvent) throws Exception {
                getSchedulerRemote().teardown();
            }

            @Override
            public void onSubscribe() {
                subscribed.set(true);
            }

        });

        scheduler.join();

        Assert.assertTrue(subscribed.get());
    }

    @Test
    public void testSchedulerRejectsOffers() throws Exception {
        final AtomicBoolean declined = new AtomicBoolean(false);

        final Scheduler scheduler = createScheduler("junit-test-framework-3", new BaseSchedulerEventHandler() {

            @Override
            public void handleEvent(Protos.Event aEvent) throws Exception {

                if (aEvent.getType() == Protos.Event.Type.OFFERS) {
                    System.out.println("Got an offer");
                    final List<org.apache.mesos.v1.Protos.Offer> offersList = aEvent.getOffers().getOffersList();

                    getSchedulerRemote().decline(
                            offersList.stream().map(org.apache.mesos.v1.Protos.Offer::getId).collect(Collectors.toList())
                    );

                    declined.set(true);
                    getSchedulerRemote().exit();
                }

            }

        });

        scheduler.join();

        Assert.assertTrue(declined.get());
    }

    @Test
    public void testSchedulerFailsOnBadHost() throws Exception {
        final AtomicBoolean failed = new AtomicBoolean(false);

        final Scheduler scheduler = createScheduler(UUID.randomUUID().toString(), "http://localhost:3433", new BaseSchedulerEventHandler() {

            @Override
            public void onTerminate(Exception aException) {
                failed.set(true);
            }

        });

        scheduler.join();

        Assert.assertTrue(failed.get());
    }

    private Scheduler createScheduler(String aFrameworkId, String aMesosHost, SchedulerEventHandler aHandler) {

        return Scheduler.newScheduler(
                new SchedulerConfig.SchedulerConfigBuilder()
                .frameworkID(aFrameworkId)
                .mesosMasterURL(aMesosHost),
                aHandler
        );

    }

    private Scheduler createScheduler(String aFrameworkId, SchedulerEventHandler aHandler) {
        return Scheduler.newScheduler(aFrameworkId, "http://10.9.10.1:5050", aHandler);
    }

}