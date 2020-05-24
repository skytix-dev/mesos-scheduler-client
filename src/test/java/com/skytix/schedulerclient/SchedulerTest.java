package com.skytix.schedulerclient;

import org.apache.mesos.v1.scheduler.Protos;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
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
            public void handleEvent(Protos.Event aEvent) {

                try {
                    getSchedulerRemote().teardown();

                } catch (IOException aE) {
                    aE.printStackTrace();
                }

            }

            @Override
            public void onSubscribe(Protos.Event.Subscribed aSubscribeEvent) {
                subscribed.set(true);
            }

        });

        scheduler.join();

        Assert.assertTrue(subscribed.get());
    }

    @Test
    public void testSchedulerRejectsOffers() throws Exception {
        final AtomicBoolean declined = new AtomicBoolean(false);

        final Scheduler scheduler = createScheduler("junit-test-framework-4", new BaseSchedulerEventHandler() {

            @Override
            public void handleEvent(Protos.Event aEvent) {

                try {
                    if (aEvent.getType() == Protos.Event.Type.OFFERS) {
                        System.out.println("Got an offer");
                        final List<org.apache.mesos.v1.Protos.Offer> offersList = aEvent.getOffers().getOffersList();

                        getSchedulerRemote().decline(
                                offersList.stream().map(org.apache.mesos.v1.Protos.Offer::getId).collect(Collectors.toList())
                        );

                        declined.set(true);
                        getSchedulerRemote().exit();

                    }

                } catch (IOException aE) {
                    aE.printStackTrace();
                }

            }

            @Override
            public void onSubscribe(Protos.Event.Subscribed aSubscribeEvent) {
                System.out.println("Subscribed successfully");
            }
        });

        scheduler.join();

        Assert.assertTrue(declined.get());
    }

    @Test
    public void testSchedulerFailsOnBadHost() throws Exception {
        final AtomicBoolean failed = new AtomicBoolean(false);

        final Scheduler scheduler = createScheduler(UUID.randomUUID().toString(), "http://10.9.10.1:5050", new BaseSchedulerEventHandler() {

            @Override
            public void onTerminate(Exception aException) {
                failed.set(true);
            }

            @Override
            public void onSubscribe(Protos.Event.Subscribed aSubscribeEvent) {

            }

        });

        scheduler.join();

        Assert.assertTrue(failed.get());
    }

    private Scheduler createScheduler(String aFrameworkId, String aMesosHost, SchedulerEventHandler aHandler) throws Exception {

        return Scheduler.newScheduler(
                SchedulerConfig.builder()
                    .frameworkID(aFrameworkId)
                    .mesosMasterURL(aMesosHost)
                .build(),
                aHandler
        );

    }

    private Scheduler createScheduler(String aFrameworkId, SchedulerEventHandler aHandler) throws Exception {
        return createScheduler(aFrameworkId, "http://10.9.10.1:5050", aHandler);
    }

}