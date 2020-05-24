package com.skytix.schedulerclient;

import org.apache.mesos.v1.scheduler.Protos;

public abstract class BaseSchedulerEventHandler implements SchedulerEventHandler {
    private SchedulerRemote mSchedulerRemote;

    @Override
    public final void onSubscribe(SchedulerRemote aScheduler, Protos.Event.Subscribed aSubscribeEvent) {
        mSchedulerRemote = aScheduler;
        onSubscribe(aSubscribeEvent);
    }

    public SchedulerRemote getSchedulerRemote() {
        return mSchedulerRemote;
    }

    public abstract void onSubscribe(Protos.Event.Subscribed aSubscribeEvent);
}
