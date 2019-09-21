package com.skytix.schedulerclient;

public abstract class BaseSchedulerEventHandler implements SchedulerEventHandler {
    private SchedulerRemote mScheduler;

    @Override
    public final void onSubscribe(SchedulerRemote aScheduler) {
        mScheduler = aScheduler;
        onSubscribe();
    }

    protected SchedulerRemote getScheduler() {
        return mScheduler;
    }

    public void onSubscribe() {

    };

}
