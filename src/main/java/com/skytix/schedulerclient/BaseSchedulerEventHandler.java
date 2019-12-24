package com.skytix.schedulerclient;

public abstract class BaseSchedulerEventHandler implements SchedulerEventHandler {
    private SchedulerRemote mSchedulerRemote;

    @Override
    public final void onSubscribe(SchedulerRemote aScheduler) {
        mSchedulerRemote = aScheduler;
        onSubscribe();
    }

    public SchedulerRemote getSchedulerRemote() {
        return mSchedulerRemote;
    }

    public void onSubscribe() {

    };

}
