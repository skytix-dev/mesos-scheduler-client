package com.skytix.schedulerclient;

import static org.apache.mesos.v1.scheduler.Protos.Event;

public interface SchedulerEventHandler {
    default void onSubscribe(SchedulerRemote aScheduler, Event.Subscribed aSubscribeEvent) {};
    default void onTerminate(Exception aException) {};
    default void onDisconnect() {};
    default void onExit() {};
    default void handleEvent(Event aEvent) {};
}
