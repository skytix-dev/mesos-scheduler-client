# mesos-scheduler-client
Java Mesos Scheduler library that uses the HTTP API to avoid the use of libmesos and provide a pure Java implementation that is straightforward to use and integrate.

It is designed to be lightweight interface that works with Java 11+ which allows developers to perform primitive scheduler functions 
## Install
You can install the library from Maven Central

``` compile group: 'au.com.skytix', name: 'mesos-scheduler-client', version: '1.0.0'```

## Usage
There is no limit on the number of schedulers your can create.  If no FrameworkID is provided on startup, a random UUID will generated.
```
final Scheduler scheduler = Scheduler.newScheduler(
        new SchedulerConfig.SchedulerConfigBuilder()
                .mesosMasterURL("http://localhost:5050"),
        new BaseSchedulerEventHandler() {
        
            @Override
            public void handleEvent(Protos.Event aEvent) throws Exception {
                // Handle event message from Mesos
            }

            @Override
            public void onSubscribe() {
                // Event message invoked after the Scheduler has successfully connected.
                // getScheduler() returns a SchedulerRemote which is used to interact with Mesos
            }

            @Override
            public void onTerminate(Exception aException) {
                // If the Scheduler unexpectedly gets terminated due to an error.
            }

            @Override
            public void onDisconnect() {
                // If the Scheduler loses it's connection with the Master
            }

            @Override
            public void onExit() {
                // If the scheduler exits without any error.
            }
        }

)

scheduler.join(); // Optional: Wait till the Scheduler exits normally or abnormally.

scheduler.close(); // If you want to manually shutdown the scheduler.  It will NOT Teardown the FrameworkID.
```

It is up to the implementor to perform scheduling logic of tasks.  The time spent handling events must be minimal otherwise delay in acknowledging or declining offers can have a negative impact on the cluster at scale.

## TODO
* Authentication
* ZK Leader Discovery