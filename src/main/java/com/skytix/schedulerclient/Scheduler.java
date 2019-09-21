package com.skytix.schedulerclient;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.mesos.v1.scheduler.Protos;
import org.apache.mesos.v1.scheduler.Protos.Event;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.UUID;
import java.util.concurrent.*;

import static org.apache.mesos.v1.Protos.*;

@Slf4j
public final class Scheduler implements Closeable {
    private static final Semaphore mSemaphore = new Semaphore(0);

    private final FrameworkID mFrameworkId;
    private final SchedulerConfig mConfig;
    private final HttpClient mHttpClient = HttpClient.newHttpClient();
    private final SchedulerEventHandler mSchedulerEventHandler;
    private final LeaderResolver mLeaderResolver;

    private ScheduledExecutorService mExecutorService = null;
    private String mMesosStreamID = null;
    private String mMasterURL = null;
    private ScheduledFuture<?> mClientThread;
    private boolean mRunning = true;

    public static Scheduler newScheduler(String aFrameworkId, String aMesosMasterURI, SchedulerEventHandler aEventHandler) {

        return newScheduler(
                new SchedulerConfig.SchedulerConfigBuilder()
                .frameworkID(aFrameworkId)
                .mesosMasterURL(aMesosMasterURI),
                aEventHandler
        );

    }

    public static Scheduler newScheduler(SchedulerConfig.SchedulerConfigBuilder aConfig, SchedulerEventHandler aEventHandler) {
        return newScheduler(aConfig, aEventHandler, Executors.newScheduledThreadPool(1));
    }

    public static Scheduler newScheduler(SchedulerConfig.SchedulerConfigBuilder aConfig, SchedulerEventHandler aEventHandler, ScheduledExecutorService aExecutorService) {
        final Scheduler scheduler = new Scheduler(aConfig.build(), aEventHandler);
        scheduler.init(aExecutorService);

        return scheduler;
    }

    private Scheduler(SchedulerConfig aConfig, SchedulerEventHandler aEventHandler) {
        final FrameworkID.Builder frameworkID = FrameworkID.newBuilder();

        if (StringUtils.isEmpty(aConfig.getFrameworkID())) {
            frameworkID.setValue(UUID.randomUUID().toString());

        } else {
            frameworkID.setValue(aConfig.getFrameworkID());
        }

        mConfig = aConfig;
        mFrameworkId = frameworkID.build();
        mSchedulerEventHandler = aEventHandler;
        final String masterURL = aConfig.getMesosMasterURL();

        if (StringUtils.isNotEmpty(masterURL)) {

            if (masterURL.startsWith("zk")) {
                mLeaderResolver = new ZooKeeperLeaderResolver();

            } else {
                mLeaderResolver = new HttpLeaderResolver(masterURL);
            }

        } else {
            throw new IllegalArgumentException("mesosMasterURL configuration is required");
        }

    }

    private void init(ScheduledExecutorService aThreadExecutorService) {
        // Discover the Mesos leader from ZK.
        mExecutorService = aThreadExecutorService;

        mClientThread = mExecutorService.schedule(() -> {

            try {
                final FrameworkInfo.Builder frameworkInfo = createFrameworkInfo();

                final Protos.Call subscribeCall = Protos.Call.newBuilder()
                        .setFrameworkId(mFrameworkId)
                        .setType(Protos.Call.Type.SUBSCRIBE)
                        .setSubscribe(
                                Protos.Call.Subscribe.newBuilder()
                                        .setFrameworkInfo(frameworkInfo)
                        )
                        .build();

                final String leader = mLeaderResolver.resolveLeader();

                final URI leaderUri = new URI(leader + "/api/v1/scheduler");

                final HttpRequest request = HttpRequest.newBuilder()
                        .uri(leaderUri)
                        .header("Content-Type", "application/x-protobuf")
                        .header("Accept", "application/x-protobuf")
                        .POST(HttpRequest.BodyPublishers.ofByteArray(subscribeCall.toByteArray()))
                        .build();

                log.info(String.format("Connecting to Mesos at: %s", leaderUri));

                final HttpResponse<InputStream> response = mHttpClient.send(request, HttpResponse.BodyHandlers.ofInputStream());

                if (response.statusCode() == 200) {
                    mMesosStreamID = response.headers().firstValue("Mesos-Stream-Id").get();
                    mMasterURL = leader;

                    final InputStream reader = new BufferedInputStream(response.body());

                    StringBuffer sb = new StringBuffer();
                    int data = reader.read();

                    while (data != -1 && mRunning) {

                        if (data == 10) {
                            // Contents of the StringBuffer should have the length of bytes to read.
                            final long recordLength = Long.parseLong(sb.toString());
                            final byte[] buffer = reader.readNBytes((int) recordLength);

                            final Event event = Event.parseFrom(buffer);

                            switch (event.getType()) {

                                case SUBSCRIBED:
                                    mSchedulerEventHandler.onSubscribe(new SchedulerRemote(this));
                                    log.info(String.format("Connected to Master as FrameworkID: %s", mFrameworkId.getValue()));
                                    break;

                                case ERROR:
                                    final String error = String.format("Error subscribing to Mesos: %s", event.getMessage());
                                    log.error(error);
                                    mSchedulerEventHandler.onTerminate(new IllegalStateException(error));
                                    return;

                                default:

                                    try {
                                        mSchedulerEventHandler.handleEvent(event);

                                    } catch (Exception aE) {
                                        log.error(aE.getMessage(), aE);
                                    }

                            }

                            sb = new StringBuffer();
                            data = reader.read();

                        } else {
                            sb.append(new String(new byte[]{(byte) data}));
                            data = reader.read();
                        }

                    }

                    if (mRunning) {
                        log.info(String.format("Scheduler '%s' %s has lost it's connection to Mesos"));
                        mSchedulerEventHandler.onDisconnect();

                    } else {
                        mSchedulerEventHandler.onExit();
                    }

                } else {
                    mSchedulerEventHandler.onTerminate(new Exception(String.format("Scheduler was unable to connect to mesos with exit code %d", response.statusCode())));
                    log.error("Error subscribing to Mesos");
                }

            } catch (URISyntaxException | IOException | InterruptedException | NoLeaderException aE) {
                mSchedulerEventHandler.onTerminate(aE);
                log.error(aE.getMessage(), aE);

            } finally {
                mSemaphore.release();
            }

        }, 0, TimeUnit.SECONDS);

    }

    public void join() throws InterruptedException {
        mSemaphore.acquire();
    }

    private FrameworkInfo.Builder createFrameworkInfo() {
        final FrameworkInfo.Builder frameworkInfo = FrameworkInfo.newBuilder()
                .setId(mFrameworkId);

        if (StringUtils.isBlank(mConfig.getUser())) {
            frameworkInfo.setUser("root");

        } else {
            frameworkInfo.setUser(mConfig.getUser());
        }

        if (StringUtils.isBlank(mConfig.getName())) {
            frameworkInfo.setName("mesos-scheduler-client");

        } else {
            frameworkInfo.setName(mConfig.getName());
        }

        if (mConfig.getFailoverTimeout() > 0) {
            frameworkInfo.setFailoverTimeout(mConfig.getFailoverTimeout());
        }

        return frameworkInfo;
    }

    @Override
    public void close() throws IOException {
        mRunning = false;
        mClientThread.cancel(false);
    }

    protected FrameworkID getFrameworkID() {
        return mFrameworkId;
    }

    protected void sendCall(Protos.Call aCall) {

        try {
            final HttpRequest request = HttpRequest.newBuilder()
                    .uri(new URI(mMasterURL + "/api/v1/scheduler"))
                    .header("Content-Type", "application/x-protobuf")
                    .header("Mesos-Stream-Id", mMesosStreamID)
                    .POST(HttpRequest.BodyPublishers.ofByteArray(aCall.toByteArray()))
                    .build();

            final HttpResponse<String> response = mHttpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 202) {
                log.error("Error sending call to Mesos: " + response.body());
            }

        } catch (URISyntaxException | IOException | InterruptedException aE) {
            aE.printStackTrace();
        }

    }

    protected Protos.Call.Builder createCall(Protos.Call.Type aType) {

        return Protos.Call.newBuilder()
                .setFrameworkId(mFrameworkId)
                .setType(aType);

    }

}
