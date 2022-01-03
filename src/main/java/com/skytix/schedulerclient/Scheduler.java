package com.skytix.schedulerclient;

import com.skytix.schedulerclient.mesos.MesosConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.mesos.v1.scheduler.Protos;
import org.apache.mesos.v1.scheduler.Protos.Event;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.UUID;
import java.util.concurrent.*;

import static org.apache.mesos.v1.Protos.*;

@Slf4j
public final class Scheduler implements Closeable {
    private static final Semaphore mSemaphore = new Semaphore(0);

    private final FrameworkID mFrameworkId;
    private final SchedulerConfig mConfig;
    private final HttpClient mHttpClient;
    private final SchedulerEventHandler mSchedulerEventHandler;
    private final LeaderResolver mLeaderResolver;

    private ScheduledExecutorService mExecutorService = null;
    private SchedulerRemote mRemote;
    private String mMesosStreamID = null;
    private String mMasterURL = null;
    private ScheduledFuture<?> mClientThread;
    private FrameworkInfo mFrameworkInfo;
    private boolean mRunning = true;

    public static Scheduler newScheduler(String aFrameworkId, String aMesosMasterURI, SchedulerEventHandler aEventHandler) throws IOException {

        return newScheduler(
                SchedulerConfig.builder()
                    .frameworkID(aFrameworkId)
                    .mesosMasterURL(aMesosMasterURI)
                .build(),
                aEventHandler
        );

    }

    public static Scheduler newScheduler(SchedulerConfig aConfig, SchedulerEventHandler aEventHandler) throws IOException {
        return newScheduler(aConfig, aEventHandler, Executors.newScheduledThreadPool(1));
    }

    public static Scheduler newScheduler(SchedulerConfig aConfig, SchedulerEventHandler aEventHandler, ScheduledExecutorService aExecutorService) throws IOException {
        final Scheduler scheduler = new Scheduler(aConfig, aEventHandler);
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

        final HttpClient.Builder httpClientBuilder = HttpClient.newBuilder();

        if (mConfig.isDisableSSLTrust()) {

            try {
                final SSLContext sslContext = SSLContext.getInstance("TLS");

                sslContext.init(null, trustAllCerts, new SecureRandom());
                httpClientBuilder.sslContext(sslContext);

            } catch (NoSuchAlgorithmException | KeyManagementException aE) {
                log.error("Unable to set SSLContext.  Skipping disabling of SSL Trust checking.");
            }

        }

        mHttpClient = httpClientBuilder.build();

        final String masterURL = aConfig.getMesosMasterURL();

        if (StringUtils.isNotEmpty(masterURL)) {

            if (masterURL.startsWith("zk")) {
                mLeaderResolver = new ZooKeeperLeaderResolver();

            } else {
                mLeaderResolver = new HttpLeaderResolver(masterURL, mHttpClient);
            }

        } else {
            throw new IllegalArgumentException("mesosMasterURL configuration is required");
        }

    }

    private void init(ScheduledExecutorService aThreadExecutorService) throws IOException {
        // Discover the Mesos leader from ZK.
        mExecutorService = aThreadExecutorService;

        try {
            mFrameworkInfo = createFrameworkInfo(mFrameworkId).build();

            final Protos.Call subscribeCall = Protos.Call.newBuilder()
                    .setFrameworkId(mFrameworkId)
                    .setType(Protos.Call.Type.SUBSCRIBE)
                    .setSubscribe(
                            Protos.Call.Subscribe.newBuilder()
                                    .setFrameworkInfo(mFrameworkInfo)
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
                log.info(String.format("Connected with Stream ID: %s", mMesosStreamID));
                mMasterURL = leader;
                mRemote = new SchedulerRemote(this);

                mClientThread = mExecutorService.schedule(() -> {

                    try {
                        final InputStream reader = new BufferedInputStream(response.body());
                        boolean subscribed = false;

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
                                        mSchedulerEventHandler.onSubscribe(mRemote, event.getSubscribed());
                                        subscribed = true;
                                        log.info(String.format("Connected to Master as FrameworkID: %s", mFrameworkId.getValue()));
                                        break;

                                    case ERROR:

                                        if (!subscribed) {
                                            final String error = String.format("Error subscribing to Mesos: %s", event.getMessage());
                                            log.error(error);
                                            mSchedulerEventHandler.onTerminate(new IllegalStateException(error));

                                        } else {
                                            mSchedulerEventHandler.handleEvent(event);
                                        }

                                        break;

                                    default:
                                        mSchedulerEventHandler.handleEvent(event);
                                        break;
                                }

                                sb = new StringBuffer();
                                data = reader.read();

                            } else {
                                sb.append(new String(new byte[]{(byte) data}));
                                data = reader.read();
                            }

                        }

                        if (mRunning) {
                            log.info(String.format("Scheduler '%s' has lost it's connection to Mesos '%s'", mFrameworkId, mMasterURL));
                            mSchedulerEventHandler.onDisconnect();

                        } else {
                            mSchedulerEventHandler.onExit();
                        }

                    } catch (IOException aE) {
                        mSchedulerEventHandler.onTerminate(aE);

                    } finally {
                        mSemaphore.release();
                    }

                }, 0, TimeUnit.SECONDS);

            } else {
                final ByteArrayOutputStream bo = new ByteArrayOutputStream();
                response.body().transferTo(bo);

                throw new IOException(String.format("Scheduler was unable to connect to mesos with exit code %d - %s", response.statusCode(), bo));
            }

        } catch (URISyntaxException | InterruptedException | NoLeaderException aE) {
            throw new IOException(aE);
        }

    }

    public void join() throws InterruptedException {
        mSemaphore.acquire();
    }

    public String getMesosMasterURL() {
        return mMasterURL;
    }

    public SchedulerRemote getRemote() {
        return mRemote;
    }

    protected FrameworkInfo.Builder createFrameworkInfo(FrameworkID aFrameworkID) {
        final FrameworkInfo.Builder frameworkInfo = FrameworkInfo.newBuilder()
                .setId(aFrameworkID);

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

        if (mConfig.getRoles() != null && !mConfig.getRoles().isEmpty()) {
            frameworkInfo.addAllRoles(mConfig.getRoles());
        }

        final OfferFilters.MinAllocatableResources.Builder minSchedulerResources = OfferFilters.MinAllocatableResources.newBuilder();

        final double minAllocatableCpu = mConfig.getMinAllocatableCpu();
        final double minAllocatableMem = mConfig.getMinAllocatableMem();
        final double minAllocatableDisk = mConfig.getMinAllocatableDisk();

        if (minAllocatableCpu > 0) {

            minSchedulerResources.addQuantities(
                    OfferFilters.ResourceQuantities.newBuilder()
                    .putQuantities(MesosConstants.SCALAR_CPU, Value.Scalar.newBuilder().setValue(minAllocatableCpu).build())
            );
        }

        if (minAllocatableMem > 0) {

            minSchedulerResources.addQuantities(
                    OfferFilters.ResourceQuantities.newBuilder()
                            .putQuantities(MesosConstants.SCALAR_MEM, Value.Scalar.newBuilder().setValue(minAllocatableMem).build())
            );
        }

        if (minAllocatableDisk > 0) {

            minSchedulerResources.addQuantities(
                    OfferFilters.ResourceQuantities.newBuilder()
                            .putQuantities(MesosConstants.SCALAR_DISK, Value.Scalar.newBuilder().setValue(minAllocatableDisk).build())
            );
        }

        if (mConfig.isEnableGPUResources()) {
            final FrameworkInfo.Capability.Builder capabilityBuilder = FrameworkInfo.Capability.newBuilder();
            capabilityBuilder.setType(FrameworkInfo.Capability.Type.GPU_RESOURCES);

            frameworkInfo.addCapabilities(capabilityBuilder);

            final double minAllocatableGpu = mConfig.getMinAllocatableGpu();

            if (minAllocatableGpu > 0) {
                minSchedulerResources.addQuantities(
                        OfferFilters.ResourceQuantities.newBuilder()
                                .putQuantities(MesosConstants.SCALAR_GPU, Value.Scalar.newBuilder().setValue(minAllocatableGpu).build())
                );
            }

        }

        //TODO: Make it possible to set min resources per offer.  For now, it's all.
        if (minSchedulerResources.getQuantitiesCount() > 0) {
            frameworkInfo.putOfferFilters(MesosConstants.ROLE_ALL, OfferFilters.newBuilder().setMinAllocatableResources(minSchedulerResources).build());
        }

        return frameworkInfo;
    }

    @Override
    public void close() throws IOException {
        mRunning = false;
        mExecutorService.shutdown();
        mClientThread.cancel(false);
    }

    @Deprecated
    protected FrameworkID getFrameworkID() {
        return mFrameworkInfo.getId();
    }

    public FrameworkInfo getFrameworkInfo() {
        return mFrameworkInfo;
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

            if (response.statusCode() != 202 && response.statusCode() != 200) {
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

    private static TrustManager[] trustAllCerts = new TrustManager[] {

            new X509TrustManager() {

                public X509Certificate[] getAcceptedIssuers() {
                    return null;
                }

                public void checkClientTrusted(
                        X509Certificate[] certs, String authType) {
                }

                public void checkServerTrusted(
                        X509Certificate[] certs, String authType) {
                }
            }
    };

}
