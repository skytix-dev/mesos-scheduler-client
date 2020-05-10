package com.skytix.schedulerclient;

import org.apache.mesos.v1.Protos;
import static org.apache.mesos.v1.Protos.*;
import static org.apache.mesos.v1.scheduler.Protos.*;

import java.io.IOException;
import java.util.List;

public final class SchedulerRemote {
    private final Scheduler mScheduler;

    protected SchedulerRemote(Scheduler aScheduler) {
        mScheduler = aScheduler;
    }

    public void exit() throws IOException {
        mScheduler.close();
    }

    public String getMesosMasterURL() {
        return mScheduler.getMesosMasterURL();
    }

    public FrameworkID getFrameworkID() {
        return mScheduler.getFrameworkID();
    }

    public void accept(List<OfferID> aOfferIDs, List<Offer.Operation> aOperations) {
        accept(aOfferIDs, aOperations, null);
    }

    public void accept(List<OfferID> aOfferIDs, List<Offer.Operation> aOperations, Double aRefuseSeconds) {

        final Call.Accept.Builder acceptBuilder = Call.Accept.newBuilder()
                .addAllOfferIds(aOfferIDs)
                .addAllOperations(aOperations);

        if (aRefuseSeconds != null) {

            acceptBuilder.setFilters(
                    Filters.newBuilder()
                            .setRefuseSeconds(aRefuseSeconds)
            );

        }

        mScheduler.sendCall(
                mScheduler.createCall(Call.Type.ACCEPT)
                        .setAccept(acceptBuilder)
                        .build()
        );

    }

    public void acknowledge(Protos.TaskStatusOrBuilder aTaskStatus) {

        mScheduler.sendCall(
                mScheduler.createCall(Call.Type.ACKNOWLEDGE)
                        .setAcknowledge(
                                Call.Acknowledge.newBuilder()
                                        .setAgentId(aTaskStatus.getAgentId())
                                        .setTaskId(aTaskStatus.getTaskId())
                                        .setUuid(aTaskStatus.getUuid())
                        ).build()
        );

    }

    public void acknowledgeOperationStatus(Protos.OperationStatusOrBuilder aOperationStatus) {

        mScheduler.sendCall(
                createCall(Call.Type.ACKNOWLEDGE_OPERATION_STATUS)
                        .setAcknowledgeOperationStatus(
                                Call.AcknowledgeOperationStatus.newBuilder()
                                        .setAgentId(aOperationStatus.getAgentId())
                                        .setResourceProviderId(aOperationStatus.getResourceProviderId())
                                        .setUuid(aOperationStatus.getUuid().getValue())
                                        .setOperationId(aOperationStatus.getOperationId())
                        ).build()
        );

    }

    public void decline(List<OfferID> aOfferIDs) {
        decline(aOfferIDs, null);
    }

    public void decline(List<org.apache.mesos.v1.Protos.OfferID> aOfferIDs, Double aRefuseSeconds) {

        final Call.Decline.Builder declineBuilder = Call.Decline.newBuilder()
                .addAllOfferIds(aOfferIDs);

        if (aRefuseSeconds != null) {

            declineBuilder.setFilters(
                    Filters.newBuilder()
                            .setRefuseSeconds(aRefuseSeconds)
            );

        }

        mScheduler.sendCall(
                createCall(Call.Type.DECLINE)
                        .setDecline(declineBuilder)
                        .build()
        );

    }

    public void kill(TaskID aTaskID, AgentID aAgentID) {

        mScheduler.sendCall(
                createCall(Call.Type.KILL)
                        .setKill(
                                Call.Kill.newBuilder()
                                        .setTaskId(aTaskID)
                                        .setAgentId(aAgentID)
                        ).build()
        );
    }

    public void reconcile(List<Call.Reconcile.Task> aTasks) {

        mScheduler.sendCall(
                createCall(Call.Type.RECONCILE)
                        .setReconcile(
                                Call.Reconcile.newBuilder()
                                        .addAllTasks(aTasks).build()
                        )
                .build()
        );
    }

    public void reconcileOperations(List<Call.ReconcileOperations.Operation> aOperations) {

        mScheduler.sendCall(
                createCall(Call.Type.RECONCILE_OPERATIONS)
                .setReconcileOperations(
                        Call.ReconcileOperations.newBuilder()
                                .addAllOperations(aOperations)
                                .build()
                ).build()
        );

    }

    public void revive(List<String> aRoles) {

        mScheduler.sendCall(
                createCall(Call.Type.REVIVE)
                .setRevive(
                        Call.Revive.newBuilder()
                                .addAllRoles(aRoles)
                                .build()
                ).build()
        );

    }

    public void suppress(List<String> aRoles) {

        mScheduler.sendCall(
                createCall(Call.Type.SUPPRESS)
                        .setSuppress(
                                Call.Suppress.newBuilder()
                                        .addAllRoles(aRoles)
                                        .build()
                        )
                        .build()

        );

    }

    public void teardown() throws IOException {
        mScheduler.sendCall(mScheduler.createCall(Call.Type.TEARDOWN).build());
        mScheduler.close();
    }

    private Call.Builder createCall(Call.Type aType) {
        return mScheduler.createCall(aType);
    }

}
