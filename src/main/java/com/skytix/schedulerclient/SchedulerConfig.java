package com.skytix.schedulerclient;

import com.skytix.schedulerclient.mesos.MinAllocatableResources;
import lombok.Builder;
import lombok.Getter;
import lombok.experimental.SuperBuilder;

import java.util.List;

@SuperBuilder(toBuilder = true)
@Getter
public class SchedulerConfig {
    private String mesosMasterURL;
    private String frameworkID;
    private String user;
    private String name;
    @Builder.Default
    private double failoverTimeout = 86400; // Default of 1 day before framework is marked as completed.
    private List<String> roles;
    @Builder.Default
    private boolean disableSSLTrust = false;
    @Builder.Default
    private boolean enableGPUResources = false;
    @Builder.Default
    private double minAllocatableCpu = 0;
    @Builder.Default
    private double minAllocatableMem = 0;
    @Builder.Default
    private double minAllocatableGpu = 0.0;
    @Builder.Default
    private double minAllocatableDisk = 0.0;
}
