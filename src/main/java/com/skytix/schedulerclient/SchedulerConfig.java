package com.skytix.schedulerclient;

import lombok.Builder;
import lombok.Getter;
import org.apache.mesos.v1.Protos;

import java.util.List;

@Builder
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

}
