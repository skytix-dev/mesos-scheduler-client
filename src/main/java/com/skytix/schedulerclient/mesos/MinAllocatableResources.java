package com.skytix.schedulerclient.mesos;

import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class MinAllocatableResources {
    private Double cpu;
    private Double mem;
    private Double gpu;
    private Double disk;
}
