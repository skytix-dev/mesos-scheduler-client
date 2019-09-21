package com.skytix.schedulerclient;

public interface LeaderResolver {
    public String resolveLeader() throws NoLeaderException;
}
