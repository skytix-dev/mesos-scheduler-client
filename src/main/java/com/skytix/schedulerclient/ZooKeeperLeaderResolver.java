package com.skytix.schedulerclient;

public class ZooKeeperLeaderResolver implements LeaderResolver {

    @Override
    public String resolveLeader() throws NoLeaderException {
        throw new UnsupportedOperationException();
    }

}
