package com.simplegram.src.ibc;

import java.net.InetAddress;

public class BrokerConnection {
    private final InetAddress broker_addr;
    private boolean isActive;
    private long lastTimeActive;

    public BrokerConnection(InetAddress addr){
        this.broker_addr = addr;
        this.isActive = false;
        this.lastTimeActive = -1;
    }

    public InetAddress getBrokerAddress() {
        return this.broker_addr;
    }

    public boolean isActive() {
        return this.isActive;
    }

    public void setActive() {
        this.isActive = true;
        this.lastTimeActive = System.currentTimeMillis();
    }

    public void setDead() {
        this.isActive = false;
    }

    public long getLastTimeActive() {
        return this.lastTimeActive;
    }



}

