package com.simplegram.src;

import java.io.IOException;

public class DemoBroker {
    public static void main(String[] args) throws IOException {
        Broker b = new Broker("addr.txt");
        b.startBroker();
    }
}