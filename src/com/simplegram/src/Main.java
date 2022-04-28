package com.simplegram.src;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {
        // /Users/George/Documents/GitHub/IBC/src/addr.txt
        Broker b = new Broker("addr.txt");
        b.startBroker();
    }
}