package com.simplegram.src;

import java.io.*;
import java.net.Socket;

public class ActionsForUser extends Thread {
    private Socket userConnection;
    private ObjectOutputStream out;
    private ObjectInputStream in;
    private boolean isConnected;

    public ActionsForUser(Socket userConnection) {
        try {
            out = new ObjectOutputStream(userConnection.getOutputStream());
            in = new ObjectInputStream(userConnection.getInputStream());
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }

    void connect() {}
    void subscribe() {}
    void push() {}
    void pull() {}

    @Override
    public void run() {
        // IMPLEMENT COMMUNICATION BETWEEN TERMINALS (CBT) PROTOCOL
        /*
        HANDLE ACTIONS OF CBT:
        - CONNECT
        - PULL
        - PUSH
        - SUBSCRIBE
        - UNSUBSCRIBE
         */
    }
}
