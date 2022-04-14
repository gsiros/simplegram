package com.simplegram.src;

import java.net.Socket;

public class ActionsForUser extends Thread {
    private Socket userConnection;

    public ActionsForUser(Socket userConnection) {
    }

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
