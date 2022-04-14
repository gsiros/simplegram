package com.simplegram.src;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;

public class Broker {

    ArrayList<UserNode> connectedUsers;
    HashMap<String, Topic> topics;

    ServerSocket userServiceProviderSocket;
    ServerSocket brokerServiceProviderSocket;

    Socket brokerConnection;
    Socket userConnection;

    // init broker to become ready for
    // new connections with usernodes
    public Broker(){

    }

    void init() {

    }

    // begin the main functionality,
    // start accepting connections.
    void start(){
        try {
            userServiceProviderSocket = new ServerSocket(4444);
            brokerServiceProviderSocket = new ServerSocket(4445);

            while (true) {

                // TODO: HANDLE CONNECTIONS; PART 1
                brokerConnection = brokerServiceProviderSocket.accept();
                Thread t_b = new ActionsForBroker(brokerConnection);
                t_b.start();


                // TODO: HANDLE CONNECTIONS; PART 2
                // Accept incoming connection FROM USERNODE.
                userConnection = userServiceProviderSocket.accept();

                Thread t = new ActionsForUser(userConnection);
                t.start();

            }
        } catch (IOException ioException) {
            ioException.printStackTrace();
        } finally {
            try {
                userServiceProviderSocket.close();
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        }
    }

}
