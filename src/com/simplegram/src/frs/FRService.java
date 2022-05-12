package com.simplegram.src.frs;

import com.simplegram.src.Topic;
import com.simplegram.src.cbt.UserHandler;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;

public class FRService extends Thread {

    private ServerSocket frsSocket;
    private HashMap<String, Topic> topics;
    private ArrayList<InetAddress> brokerAddresses;

    public FRService(ArrayList<InetAddress> brokerAddresses, HashMap<String, Topic> topics){
        this.brokerAddresses = brokerAddresses;
        this.topics = topics;
    }

    @Override
    public void run() {
        // Bind server tcp socket:
        try {
            this.frsSocket = new ServerSocket(5002);

            while(true){
                Socket frsBrokerSocket = this.frsSocket.accept();
                FRSRequestHandler frsRequestHandler = new FRSRequestHandler(frsBrokerSocket, this.brokerAddresses, this.topics);
                frsRequestHandler.start();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
