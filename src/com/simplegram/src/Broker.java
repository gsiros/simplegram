package com.simplegram.src;

import com.simplegram.src.cbt.UserHandler;
import com.simplegram.src.ibc.BrokerConnection;
import com.simplegram.src.ibc.ReceiveHandler;
import com.simplegram.src.ibc.SendHandler;

import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;

public class Broker {

    // Broker data structures
    private HashMap<String, Topic> topics;

    // IBC protocol
    private DatagramSocket ibcSocket;
    private ArrayList<InetAddress> brokerAddresses;
    private HashMap<InetAddress, BrokerConnection> brokerConnections;

    // CBT protocol
    private ServerSocket cbtSocket;

    public Broker(String brokers_addr_file){
        this.topics = new HashMap<String, Topic>();
        this.brokerAddresses = new ArrayList<InetAddress>();
        this.brokerConnections = new HashMap<InetAddress,BrokerConnection>();
        readBrokers(brokers_addr_file);
    }

    private void readBrokers(String filename){
        File f = new File(filename);
        try {
            Scanner sc = new Scanner(f);
            while(sc.hasNextLine()){
                InetAddress ia = InetAddress.getByName(sc.nextLine());
                this.brokerAddresses.add(ia);
                this.brokerConnections.put(ia, new BrokerConnection(ia));
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    /**
     * startInterBrokerCommunication:
     *
     * starts IBC protocol.
     *
     *
     * @throws SocketException
     */
    private void startInterBrokerCommunication() throws SocketException {
        this.ibcSocket = new DatagramSocket(4444);

        ConnectionChecker cc = new ConnectionChecker(this.brokerConnections);
        cc.start();

        ReceiveHandler rh = new ReceiveHandler(this.ibcSocket, this.brokerConnections);
        rh.start();

        for(InetAddress addr : this.brokerAddresses){
            SendHandler sh = new SendHandler(this.ibcSocket, addr);
            sh.start();
        }
    }

    private void startCommunicationBetweenTerminals() throws IOException {

        // Bind server tcp socket:
        this.cbtSocket = new ServerSocket(5001);

        while(true){
            Socket userConSocket = this.cbtSocket.accept();
            UserHandler uh = new UserHandler(userConSocket, this.topics);
            uh.start();
        }

    }

    public void startBroker() throws IOException {

        this.topics.put("test", new Topic("test"));

        // Start IBC service.
        this.startInterBrokerCommunication();
        // Start CBT service. -- WARNING, while-true loop in startCBT.
        this.startCommunicationBetweenTerminals();
    }
}