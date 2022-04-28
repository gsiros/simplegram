package com.simplegram.src;

import com.simplegram.src.MD5.MD5;
import com.simplegram.src.cbt.UserHandler;
import com.simplegram.src.ibc.BrokerConnection;
import com.simplegram.src.ibc.ReceiveHandler;
import com.simplegram.src.ibc.SendHandler;
import com.simplegram.src.logging.TerminalColors;

import java.io.*;
import java.math.BigInteger;
import java.net.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;

public class Broker {
    private int brokerID;
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
                String line = sc.nextLine();
                String[] data = line.split(",");
                if (data[1].equals("localhost")){
                    this.brokerID = Integer.parseInt(data[0]);
                }else {
                    InetAddress ia = InetAddress.getByName(data[1]);
                    this.brokerAddresses.add(ia);
                    this.brokerConnections.put(ia, new BrokerConnection(Integer.parseInt(data[0]), ia));
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    /**
     * calculates hashCodes of a given topic.
     * returns the suitable broker.
     */

    private int getAssignedBroker(String topicName){
        String hashedInput = MD5.hash(topicName);
        BigInteger topicDec = new BigInteger(hashedInput,16);

        //we use modulo 3, because we will have 3 brokers.
        //we need to spread the topics equally to the brokers.
        int brokerToAssign = topicDec.mod(BigInteger.valueOf(this.brokerConnections.size())).intValue();

        //TODO: fault tolerance



        return brokerToAssign;
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
        StoryChecker sc = new StoryChecker(this.topics);
        sc.start();

        // Start CBT service. -- WARNING, while-true loop in startCBT.
        this.startCommunicationBetweenTerminals();
    }


}