package com.simplegram.src;

import com.simplegram.src.MD5.MD5;
import com.simplegram.src.cbt.UserHandler;
import com.simplegram.src.frs.FRSController;
import com.simplegram.src.ibc.BrokerConnection;
import com.simplegram.src.ibc.ReceiveHandler;
import com.simplegram.src.ibc.SendHandler;

import java.io.*;
import java.math.BigInteger;
import java.net.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;

public class Broker {
    // variable to store Broker ID.
    private int brokerID;
    // Data structure to hold all the topics
    // that the broker is responsible for.
    // ex. topics['topic_name'] -> Topic()
    private HashMap<String, Topic> topics;


    // IBC (InterBroker Communication) protocol
    private DatagramSocket ibcSocket;
    private ArrayList<InetAddress> brokerAddresses;
    private HashMap<InetAddress, BrokerConnection> brokerConnections;

    // CBT (Communication Between Terminals) protocol
    private ServerSocket cbtSocket;

    private FRSController frsController;

    public Broker(int brokerID){
        // Initialization of data structures:
        this.brokerID = brokerID;
        this.topics = new HashMap<String, Topic>();
        this.brokerAddresses = new ArrayList<InetAddress>();
        this.brokerConnections = new HashMap<InetAddress,BrokerConnection>();
        this.frsController = new FRSController(this.topics, this.brokerAddresses, this.brokerConnections);
    }

    /**
     * This method reads the ID and IP address for every broker
     * node in the system.
     * @param filename - the path of the configuration file.
     * @return Nothing.
     */
    public void init(String filename){
        File f = new File(filename);
        try {
            Scanner sc = new Scanner(f);
            while(sc.hasNextLine()){
                String line = sc.nextLine();
                String[] data = line.split(",");
                if (Integer.parseInt(data[0]) != this.brokerID){
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
     * This method is used to hash the name of the topic and
     * assign it to one of the available brokers on the network
     * leveraging modular arithmetic.
     * @param topicName - the name of the topic in string format.
     * @return id of responsible broker.
     */
    public int getAssignedBroker(String topicName){
        String hashedInput = MD5.hash(topicName);
        BigInteger topicDec = new BigInteger(hashedInput,16);

        //we use modulo 3, because we will have 3 brokers.
        //we need to spread the topics equally to the brokers.
        int brokerIDToAssign = topicDec.mod(BigInteger.valueOf(this.brokerConnections.size()+1)).intValue();
        BrokerConnection brokerToAssign;
        if(brokerIDToAssign != this.brokerID){
            synchronized (this.brokerConnections){
                brokerToAssign = this.brokerConnections.get(this.brokerAddresses.get(brokerIDToAssign));
                while(!brokerToAssign.isActive()){
                    brokerIDToAssign = (brokerIDToAssign + 1) % (this.brokerConnections.size()+1);
                    brokerToAssign = this.brokerConnections.get(this.brokerAddresses.get(brokerIDToAssign));
                }
            }
        }
        return brokerIDToAssign;

    }



    /**
     * This method is used to start the IBC (InterBroker Communication)
     * service. It binds the service's UDP socket on port 4444 and starts
     * the threads responsible for handling incoming 'ALIVE' messages from
     * other brokers. Finally, it starts a daemon thread that checks periodically
     * if a broker is dead or alive.
     *
     * @throws SocketException
     */
    private void startInterBrokerCommunication() throws SocketException {
        this.ibcSocket = new DatagramSocket(4444);

        ConnectionChecker cc = new ConnectionChecker(this, this.brokerConnections, this.topics);
        cc.start();

        ReceiveHandler rh = new ReceiveHandler(this.ibcSocket, this.brokerConnections);
        rh.start();

        for(InetAddress addr : this.brokerAddresses){
            SendHandler sh = new SendHandler(this.ibcSocket, addr);
            sh.start();
        }
    }

    /**
     * This method is used to start the CBT (Communication Between Terminals)
     * service. It binds a TCP server socket on port 5001 and listens for incoming
     * UserNode connections. If a connection is established, the method starts a
     * 'UserHandler' thread to handle the UserNode request.
     *
     * @throws IOException
     */
    private void startCommunicationBetweenTerminals() throws IOException {

        // Bind server tcp socket:
        this.cbtSocket = new ServerSocket(5001);

        while(true){
            Socket userConSocket = this.cbtSocket.accept();
            UserHandler uh = new UserHandler(this, userConSocket, this.topics);
            uh.start();
        }

    }

    /**
     * This method starts the two fundamental services (IBC & CBT) and starts
     * a thread to check if a story has expired.
     *
     * @throws IOException
     */
    public void startBroker() throws IOException {

        // Start IBC service.
        this.startInterBrokerCommunication();
        StoryChecker sc = new StoryChecker(this.topics);
        sc.start();

        this.frsController.startFaultRecoveryService();

        // Start CBT service. -- WARNING, while-true loop in startCBT.
        this.startCommunicationBetweenTerminals();
    }

    public int getBrokerID() {
        return brokerID;
    }

    public FRSController getFRS() {
        return this.frsController;
    }
}