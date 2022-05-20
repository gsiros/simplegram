package com.simplegram.src.frs;

import com.simplegram.src.*;
import com.simplegram.src.ibc.BrokerConnection;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

/**
 * Failure Recovery System (FRS) Controller class.
 */
public class FRSController {

    private HashMap<String, Topic> topics;
    private ArrayList<InetAddress> brokerAddresses;
    private HashMap<InetAddress, BrokerConnection> brokerConnections;

    private HashMap<InetAddress, ArrayList<Thread>> outgoingRequests;

    // Public Interface
    public FRSController(
            HashMap<String, Topic> topics,
            ArrayList<InetAddress> brokerAddresses,
            HashMap<InetAddress, BrokerConnection> brokerConnections
    ) {
        this.topics = topics;
        this.brokerAddresses = brokerAddresses;
        this.brokerConnections = brokerConnections;
        this.outgoingRequests = new HashMap<InetAddress, ArrayList<Thread>>();
        for(InetAddress ie : this.brokerAddresses){
            this.outgoingRequests.put(ie, new ArrayList<Thread>());
        }
    }

    public void broadcastSub(String username, String topicname){
        for (BrokerConnection br : this.brokerConnections.values()){
            SubToTopicHandler subToTopicHandler = new SubToTopicHandler(
                    br,
                    this.topics,
                    username,
                    topicname
            );
            ArrayList<Thread> outgoings = this.outgoingRequests.get(br.getBrokerAddress());
            synchronized (outgoings){
                outgoings.add(subToTopicHandler);
            }
        }
    }

    public void broadcastUnSub(String username, String topicname){
        for (BrokerConnection br : this.brokerConnections.values()){
            UnsubFromTopicHandler unsubFromTopicHandler = new UnsubFromTopicHandler(
                    br,
                    this.topics,
                    username,
                    topicname
            );
            ArrayList<Thread> outgoings = this.outgoingRequests.get(br.getBrokerAddress());
            synchronized (outgoings){
                outgoings.add(unsubFromTopicHandler);
            }
        }
    }

    public void broadcastPush(String username, String topicname, Value value){
        for (BrokerConnection br : this.brokerConnections.values()) {
            PushHandler pushRequest = new PushHandler(
                    br,
                    this.topics,
                    username,
                    topicname,
                    value
            );
            ArrayList<Thread> outgoings = this.outgoingRequests.get(br.getBrokerAddress());
            synchronized (outgoings){
                outgoings.add(pushRequest);
            }
        }
    }
    
    public void startFaultRecoverySystem(){

        FRService frs = new FRService(this.brokerAddresses, this.topics);
        frs.start();

        // Request TMZ Briefing
        //PullHandler pullHandler = new PullHandler(this.brokerConnections, this.topics);
        //pullHandler.run(); //THIS THREAD MUST FINISH BEFORE OTHERS ARE CREATED.
        /*try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }*/

        for(InetAddress ie : this.brokerConnections.keySet()){
            ArrayList<Thread> outgoings = this.outgoingRequests.get(ie);
            OutgoingScheduler outgoingDispatcher = new OutgoingScheduler(outgoings);
            outgoingDispatcher.start();
        }

    }

    class OutgoingScheduler extends Thread {

        private ArrayList<Thread> outgoings;

        public OutgoingScheduler(ArrayList<Thread> outgoings){
            this.outgoings = outgoings;
        }

        @Override
        public void run(){
            while(true){
                synchronized (this.outgoings){
                    if(!this.outgoings.isEmpty()){
                        Thread firstCome = this.outgoings.get(0);
                        if(firstCome.getState() == State.NEW){
                            firstCome.start();
                        } else if(firstCome.getState() == State.TERMINATED){
                            this.outgoings.remove(0);
                        }
                    }
                }
            }
        }
    }

    // PUBLISHER FAMILY THREADS
    /**
     * This is the parent class for handling Publisher-oriented actions.
     */
    class PublisherHandler extends Thread {

        Socket frsSocket;
        ObjectInputStream frsIn;
        ObjectOutputStream frsOut;

        String username;
        String target_topic;
        Value value;
        HashMap<String, Topic> topics;

        public PublisherHandler(
                HashMap<String, Topic> topics,
                String username,
                String target_topic,
                Value val
        ){
            this.username = username;
            this.target_topic = target_topic;
            this.value = val;
            this.topics = topics;
        }
    }

    /**
     * This class handles the terminal's 'PUSH' request to a broker.
     */
    class PushHandler extends PublisherHandler {

        BrokerConnection brokerConnection;

        public PushHandler(BrokerConnection brokerConnection,
                           HashMap<String, Topic> topics,
                           String username,
                           String target_topic,
                           Value val) {
            super(topics, username, target_topic, val);
            this.brokerConnection = brokerConnection;
        }

        /**
         * This method is used to push a multimedia file to a topic.
         * It sends the multimedia file's metadata and chunks separately.
         * @param mf2send the multimediafile object
         */
        private void sendFile(MultimediaFile mf2send){
            try {
                int bytes = 0;
                ArrayList<byte[]> chunks = mf2send.getChunks();
                MultimediaFile mf2send_empty;


                if(mf2send instanceof Story){
                    mf2send_empty = new Story(
                            mf2send.getDateSent(),
                            mf2send.getSentFrom(),
                            mf2send.getFilename(),
                            chunks.size(),
                            new ArrayList<byte[]>()
                    );
                } else {
                    mf2send_empty = new MultimediaFile(
                            mf2send.getDateSent(),
                            mf2send.getSentFrom(),
                            mf2send.getFilename(),
                            chunks.size(),
                            new ArrayList<byte[]>()
                    );
                }
                System.out.println("new chunks: "+mf2send_empty.getChunks().size());

                this.frsOut.writeObject(mf2send_empty);
                this.frsOut.flush();

                for (int i = 0; i < chunks.size(); i++) {
                    System.out.println("Sending chunk #" + i);
                    this.frsOut.write(chunks.get(i), 0, 512 * 1024);
                    this.frsOut.flush();

                }
            }catch (Exception e){
                e.printStackTrace();
            }
        }

        @Override
        public void run() {

            try {

                String brokerIp = this.brokerConnection.getBrokerAddress().toString().split("/")[1];
                //System.out.println("Attempting to send to Broker with IP: "+brokerIp);
                this.frsSocket = new Socket();
                this.frsSocket.connect(new InetSocketAddress(brokerIp, 5002), 3000);
                this.frsOut = new ObjectOutputStream(frsSocket.getOutputStream());
                this.frsIn = new ObjectInputStream(frsSocket.getInputStream());

                // Declare request...
                this.frsOut.writeUTF("PUSH");
                this.frsOut.flush();

                // Declare user
                this.frsOut.writeUTF(this.username);
                this.frsOut.flush();

                // Declare topic
                this.frsOut.writeUTF(this.target_topic);
                this.frsOut.flush();

                String confirmationToProceed = this.frsIn.readUTF();
                if(confirmationToProceed.equals("CONFIRM")){
                    if(this.value instanceof Message){
                        this.frsOut.writeUTF("MSG");
                        this.frsOut.flush();

                        this.frsOut.writeObject(this.value);
                        this.frsOut.flush();


                    } else if(this.value instanceof Story) {
                        this.frsOut.writeUTF("STORY");
                        this.frsOut.flush();

                        sendFile((Story) this.value);

                    } else if(this.value instanceof MultimediaFile) {
                        this.frsOut.writeUTF("MULTIF");
                        this.frsOut.flush();

                        sendFile((MultimediaFile) this.value);

                    }

                    String brokerReply = this.frsIn.readUTF();
                    //System.out.println(brokerReply);

                } else {
                    // Get correct broker...
                    //System.out.println("Broker not responsible for topic... communicating with broker #" + correctBrokerToCommTo);
                }



            } catch(SocketTimeoutException ste) {
                //ste.printStackTrace();
                //System.out.println("TIMEOUT Broker unreachable or dead...");
                //System.out.println("Assigning to next alive broker and trying again...");

            } catch(ConnectException ce){
                ce.printStackTrace();
                //System.out.println("Broker unreachable or dead...");
                //System.out.println("Assigning to next alive broker and trying again...");

            }catch (Exception e) {
                //e.printStackTrace();
            } finally {
                try {
                    if (this.frsIn != null)
                        this.frsIn.close();
                    if (this.frsOut != null)
                        this.frsOut.close();
                    if (this.frsSocket != null){
                        if (!this.frsSocket.isClosed()) {
                            this.frsSocket.close();
                        }
                    }
                }catch (IOException e){
                    e.printStackTrace();
                }

            }
        }
    }


    // SUBSCRIBER FAMILY THREADS:
    /**
     * This is the parent class for handling Subscriber-oriented actions.
     */
    class SubscriberHandler extends Thread {
        Socket frsSocket;
        ObjectInputStream frsIn;
        ObjectOutputStream frsOut;

        String username;

        public SubscriberHandler(
                String username
        ) {
            this.username = username;
        }

    }

    /**
     * This class handles the terminal's 'SUB' request to a broker.
     */
    class SubToTopicHandler extends SubscriberHandler {

        String topicname;
        HashMap<String, Topic> topics;
        BrokerConnection brokerConnection;

        public SubToTopicHandler(
                BrokerConnection brokerConnection,
                HashMap<String, Topic> topics,
                String username,
                String topicname
        ) {
            super(username);
            this.brokerConnection = brokerConnection;
            this.topics = topics;
            this.topicname = topicname;
        }

        @Override
        public void run() {

            try {

                String brokerIp = this.brokerConnection.getBrokerAddress().toString().split("/")[1];
                // TODO: debug
                System.out.println("Attempting to broadcast to Broker with IP: "+brokerIp);
                this.frsSocket = new Socket();
                this.frsSocket.connect(new InetSocketAddress(brokerIp, 5002), 3000);
                this.frsOut = new ObjectOutputStream(frsSocket.getOutputStream());
                this.frsIn = new ObjectInputStream(frsSocket.getInputStream());


                this.frsOut.writeUTF("SUB");
                this.frsOut.flush();

                this.frsOut.writeUTF(this.username);
                this.frsOut.flush();


                this.frsOut.writeUTF(this.topicname);
                this.frsOut.flush();

                String confirmationToProceed = this.frsIn.readUTF();
                if (confirmationToProceed.equals("CONFIRM")) {
                    String reply = this.frsIn.readUTF();
                    //TODO: debug

                    System.out.println(reply);
                } else if (confirmationToProceed.equals("DENY")) {
                    // Get correct broker...
                    int correctBrokerToCommTo = Integer.parseInt(this.frsIn.readUTF());
                    // TODO: debug
                    //System.out.println("Broker not responsible for topic... communicating with broker #" + correctBrokerToCommTo);
                    System.out.println("DENY received");
                }


            } catch(SocketTimeoutException ste) {
                ste.printStackTrace();
                System.out.println("TIMEOUT Broker unreachable or dead...");
                //System.out.println("Assigning to next alive broker and trying again...");

            } catch(ConnectException ce){
                ce.printStackTrace();
                //System.out.println("Broker unreachable or dead...");
                //System.out.println("Assigning to next alive broker and trying again...");

            }catch (Exception e) {
                //e.printStackTrace();
            } finally {
                try {
                    if (this.frsIn != null)
                        this.frsIn.close();
                    if (this.frsOut != null)
                        this.frsOut.close();
                    if (this.frsSocket != null){
                        if (!this.frsSocket.isClosed()) {
                            this.frsSocket.close();
                        }
                    }
                }catch (IOException e){
                    //e.printStackTrace();
                }
            }



        }
    }

    /**
     * This class handles the terminal's 'UNSUB' request to a broker.
     */
    class UnsubFromTopicHandler extends SubscriberHandler {

        String topicname;
        HashMap<String, Topic> topics;
        BrokerConnection brokerConnection;

        public UnsubFromTopicHandler(
                BrokerConnection brokerConnection,
                HashMap<String, Topic> topics,
                String username,
                String topicname
        ) {
            super(username);
            this.brokerConnection = brokerConnection;
            this.topics = topics;
            this.topicname = topicname;
        }

        @Override
        public void run() {

            try {

                String brokerIp = this.brokerConnection.getBrokerAddress().toString().split("/")[1];
                // TODO: debug
                //System.out.println("Attempting to send to Broker with IP: "+brokerIp);
                this.frsSocket = new Socket();
                this.frsSocket.connect(new InetSocketAddress(brokerIp, 5002), 3000);
                this.frsOut = new ObjectOutputStream(frsSocket.getOutputStream());
                this.frsIn = new ObjectInputStream(frsSocket.getInputStream());


                this.frsOut.writeUTF("UNSUB");
                this.frsOut.flush();

                this.frsOut.writeUTF(this.username);
                this.frsOut.flush();


                this.frsOut.writeUTF(this.topicname);
                this.frsOut.flush();

                String confirmationToProceed = this.frsIn.readUTF();
                if (confirmationToProceed.equals("CONFIRM")) {
                    String reply = this.frsIn.readUTF();
                    //TODO: debug
                    //System.out.println(reply);
                } else if (confirmationToProceed.equals("DENY")) {
                    // Get correct broker...
                    int correctBrokerToCommTo = Integer.parseInt(this.frsIn.readUTF());
                    // TODO: debug
                    //System.out.println("Broker not responsible for topic... communicating with broker #" + correctBrokerToCommTo);
                }


            } catch(SocketTimeoutException ste) {
                //ste.printStackTrace();
                //System.out.println("TIMEOUT Broker unreachable or dead...");
                //System.out.println("Assigning to next alive broker and trying again...");

            } catch(ConnectException ce){
                //ce.printStackTrace();
                //System.out.println("Broker unreachable or dead...");
                //System.out.println("Assigning to next alive broker and trying again...");

            }catch (Exception e) {
                //e.printStackTrace();
            } finally {
                try {
                    if (this.frsIn != null)
                        this.frsIn.close();
                    if (this.frsOut != null)
                        this.frsOut.close();
                    if (this.frsSocket != null){
                        if (!this.frsSocket.isClosed()) {
                            this.frsSocket.close();
                        }
                    }
                }catch (IOException e){
                    //e.printStackTrace();
                }
            }



        }
    }

    /**
     * This class handles the terminal's 'PULL' request to a broker.
     */
    // TODO: to implement...
    class PullHandler extends SubscriberHandler {

        HashMap<String, Topic> topics;
        HashMap<InetAddress, BrokerConnection> brokerConnections;

        public PullHandler(
                HashMap<InetAddress, BrokerConnection> brokerConnections,
                HashMap<String, Topic> topics
        ) {
            super(null);
            this.topics = topics;
            this.brokerConnections = brokerConnections;
        }

        /**
         * This method is a daemon that periodically pulls the latest values
         * from a broker.
         */
        @Override
        public void run() {

            boolean requestComplete = false;
            int brokerID = (int) (Math.random() * (this.brokerConnections.size()));
            InetAddress brokerAddress = null;

            while(!requestComplete) {
                //if(brokerConnection.isActive()){

                synchronized (this.brokerConnections){
                    for(BrokerConnection bc : this.brokerConnections.values()){
                        if(bc.getBrokerID() == brokerID)
                            brokerAddress = bc.getBrokerAddress();
                    }
                }

                try {

                    if(brokerAddress == null){
                        throw new ConnectException();
                    }

                    String brokerIp = brokerAddress.toString().split("/")[1];
                    this.frsSocket = new Socket();
                    this.frsSocket.connect(new InetSocketAddress(brokerIp, 5002), 3000);
                    this.frsOut = new ObjectOutputStream(frsSocket.getOutputStream());
                    this.frsIn = new ObjectInputStream(frsSocket.getInputStream());
                    int reply = Integer.MIN_VALUE; // reply option
                    String topicname = null;
                    // Send request:
                    this.frsOut.writeUTF("PULL");
                    this.frsOut.flush();

                    while (reply != -1) {
                        reply = this.frsIn.readInt();
                        if (reply == 0) {
                            // Create new topic.
                            topicname = this.frsIn.readUTF();
                            synchronized (this.topics) {
                                if (!this.topics.keySet().contains(topicname)) {
                                    this.topics.put(topicname, new Topic(topicname));
                                }
                            }
                        } else if (reply == 1) {
                            String sub_name = this.frsIn.readUTF();
                            Topic topic = this.topics.get(topicname);
                            synchronized (topic) {
                                if (!topic.isSubbed(sub_name)) {
                                    topic.addUser(sub_name);
                                }
                            }
                        }
                    }

                    requestComplete = true;

                } catch (SocketTimeoutException ste) {
                    // TODO: Fault tolerance? Might not make it in the final version.
                    System.out.println("Broker unavailable, trying another...");
                    brokerID = (brokerID + 1) % this.brokerConnections.size();
                /*synchronized (brokerConnection){
                    brokerConnection.setDead();
                }*/
                    //System.out.println("PULL FAIL: Broker #" + brokerConnection.getBrokerID() + " (" + brokerConnection.getBrokerAddress().toString() + ") might be dead...");
                } catch (Exception e) {
                    //e.printStackTrace();
                } finally {
                    try {
                        if (this.frsIn != null)
                            this.frsIn.close();
                        if (this.frsOut != null)
                            this.frsOut.close();
                        if (this.frsSocket != null)
                            if (!this.frsSocket.isClosed()) {
                                this.frsSocket.close();
                            }
                    } catch (IOException e) {
                        //e.printStackTrace();
                    }
                }
            }
            //}

        }
    }

    // Fault Recovery Service Handler


}
