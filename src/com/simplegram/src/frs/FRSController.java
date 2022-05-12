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

/**
 * Failure Recovery Service (FRS) Controller class.
 */
public class FRSController {

    private HashMap<String, Topic> topics;
    private ArrayList<InetAddress> brokerAddresses;
    private HashMap<InetAddress, BrokerConnection> brokerConnections;
    
    // Public Interface
    public FRSController(
            HashMap<String, Topic> topics,
            ArrayList<InetAddress> brokerAddresses,
            HashMap<InetAddress, BrokerConnection> brokerConnections
    ) {
        this.topics = topics;
        this.brokerAddresses = brokerAddresses;
        this.brokerConnections = brokerConnections;
    }

    public void broadcastSub(String username, String topicname){
        for (BrokerConnection br : this.brokerConnections.values()){
            SubToTopicHandler subToTopicHandler = new SubToTopicHandler(
                    br,
                    this.topics,
                    username,
                    topicname
            );
            subToTopicHandler.start();
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
            unsubFromTopicHandler.start();
        }
    }

    public void broadcastPush(String username, String topicname, Value value){
        for (BrokerConnection br : this.brokerConnections.values()) {
            PushHandler ph2 = new PushHandler(
                    br,
                    this.topics,
                    username,
                    topicname,
                    value
            );
            ph2.start();
        }
    }
    
    public void startFaultRecoveryService(){
        FRService frs = new FRService(this.brokerAddresses, this.topics);
        frs.start();
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
                //System.out.println("Attempting to send to Broker with IP: "+brokerIp);
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
        BrokerConnection brokerConnection;

        public PullHandler(
                BrokerConnection brokerConnection,
                HashMap<String, Topic> topics,
                String username
        ) {
            super(username);
            this.topics = topics;
            this.brokerConnection = brokerConnection;
        }

        /**
         * This method is used in order to receive a multimedia file
         * from a broker.
         * @param val_type the type of multimedia file to receive [MULTIF/STORY]
         * @return MultimediaFile object
         * @throws Exception
         */
        private MultimediaFile receiveFile(String val_type) throws Exception{//data transfer with chunking

            MultimediaFile mf_rcv;
            if(val_type.equals("MULTIF")){
                mf_rcv = (MultimediaFile) this.frsIn.readObject();
            } else {
                mf_rcv = (Story) this.frsIn.readObject();
            }

            int size = mf_rcv.getFileSize();// amount of expected chunks
            String filename = mf_rcv.getFilename();// read file name

            FileOutputStream fileOutputStream = new FileOutputStream(filename);
            byte[] buffer = new byte[512*1024]; //512 * 2^10 (512KByte chunk size)

            while (size>0) {
                this.frsIn.readFully(buffer, 0, 512*1024);
                fileOutputStream.write(buffer,0,512*1024);
                size --;
            }
            fileOutputStream.close();
            return mf_rcv;
        }

        /**
         * This method is a daemon that periodically pulls the latest values
         * from a broker.
         */
        @Override
        public void run() {



            while(true) {
                //if(brokerConnection.isActive()){
                try {
                    // TODO: if timeout then make broker dead. Might not make it in the final version.
                    String brokerIp = this.brokerConnection.getBrokerAddress().toString().split("/")[1];
                    this.frsSocket = new Socket();
                    this.frsSocket.connect(new InetSocketAddress(brokerIp, 5002), 3000);
                    this.frsOut = new ObjectOutputStream(frsSocket.getOutputStream());
                    this.frsIn = new ObjectInputStream(frsSocket.getInputStream());
                    // TODO: debug
                    //System.out.println("Pulling recent values from Broker #" + brokerConnection.getBrokerID() + " (" + brokerIp + ")");
                    HashMap<String, ArrayList<Value>> unreads = new HashMap<String, ArrayList<Value>>();

                    this.frsOut.writeUTF("PULL");
                    this.frsOut.flush();

                    this.frsOut.writeUTF(this.username);
                    this.frsOut.flush();

                    do {
                        String topic_name = this.frsIn.readUTF();
                        if (topic_name.equals("---"))
                            break;
                        String val_type = this.frsIn.readUTF();
                        Value v = null;
                        if (val_type.equals("MSG")) {
                            v = (Message) this.frsIn.readObject();
                        } else if (val_type.equals("MULTIF") || val_type.equals("STORY")) {
                            v = this.receiveFile(val_type);
                        }

                        // add to unread queue
                        if (unreads.get(topic_name) == null)
                            unreads.put(topic_name, new ArrayList<Value>());
                        unreads.get(topic_name).add(v);


                    } while (!this.frsIn.readUTF().equals("---"));

                    synchronized (this.topics) {
                        for (String topicName : unreads.keySet()) {
                            Topic localTopic = this.topics.get(topicName);
                            ArrayList<Value> unreadValues = unreads.get(topicName);
                            for (Value val : unreadValues) {
                                if (val instanceof Story) {
                                    localTopic.addStory((Story) val);
                                    //TODO: debug
                                    //System.out.println(TerminalColors.ANSI_GREEN + val.getSentFrom() + "@" + topicName + ": (STORY) " + val + TerminalColors.ANSI_RESET);
                                } else {
                                    localTopic.addMessage(val);
                                    // TODO: debug
                                    //System.out.println(TerminalColors.ANSI_GREEN + val.getSentFrom() + "@" + topicName + ": " + val + TerminalColors.ANSI_RESET);
                                }
                            }
                        }
                    }

                } catch(SocketTimeoutException ste) {
                    // TODO: Fault tolerance? Might not make it in the final version.
                    /*synchronized (brokerConnection){
                        brokerConnection.setDead();
                    }*/
                    System.out.println("PULL FAIL: Broker #" + brokerConnection.getBrokerID()+" (" + brokerConnection.getBrokerAddress().toString()+") might be dead...");
                }catch (Exception e) {
                    //e.printStackTrace();
                } finally {
                    try{
                        if(this.frsIn!=null)
                            this.frsIn.close();
                        if(this.frsOut!=null)
                            this.frsOut.close();
                        if(this.frsSocket != null)
                            if (!this.frsSocket.isClosed()){
                                this.frsSocket.close();
                            }
                    }catch (IOException e){
                        //e.printStackTrace();
                    }
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            //}

        }
    }

    // Fault Recovery Service Handler


}
