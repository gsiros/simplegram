package com.simplegram.src;

import com.simplegram.src.ibc.BrokerConnection;
import com.simplegram.src.logging.TerminalColors;

import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.HashMap;

public class UserNode {

    // Local data structures
    private HashMap<String, Topic> topics;

    // The list of brokers that the node requires
    // in order to publish and consume data.
    private ArrayList<InetAddress> brokerAddresses;
    private HashMap<InetAddress, BrokerConnection> brokerConnections;

    private Socket cbtSocket;

    public static void main(String[] args) {
        UserNode un = new UserNode();
        un.userStart();
    }


    public UserNode() {

        this.topics = new HashMap<String, Topic>();
        this.brokerAddresses = new ArrayList<InetAddress>();
        this.brokerConnections = new HashMap<InetAddress, BrokerConnection>();
    }

    public void userStart() {

        try {
            this.brokerAddresses.add(InetAddress.getByName("192.168.1.8"));
            this.brokerConnections.put(InetAddress.getByName("192.168.1.8"), new BrokerConnection(0, InetAddress.getByName("192.168.1.8")));
            this.brokerAddresses.add(InetAddress.getByName("192.168.1.9"));
            this.brokerConnections.put(InetAddress.getByName("192.168.1.9"), new BrokerConnection(1, InetAddress.getByName("192.168.1.9")));

        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        //this.topics.put("test", new Topic("test"));
        //this.topics.get("test").addUser("george");
        //this.topics.get("test").setAssignedBrokerID(1);
        //this.topics.get("test").addUser("george");

        //this.topics.put("7328465234", new Topic("7328465234"));
        //this.topics.get("7328465234").addUser("george");
        //this.topics.get("7328465234").setAssignedBrokerID(0);

        StoryChecker sc = new StoryChecker(this.topics);
        sc.start();

        for(BrokerConnection bc : this.brokerConnections.values()){
            PullHandler pullh = new PullHandler(bc,this.topics, "george");
            pullh.start();
        }

        SubToTopicHandler subToTopicHandler = new SubToTopicHandler(this.brokerConnections, this.topics, "george", "7328465234");
        subToTopicHandler.start();

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        PushHandler ph = new PushHandler(
                this.brokerConnections,
                this.topics,
                "george",
                "7328465234",
                new Message("george","Hello chat")
        );
        ph.start();

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        ArrayList<byte[]> chunks = UserNode.chunkify("/Users/George/Downloads/sena.jpeg");
        MultimediaFile mystory = new Story("george",
                "sena.jpeg",
                chunks.size(),
                chunks
        );
        System.out.println("I sent this at "+mystory.getDateSent());

        PushHandler ph2 = new PushHandler(
                this.brokerConnections,
                this.topics,
                "george",
                "7328465234",
                mystory
        );
        ph2.start();

        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        /*

        MultimediaFile mystory2 = new Story("george",
                "sena.jpeg",
                chunks.size(),
                chunks
        );

        PushHandler ph3 = new PushHandler(
                this.topics,
                "george",
                "test",
                mystory2
        );
        ph3.start();*/

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        UnsubFromTopicHandler unsubFromTopicHandler = new UnsubFromTopicHandler(
                this.brokerConnections,
                this.topics,
                "george",
                "7328465234");
        unsubFromTopicHandler.start();


        //ArrayList<byte[]> rex_chunks = this.chunkify("/Users/George/Documents/Photos/rex.jpg");

        /*PubHandler ph2 = new PubHandler(
                this.topics,
                "george",
                "test",
                new MultimediaFile(
                        "george",
                        "rex.jpg",
                        rex_chunks.size(),
                        rex_chunks
                )
        );
        ph2.start();*/

    }

    // Break file to chunks...
    static ArrayList<byte[]> chunkify(String path) {

        ArrayList<byte[]> chunks = new ArrayList<byte[]>();

        try {
            int bytes = 0;
            File file = new File(path);
            FileInputStream fileInputStream = new FileInputStream(file);

            // break file into chunks
            byte[] buffer = new byte[512 * 1024];
            while ((bytes = fileInputStream.read(buffer)) != -1) {
                chunks.add(buffer.clone());
            }

            fileInputStream.close();

        } catch (Exception e){
            e.printStackTrace();
        }

        return chunks;
    }

    public void shutdown() {
        try {
            if (!this.cbtSocket.isClosed()) {
                this.cbtSocket.close();
            }

        } catch (IOException e) {
            //
        }
    }

    // PUBLISHER FAMILY THREADS

    class PublisherHandler extends Thread {

        Socket cbtSocket;
        ObjectInputStream cbtIn;
        ObjectOutputStream cbtOut;

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

    class PushHandler extends PublisherHandler {

        HashMap<InetAddress, BrokerConnection> brokerConnections;

        public PushHandler(HashMap<InetAddress, BrokerConnection> brokerConnections,
                           HashMap<String, Topic> topics,
                           String username,
                           String target_topic,
                           Value val) {
            super(topics, username, target_topic, val);
            this.brokerConnections = brokerConnections;
        }

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

                this.cbtOut.writeObject(mf2send_empty);
                this.cbtOut.flush();

                for (int i = 0; i < chunks.size(); i++) {
                    System.out.println("Sending chunk #" + i);
                    this.cbtOut.write(chunks.get(i), 0, 512 * 1024);
                    this.cbtOut.flush();

                }
            }catch (Exception e){
                e.printStackTrace();
            }
        }

        @Override
        public void run() {

            boolean requestComplete = false;
            int brokerID;
            InetAddress brokerAddress = null;


            while (!requestComplete) {

                synchronized (this.topics) {
                    brokerID = this.topics.get(this.target_topic).getAssignedBrokerID();
                }

                synchronized (this.brokerConnections) {
                    for (BrokerConnection bc : this.brokerConnections.values()) {
                        if (bc.getBrokerID() == brokerID)
                            brokerAddress = bc.getBrokerAddress();
                    }
                }

                try {
                    if(brokerAddress == null){
                        throw new ConnectException();
                    }

                    String brokerIp = brokerAddress.toString().split("/")[1];
                    System.out.println("Attempting to send to Broker with IP: "+brokerIp);
                    this.cbtSocket = new Socket();
                    this.cbtSocket.connect(new InetSocketAddress(brokerIp, 5001), 3000);
                    this.cbtOut = new ObjectOutputStream(cbtSocket.getOutputStream());
                    this.cbtIn = new ObjectInputStream(cbtSocket.getInputStream());

                    // Declare request...
                    this.cbtOut.writeUTF("PUSH");
                    this.cbtOut.flush();

                    // Declare user
                    this.cbtOut.writeUTF(this.username);
                    this.cbtOut.flush();

                    // Declare topic
                    this.cbtOut.writeUTF(this.target_topic);
                    this.cbtOut.flush();

                    String confirmationToProceed = this.cbtIn.readUTF();
                    if(confirmationToProceed.equals("CONFIRM")){
                        if(this.value instanceof Message){
                            this.cbtOut.writeUTF("MSG");
                            this.cbtOut.flush();

                            this.cbtOut.writeObject(this.value);
                            this.cbtOut.flush();


                        } else if(this.value instanceof Story) {
                            this.cbtOut.writeUTF("STORY");
                            this.cbtOut.flush();

                            sendFile((Story) this.value);

                        } else if(this.value instanceof MultimediaFile) {
                            this.cbtOut.writeUTF("MULTIF");
                            this.cbtOut.flush();

                            sendFile((MultimediaFile) this.value);

                        }

                        String brokerReply = this.cbtIn.readUTF();
                        System.out.println(brokerReply);
                        requestComplete = true;

                    } else {
                        // Get correct broker...
                        int correctBrokerToCommTo = Integer.parseInt(this.cbtIn.readUTF());
                        synchronized (this.topics) {
                            this.topics.get(target_topic).setAssignedBrokerID(correctBrokerToCommTo);
                        }
                        System.out.println("Broker not responsible for topic... communicating with broker #" + correctBrokerToCommTo);
                    }



                } catch(SocketTimeoutException ste) {
                    ste.printStackTrace();
                    System.out.println("TIMEOUT Broker unreachable or dead...");
                    synchronized (this.topics) {
                        Topic t = this.topics.get(target_topic);
                        System.out.println("previous BrokerID: "+t.getAssignedBrokerID());
                        t.setAssignedBrokerID((t.getAssignedBrokerID() + 1) % (this.brokerConnections.size()));
                        System.out.println("next BrokerID: "+t.getAssignedBrokerID());
                    }
                    System.out.println("Assigning to next alive broker and trying again...");

                } catch(ConnectException ce){
                    ce.printStackTrace();
                    System.out.println("Broker unreachable or dead...");
                    synchronized (this.topics) {
                        Topic t = this.topics.get(target_topic);
                        System.out.println("previous BrokerID: "+t.getAssignedBrokerID());
                        t.setAssignedBrokerID((t.getAssignedBrokerID() + 1) % (this.brokerConnections.size()));
                        System.out.println("next BrokerID: "+t.getAssignedBrokerID());
                    }
                    System.out.println("Assigning to next alive broker and trying again...");

                }catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    try {
                        if (this.cbtIn != null)
                            this.cbtIn.close();
                        if (this.cbtOut != null)
                            this.cbtOut.close();
                        if (this.cbtSocket != null){
                            if (!this.cbtSocket.isClosed()) {
                                this.cbtSocket.close();
                            }
                        }
                    }catch (IOException e){
                        e.printStackTrace();
                    }
                }

            }


        }
    }


    // SUBSCRIBER FAMILY THREADS:

    class SubscriberHandler extends Thread {
        Socket cbtSocket;
        ObjectInputStream cbtIn;
        ObjectOutputStream cbtOut;

        String username;

        public SubscriberHandler(
                String username
        ) {
            this.username = username;
        }

    }

    class SubToTopicHandler extends SubscriberHandler {

        String topicname;
        HashMap<String, Topic> topics;
        HashMap<InetAddress, BrokerConnection> brokerConnections;

        public SubToTopicHandler(
                HashMap<InetAddress, BrokerConnection> brokerConnections,
                HashMap<String, Topic> topics,
                String username,
                String topicname
                ) {
            super(username);
            this.brokerConnections = brokerConnections;
            this.topics = topics;
            this.topicname = topicname;
        }

        @Override
        public void run() {

            boolean requestComplete = false;
            int brokerID = (int) (Math.random() * (this.brokerConnections.size()));
            InetAddress brokerAddress = null;


            while (!requestComplete){

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
                    System.out.println("Attempting to send to Broker with IP: "+brokerIp);
                    this.cbtSocket = new Socket();
                    this.cbtSocket.connect(new InetSocketAddress(brokerIp, 5001), 3000);
                    this.cbtOut = new ObjectOutputStream(cbtSocket.getOutputStream());
                    this.cbtIn = new ObjectInputStream(cbtSocket.getInputStream());


                    this.cbtOut.writeUTF("SUB");
                    this.cbtOut.flush();

                    this.cbtOut.writeUTF(this.username);
                    this.cbtOut.flush();


                    this.cbtOut.writeUTF(this.topicname);
                    this.cbtOut.flush();

                    String confirmationToProceed = this.cbtIn.readUTF();
                    if (confirmationToProceed.equals("CONFIRM")) {
                        // the broker is the correct...
                        synchronized (this.topics) {
                            this.topics.put(this.topicname, new Topic(this.topicname));
                            this.topics.get(this.topicname).setAssignedBrokerID(brokerID);
                            this.topics.get(this.topicname).addUser(this.username);
                        }
                        String reply = this.cbtIn.readUTF();
                        System.out.println(reply);
                        requestComplete = true;
                    } else if (confirmationToProceed.equals("DENY")) {
                        // Get correct broker...
                        int correctBrokerToCommTo = Integer.parseInt(this.cbtIn.readUTF());
                        brokerID = correctBrokerToCommTo;
                        System.out.println("Broker not responsible for topic... communicating with broker #" + correctBrokerToCommTo);
                    }


                } catch(SocketTimeoutException ste) {
                    ste.printStackTrace();
                    System.out.println("TIMEOUT Broker unreachable or dead...");
                    synchronized (this.topics) {
                        Topic t = this.topics.get(topicname);
                        System.out.println("previous BrokerID: "+t.getAssignedBrokerID());
                        t.setAssignedBrokerID((t.getAssignedBrokerID() + 1) % (this.brokerConnections.size()));
                        System.out.println("next BrokerID: "+t.getAssignedBrokerID());
                    }
                    System.out.println("Assigning to next alive broker and trying again...");

                } catch(ConnectException ce){
                    ce.printStackTrace();
                    System.out.println("Broker unreachable or dead...");
                    synchronized (this.topics) {
                        Topic t = this.topics.get(topicname);
                        System.out.println("previous BrokerID: "+t.getAssignedBrokerID());
                        t.setAssignedBrokerID((t.getAssignedBrokerID() + 1) % (this.brokerConnections.size()));
                        System.out.println("next BrokerID: "+t.getAssignedBrokerID());
                    }
                    System.out.println("Assigning to next alive broker and trying again...");

                }catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    try {
                        if (this.cbtIn != null)
                            this.cbtIn.close();
                        if (this.cbtOut != null)
                            this.cbtOut.close();
                        if (this.cbtSocket != null){
                            if (!this.cbtSocket.isClosed()) {
                                this.cbtSocket.close();
                            }
                        }
                    }catch (IOException e){
                        e.printStackTrace();
                    }
                }
            }


        }
    }

    class UnsubFromTopicHandler extends SubscriberHandler {

        String topicname;
        HashMap<String, Topic> topics;
        HashMap<InetAddress, BrokerConnection> brokerConnections;

        public UnsubFromTopicHandler(
                HashMap<InetAddress, BrokerConnection> brokerConnections,
                HashMap<String, Topic> topics,
                String username,
                String topicname
        ) {
            super(username);
            this.brokerConnections = brokerConnections;
            this.topics = topics;
            this.topicname = topicname;
        }

        @Override
        public void run() {

            boolean requestComplete = false;
            int brokerID;
            InetAddress brokerAddress = null;

            while (!requestComplete) {

                synchronized (this.topics) {
                    brokerID = this.topics.get(this.topicname).getAssignedBrokerID();
                }

                synchronized (this.brokerConnections) {
                    for (BrokerConnection bc : this.brokerConnections.values()) {
                        if (bc.getBrokerID() == brokerID)
                            brokerAddress = bc.getBrokerAddress();
                    }
                }



                try {
                    if(brokerAddress == null){
                        throw new ConnectException();
                    }

                    String brokerIp = brokerAddress.toString().split("/")[1];
                    System.out.println("Attempting to send to Broker with IP: "+brokerIp);
                    this.cbtSocket = new Socket();
                    this.cbtSocket.connect(new InetSocketAddress(brokerIp, 5001), 3000);
                    this.cbtOut = new ObjectOutputStream(cbtSocket.getOutputStream());
                    this.cbtIn = new ObjectInputStream(cbtSocket.getInputStream());

                    this.cbtOut.writeUTF("UNSUB");
                    this.cbtOut.flush();

                    this.cbtOut.writeUTF(this.username);
                    this.cbtOut.flush();


                    this.cbtOut.writeUTF(this.topicname);
                    this.cbtOut.flush();


                    // server reply
                    String confirmationToProceed = this.cbtIn.readUTF();
                    if(confirmationToProceed.equals("CONFIRM")){
                        synchronized (this.topics){
                            this.topics.remove(this.topicname);
                        }

                        String reply = this.cbtIn.readUTF();
                        System.out.println(reply);
                        requestComplete = true;
                    } else if (confirmationToProceed.equals("DENY")){
                        int correctBrokerToCommTo = Integer.parseInt(this.cbtIn.readUTF());
                        synchronized (this.topics) {
                            this.topics.get(topicname).setAssignedBrokerID(correctBrokerToCommTo);
                        }
                        System.out.println("Broker not responsible for topic... communicating with broker #" + correctBrokerToCommTo);
                    }


                } catch(SocketTimeoutException ste) {
                    ste.printStackTrace();
                    System.out.println("TIMEOUT Broker unreachable or dead...");
                    synchronized (this.topics) {
                        Topic t = this.topics.get(topicname);
                        System.out.println("previous BrokerID: "+t.getAssignedBrokerID());
                        t.setAssignedBrokerID((t.getAssignedBrokerID() + 1) % (this.brokerConnections.size()));
                        System.out.println("next BrokerID: "+t.getAssignedBrokerID());
                    }
                    System.out.println("Assigning to next alive broker and trying again...");

                } catch(ConnectException ce){
                    ce.printStackTrace();
                    System.out.println("Broker unreachable or dead...");
                    synchronized (this.topics) {
                        Topic t = this.topics.get(topicname);
                        System.out.println("previous BrokerID: "+t.getAssignedBrokerID());
                        t.setAssignedBrokerID((t.getAssignedBrokerID() + 1) % (this.brokerConnections.size()));
                        System.out.println("next BrokerID: "+t.getAssignedBrokerID());
                    }
                    System.out.println("Assigning to next alive broker and trying again...");

                }catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    try {
                        if (this.cbtIn != null)
                            this.cbtIn.close();
                        if (this.cbtOut != null)
                            this.cbtOut.close();
                        if (this.cbtSocket != null){
                            if (!this.cbtSocket.isClosed()) {
                                this.cbtSocket.close();
                            }
                        }
                    }catch (IOException e){
                        e.printStackTrace();
                    }
                }

                // END WHILE
            }

        }
    }

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

        private MultimediaFile receiveFile(String val_type) throws Exception{//data transfer with chunking

            MultimediaFile mf_rcv;
            if(val_type.equals("MULTIF")){
                mf_rcv = (MultimediaFile) this.cbtIn.readObject();
            } else {
                mf_rcv = (Story) this.cbtIn.readObject();
            }

            int size = mf_rcv.getFileSize();// amount of expected chunks
            String filename = mf_rcv.getFilename();// read file name

            FileOutputStream fileOutputStream = new FileOutputStream(filename);
            byte[] buffer = new byte[512*1024]; //512 * 2^10 (512KByte chunk size)

            while (size>0) {
                this.cbtIn.readFully(buffer, 0, 512*1024);
                fileOutputStream.write(buffer,0,512*1024);
                size --;
            }
            fileOutputStream.close();
            return mf_rcv;
        }

        @Override
        public void run() {



            while(true) {
                //if(brokerConnection.isActive()){
                try {
                    // TODO: if timeout then make broker dead.
                    String brokerIp = this.brokerConnection.getBrokerAddress().toString().split("/")[1];
                    this.cbtSocket = new Socket();
                    this.cbtSocket.connect(new InetSocketAddress(brokerIp, 5001), 3000);
                    this.cbtOut = new ObjectOutputStream(cbtSocket.getOutputStream());
                    this.cbtIn = new ObjectInputStream(cbtSocket.getInputStream());
                    System.out.println("Pulling recent values from Broker #" + brokerConnection.getBrokerID() + " (" + brokerIp + ")");
                    HashMap<String, ArrayList<Value>> unreads = new HashMap<String, ArrayList<Value>>();

                    this.cbtOut.writeUTF("PULL");
                    this.cbtOut.flush();

                    this.cbtOut.writeUTF(this.username);
                    this.cbtOut.flush();

                    do {
                        String topic_name = this.cbtIn.readUTF();
                        if (topic_name.equals("---"))
                            break;
                        String val_type = this.cbtIn.readUTF();
                        Value v = null;
                        if (val_type.equals("MSG")) {
                            v = (Message) this.cbtIn.readObject();
                        } else if (val_type.equals("MULTIF") || val_type.equals("STORY")) {
                            v = this.receiveFile(val_type);
                        }

                        // add to unread queue
                        if (unreads.get(topic_name) == null)
                            unreads.put(topic_name, new ArrayList<Value>());
                        unreads.get(topic_name).add(v);


                    } while (!this.cbtIn.readUTF().equals("---"));

                    synchronized (this.topics) {
                        for (String topicName : unreads.keySet()) {
                            Topic localTopic = this.topics.get(topicName);
                            ArrayList<Value> unreadValues = unreads.get(topicName);
                            for (Value val : unreadValues) {
                                if (val instanceof Story) {
                                    localTopic.addStory((Story) val);
                                    System.out.println(TerminalColors.ANSI_GREEN + val.getSentFrom() + "@" + topicName + ": (STORY) " + val + TerminalColors.ANSI_RESET);
                                } else {
                                    localTopic.addMessage(val);
                                    System.out.println(TerminalColors.ANSI_GREEN + val.getSentFrom() + "@" + topicName + ": " + val + TerminalColors.ANSI_RESET);
                                }
                            }
                        }
                    }

                } catch(SocketTimeoutException ste) {
                    /*synchronized (brokerConnection){
                        brokerConnection.setDead();
                    }*/
                    System.out.println("PULL FAIL: Broker #" + brokerConnection.getBrokerID()+" (" + brokerConnection.getBrokerAddress().toString()+") might be dead...");
                }catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    try{
                        if(this.cbtIn!=null)
                            this.cbtIn.close();
                        if(this.cbtOut!=null)
                            this.cbtOut.close();
                        if(this.cbtSocket != null)
                            if (!this.cbtSocket.isClosed()){
                                this.cbtSocket.close();
                            }
                    }catch (IOException e){
                        e.printStackTrace();
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


}
