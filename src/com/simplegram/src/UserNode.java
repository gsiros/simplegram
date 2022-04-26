package com.simplegram.src;

import com.simplegram.src.ibc.BrokerConnection;
import com.simplegram.src.logging.TerminalColors;

import java.io.*;
import java.net.InetAddress;
import java.net.Socket;
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
    }

    public void userStart() {
        this.topics.put("test", new Topic("test"));
        this.topics.get("test").addUser("george");

        PullHandler pullh = new PullHandler(this.topics, "george");
        pullh.start();

        SubToTopicHandler subToTopicHandler = new SubToTopicHandler(this.topics, "george", "test");
        subToTopicHandler.start();

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        PushHandler ph = new PushHandler(
                this.topics,
                "george",
                "test",
                new Message("george","Hello chat")
        );
        ph.start();

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }



        UnsubFromTopicHandler unsubFromTopicHandler = new UnsubFromTopicHandler(this.topics, "george", "test");
        unsubFromTopicHandler.start();

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        PushHandler ph2 = new PushHandler(
                this.topics,
                "george",
                "test",
                new Message("george","Hello chat again")
        );
        ph2.start();

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
        }


    }

    class PushHandler extends PublisherHandler {


        public PushHandler(HashMap<String, Topic> topics, String username, String target_topic, Value val) {
            super(topics, username, target_topic, val);
        }

        private void sendFile(MultimediaFile mf2send){
            try {
                int bytes = 0;

                ArrayList<byte[]> chunks = mf2send.getChunks();

                // Create MultiMedia Object
                MultimediaFile mf2send_empty = new MultimediaFile(
                        mf2send.getSentFrom(),
                        mf2send.getFilename(),
                        chunks.size(),
                        new ArrayList<byte[]>()
                );

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
            // TODO: PUSH!
            try {
                this.cbtSocket = new Socket("localhost", 5001);
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

                // TODO: CHECK BROKER REPLY

                String reply = this.cbtIn.readUTF();
                if(reply.equals("OK")){
                    if(this.value instanceof Message){
                        this.cbtOut.writeUTF("MSG");
                        this.cbtOut.flush();

                        this.cbtOut.writeObject(this.value);
                        this.cbtOut.flush();


                    } else if(this.value instanceof MultimediaFile) {
                        this.cbtOut.writeUTF("MULTIF");
                        this.cbtOut.flush();

                        sendFile((MultimediaFile) this.value);

                    }
                }

                String brokerReply = this.cbtIn.readUTF();
                System.out.println(brokerReply);

            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try{
                    this.cbtIn.close();
                    this.cbtOut.close();
                    if (!this.cbtSocket.isClosed()){
                        this.cbtSocket.close();
                    }
                }catch (Exception e){
                    e.printStackTrace();
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

        public SubToTopicHandler(
                HashMap<String, Topic> topics,
                String username,
                String topicname
                ) {
            super(username);
            this.topics = topics;
            this.topicname = topicname;
        }

        @Override
        public void run() {

            try {
                this.cbtSocket = new Socket("localhost", 5001);
                this.cbtOut = new ObjectOutputStream(cbtSocket.getOutputStream());
                this.cbtIn = new ObjectInputStream(cbtSocket.getInputStream());

                this.cbtOut.writeUTF("SUB");
                this.cbtOut.flush();

                this.cbtOut.writeUTF(this.username);
                this.cbtOut.flush();


                this.cbtOut.writeUTF(this.topicname);
                this.cbtOut.flush();


                // server reply
                String reply = this.cbtIn.readUTF();
                if(reply.equals("OK")){
                    synchronized (this.topics){
                        this.topics.put(this.topicname, new Topic(this.topicname));
                    }
                }


            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try{
                    this.cbtIn.close();
                    this.cbtOut.close();
                    if (!this.cbtSocket.isClosed()){
                        this.cbtSocket.close();
                    }
                }catch (IOException e){
                    e.printStackTrace();
                }
            }

        }
    }

    class UnsubFromTopicHandler extends SubscriberHandler {

        String topicname;
        HashMap<String, Topic> topics;

        public UnsubFromTopicHandler(
                HashMap<String, Topic> topics,
                String username,
                String topicname
        ) {
            super(username);
            this.topics = topics;
            this.topicname = topicname;
        }

        @Override
        public void run() {

            try {
                this.cbtSocket = new Socket("localhost", 5001);
                this.cbtOut = new ObjectOutputStream(cbtSocket.getOutputStream());
                this.cbtIn = new ObjectInputStream(cbtSocket.getInputStream());

                this.cbtOut.writeUTF("UNSUB");
                this.cbtOut.flush();

                this.cbtOut.writeUTF(this.username);
                this.cbtOut.flush();


                this.cbtOut.writeUTF(this.topicname);
                this.cbtOut.flush();


                // server reply
                String reply = this.cbtIn.readUTF();
                if(reply.equals("OK")){
                    synchronized (this.topics){
                        this.topics.remove(this.topicname);
                    }
                } else{
                    System.out.println("scat");
                }


            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try{
                    this.cbtIn.close();
                    this.cbtOut.close();
                    if (!this.cbtSocket.isClosed()){
                        this.cbtSocket.close();
                    }
                }catch (IOException e){
                    e.printStackTrace();
                }
            }

        }
    }

    class PullHandler extends SubscriberHandler {

        HashMap<String, Topic> topics;


        public PullHandler(
                HashMap<String, Topic> topics,
                String username
                ) {
            super(username);
            this.topics = topics;
        }

        private MultimediaFile receiveFile() throws Exception{//data transfer with chunking
            MultimediaFile mf_rcv = (MultimediaFile) this.cbtIn.readObject();

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
            while(true){
                try {
                    this.cbtSocket = new Socket("localhost", 5001);
                    this.cbtOut = new ObjectOutputStream(cbtSocket.getOutputStream());
                    this.cbtIn = new ObjectInputStream(cbtSocket.getInputStream());

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
                        } else if (val_type.equals("MULTIF")) {
                            v = this.receiveFile();
                        }

                        // add to unread queue
                        if(unreads.get(topic_name) == null)
                            unreads.put(topic_name, new ArrayList<Value>());
                        unreads.get(topic_name).add(v);

                        System.out.println(TerminalColors.ANSI_GREEN + v.getSentFrom() + "@" + topic_name + ": " + v + TerminalColors.ANSI_RESET);
                    } while (!this.cbtIn.readUTF().equals("---"));

                    synchronized (this.topics) {
                        for (String topicName : unreads.keySet()) {
                            Topic localTopic = this.topics.get(topicName);
                            ArrayList<Value> unreadValues = unreads.get(topicName);
                            for (Value val : unreadValues) {
                                localTopic.addMessage(val);
                            }
                        }
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    try{
                        this.cbtIn.close();
                        this.cbtOut.close();
                        if (!this.cbtSocket.isClosed()){
                            this.cbtSocket.close();
                        }
                    }catch (IOException e){
                        e.printStackTrace();
                    }
                }

                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

        }
    }


}
