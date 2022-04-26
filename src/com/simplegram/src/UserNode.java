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

        SubHandler sh = new SubHandler("george", this.topics);
        sh.start();

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




    class PubHandler extends Thread {

        private Socket cbtSocket;
        private ObjectInputStream cbtIn;
        private ObjectOutputStream cbtOut;

        public PubHandler(Socket cbtSocket){
            this.cbtSocket = cbtSocket;
        }

        @Override
        public void run() {

        }
    }

    class SubHandler extends Thread {

        private Socket cbtSocket;
        private ObjectInputStream cbtIn;
        private ObjectOutputStream cbtOut;

        private String username;
        private HashMap<String, Topic> topics;

        public SubHandler(
                String username,
                HashMap<String, Topic> topics
        ) {
            this.username = username;
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
