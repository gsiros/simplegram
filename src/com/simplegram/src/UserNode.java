package com.simplegram.src;

import com.simplegram.src.ibc.BrokerConnection;

import java.io.*;
import java.net.InetAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;

public class UserNode {

    // <topicName , arraylist of values (msgs, etc)
    HashMap<String, ArrayList<Value>> messageLists;

    // The list of brokers that the node requires
    // in order to publish and consume data.

    private ArrayList<InetAddress> brokerAddresses;
    private HashMap<InetAddress, BrokerConnection> brokerConnections;

    private Socket cbtSocket;
    private boolean daemon;

    public UserNode() throws IOException {
        this.daemon = true;
        this.cbtSocket = new Socket("localhost", 5000);
    }

    public void userStart() {


        // TODO: we might need two sockets after all...

        ActionsForPublisher publisherHandler = new ActionsForPublisher(this.cbtSocket);
        publisherHandler.start();

        ActionsForSubscriber subscriberHandler = new ActionsForSubscriber(this.cbtSocket);
        subscriberHandler.start();

        System.out.println("Connected to network..");


    }

    public void shutdown() {
        daemon = false;
        try {
            if (!this.cbtSocket.isClosed()) {
                this.cbtSocket.close();
            }

        } catch (IOException e) {
            //
        }
    }


    class ActionsForPublisher extends Thread {

        private Socket cbtSocket;
        private DataInputStream cbtIn;
        private DataOutputStream cbtOut;

        public ActionsForPublisher(Socket cbtSocket) {
            this.cbtSocket = cbtSocket;
            try {
                this.cbtIn = new DataInputStream(this.cbtSocket.getInputStream());
                this.cbtOut = new DataOutputStream(this.cbtSocket.getOutputStream());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void run() {
            try {
                BufferedReader inputReader = new BufferedReader(new InputStreamReader(System.in));
                while (daemon) {
                    String input = inputReader.readLine();
                    switch (input) {
                        case ("send"): {
                            this.cbtOut.writeUTF("send");
                            this.cbtOut.flush();
                            System.out.println("Give the name of the file");
                            String filename = inputReader.readLine();
                            push(filename);
                            System.out.println(this.cbtIn.readUTF());
                            break;
                        }
                        case ("quit"): {
                            this.cbtOut.writeUTF("quit");
                            this.cbtOut.flush();
                            inputReader.close();
                            shutdown();
                            break;
                        }
                        default: {
                            System.out.println("Not a valid input");
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private void push(String path) throws Exception {
            int bytes = 0;
            File file = new File(path);
            FileInputStream fileInputStream = new FileInputStream(file);

            // send file size
            this.cbtOut.writeLong(file.length());
            this.cbtOut.flush();
            //send file type
            this.cbtOut.writeUTF(path.substring(path.lastIndexOf("/") + 1));
            this.cbtOut.flush();
            // break file into chunks
            byte[] buffer = new byte[512 * 1024];
            while ((bytes = fileInputStream.read(buffer)) != -1) {
                this.cbtOut.write(buffer, 0, bytes);
                this.cbtOut.flush();
            }
            fileInputStream.close();
        }
    }

    class ActionsForSubscriber extends Thread {

        private Socket cbtSocket;
        private DataInputStream cbtIn;
        private DataOutputStream cbtOut;

        public ActionsForSubscriber(Socket cbtSocket) {
            this.cbtSocket = cbtSocket;
            try {
                this.cbtIn = new DataInputStream(this.cbtSocket.getInputStream());
                this.cbtOut = new DataOutputStream(this.cbtSocket.getOutputStream());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void run() {
            try {
                // TODO: periodically ask for updates.
                /*String request;
                while(!this.cbtSocket.isClosed()) {
                    request = this.cbtIn.readUTF();
                    if (request == "pull") {
                        String topicName = this.cbtIn.readUTF();
                        pull(topicName);
                    }
                }*/
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private void pull(String topicName) {

        }
    }

    public HashMap<String, ArrayList<Value>> getMessageLists() {
        return messageLists;
    }
}
