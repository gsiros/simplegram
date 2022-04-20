package com.simplegram.src;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;

public class UserNode {

    // <topicName , arraylist of values (msgs, etc)
    HashMap<String, ArrayList<Value>> topics;

    // The list of brokers that the node requires
    // in order to publish and consume data.
    ArrayList<Broker> brokers;
    private Socket pubSocket;
    private Socket subSocket;
    private DataInputStream pubIn;
    private DataInputStream subIn;
    private DataOutputStream pubOut;
    private DataOutputStream subOut;
    private boolean daemon;


    public UserNode() {
        this.daemon = true;
    }

    public void userStart() {
        try {
            // Tha xreiastoume kapoia lista or something me ip - brokersockets..
            pubSocket = new Socket("localhost", 5000);
            pubOut = new DataOutputStream(pubSocket.getOutputStream());
            pubIn = new DataInputStream(pubSocket.getInputStream());

            subSocket = new Socket("localhost", 5001);
            subOut = new DataOutputStream(subSocket.getOutputStream());
            subIn = new DataInputStream(subSocket.getInputStream());

            ActionsForPublisher publisherHandler = new ActionsForPublisher();
            Thread pubthread = new Thread(publisherHandler);
            pubthread.start();

            ActionsForSubscriber subscriberHandler = new ActionsForSubscriber();
            Thread subthread = new Thread(subscriberHandler);
            subthread.start();

            System.out.println("Connected to network..");

        } catch (IOException e) {
            //
        }
    }

    public void shutdown() {
        daemon = false;
        try {
            pubIn.close();
            subIn.close();
            pubOut.close();
            subOut.close();
            if (!pubSocket.isClosed()) {
                pubSocket.close();
            }
            if (!subSocket.isClosed()) {
                subSocket.close();
            }
        } catch (IOException e) {
            //
        }
    }


    class ActionsForPublisher implements Runnable {
        @Override
        public void run() {
            try {
                BufferedReader inputReader = new BufferedReader(new InputStreamReader(System.in));
                while (daemon) {
                    String input = inputReader.readLine();
                    switch (input) {
                        case ("send"): {
                            pubOut.writeUTF("send");
                            pubOut.flush();
                            System.out.println("Give the name of the file");
                            String filename = inputReader.readLine();
                            push(filename);
                            System.out.println(pubIn.readUTF());
                            break;
                        }
                        case ("quit"): {
                            pubOut.writeUTF("quit");
                            pubOut.flush();
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
            pubOut.writeLong(file.length());
            pubOut.flush();
            //send file type
            pubOut.writeUTF(path.substring(path.lastIndexOf("/") + 1));
            pubOut.flush();
            // break file into chunks
            byte[] buffer = new byte[512 * 1024];
            while ((bytes = fileInputStream.read(buffer)) != -1) {
                pubOut.write(buffer, 0, bytes);
                pubOut.flush();
            }
            fileInputStream.close();
        }
    }

    class ActionsForSubscriber implements Runnable {

        @Override
        public void run() {
            try {
                String request;
                while(!subSocket.isClosed()) {
                    request = subIn.readUTF();
                    if (request == "pull") {
                        String topicName = subIn.readUTF();
                        pull(topicName);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private void pull(String topicName) {

        }



    }
}
