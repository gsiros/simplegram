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

    //private HashMap<Integer, BrokerHandler> brokers; // list of broker connections

    /*
    * Kind controversial data structures...
    * A unified userconnections arraylist should be
    * used instead.
    * */
    // TODO: unify the data structures...
    //private ArrayList<SubscriberHandler> connectedSubscribers;
    //private ArrayList<PublisherHandler> connectedPublishers;
    // to be replaced...

    // Broker data structures
    private HashMap<String, Topic> topics;
    private HashMap<String, ArrayList<Value>> messageQueue; //messages to be sent <topicname, message>
    private boolean daemon;


    // IBC protocol
    private DatagramSocket ibcSocket;
    private ArrayList<InetAddress> brokerAddresses;
    private HashMap<InetAddress, BrokerConnection> brokerConnections;

    /*
    * Again, a single socket should be used instead...
    * */
    // TODO: unify sockets...
    //private ServerSocket pubServiceProviderSocket;
    //private ServerSocket subServiceProviderSocket;
    // Communication Between Terminals socket.
    private ServerSocket cbtSocket;

    public Broker(String brokers_addr_file){
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
        // TODO: handle connections from usernodes.

        // Bind server tcp socket:
        this.cbtSocket = new ServerSocket(5000);

        while(true){
            Socket userConSocket = this.cbtSocket.accept();
            UserHandler uh = new UserHandler(userConSocket, this.topics);
            uh.start();
        }

    }


    public void startBroker() throws IOException {

        this.topics = new HashMap<String, Topic>();
        this.messageQueue = new HashMap<String, ArrayList<Value>>();
        this.daemon = true;

        this.topics.put("test", new Topic("test"));
        this.topics.put("tes1", new Topic("tes1"));

        // Start IBC service.
        this.startInterBrokerCommunication();
        // Start CBT service. -- WARNING, while-true loop in startCBT.
        this.startCommunicationBetweenTerminals();

        try {
            BufferedReader input = new BufferedReader(new InputStreamReader(System.in));
            //System.out.println("Give port number(Client Server): ");
            //int port = Integer.parseInt(input.readLine());
            //userServiceProviderSocket = new ServerSocket(port);

            // for now:
            //pubServiceProviderSocket = new ServerSocket(5000);
            //subServiceProviderSocket = new ServerSocket(5001);

            /*System.out.println("Give port number(Broker Communication)");
            int port = Integer.parseInt(input.readLine());
            brokerServiceProviderSocket = new ServerSocket(port);

            for (int i = 1; i <= 2; i++) {
                System.out.println("Give IP for Broker " + i + ":");
                String ip = input.readLine();
                System.out.println("Give port number for Broker " + i + ":");
                port = Integer.parseInt(input.readLine());

                BrokerHandler brokerHandler = new BrokerHandler(new Socket(ip, port));
                Thread brokerHandlerThread = new Thread(brokerHandler);
                brokers.put(i, brokerHandler);
                brokerHandlerThread.start();
            }*/

            //TODO: start pub and sub services..
            //Publisher Service
            /*new Thread(new Runnable() {
                @Override
                public void run() {
                    try{
                        //accept incoming connections
                        while(daemon) {
                            Socket userSocket = pubServiceProviderSocket.accept();
                            System.out.println(userSocket + "connected.");

                            PublisherHandler handler = new PublisherHandler(userSocket);
                            Thread thread = new Thread(handler);
                            connectedPublishers.add(handler);
                        }
                    }catch (IOException e){

                    }

                }
            }).start();

            //Subscriber Service
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try{
                        //accept incoming connections
                        while(daemon) {
                            Socket userSocket = subServiceProviderSocket.accept();
                            System.out.println(userSocket + "connected.");

                            SubscriberHandler handler = new SubscriberHandler(userSocket);
                            Thread thread = new Thread(handler);
                            connectedSubscribers.add(handler);
                        }
                    }catch (IOException e){

                    }

                }
            }).start();*/

        } catch (Exception E) {
            //
        }


    }

    // delete se ligo
    // begin the main functionality,
    // start accepting connections.
    /*
    void start(){
        try {

            // User listening on port 4444
            userServiceProviderSocket = new ServerSocket(4444);
            // Broker listening on port 4445
            brokerServiceProviderSocket = new ServerSocket(4445);

            while (true) {

                // TODO: HANDLE CONNECTIONS; PART 1
                brokerConnection = brokerServiceProviderSocket.accept();
                Thread t_b = new ActionsForBroker(brokerConnection);
                t_b.start();


                // TODO: HANDLE CONNECTIONS; PART 2
                // Accept incoming connection FROM USERNODE.
                userConnection = userServiceProviderSocket.accept();
                Thread t = new ActionsForUser(userConnection);
                t.start();

            }
        } catch (IOException ioException) {
            ioException.printStackTrace();
        } finally {
            try {
                userServiceProviderSocket.close();
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        }
    }
    */
    


/*
TODO: pull method

    public void pull(String topicName) { //update all subscribers of Topic("topicName")
        Topic topic = topics.get(topicName);
        for (UserNode usrNode : topic.getSubscribers()){
            int lastMsgIndex = connectedSubscribers.get(connectedSubscribers.indexOf(usrNode)).getMessageLists().get(topicName).size(); // ask the user for the index of the latest message in the user's msgQueue on said topic
            while (lastMsgIndex <= topic.getMessageQueue().size()){
                Value msg = topic.getMessageQueue().get(lastMsgIndex);//message to be delivered
                //send msg to user
                //ask for new messageListSize and repeat loop
            }
        }
    }
*/

    class PublisherHandler implements Runnable{//actions for Server

        private Socket client;
        private DataOutputStream out;
        private DataInputStream in;

        public PublisherHandler(Socket client){
            this.client = client;
        }

        @Override
        public void run() {
            try{
                in = new DataInputStream(client.getInputStream());
                out = new DataOutputStream(client.getOutputStream());
                out.writeUTF("Connection Established. Enter a request...");
                out.flush();
                String request;
                while (!client.isClosed()){
                    request = in.readUTF();
                    if(request == null){
                        ;//do nothing
                    }else if(request.equals("send")){
                        receiveFile();
                        out.writeUTF("File Received!");
                        request = null;
                        out.writeUTF("Anything else?");
                        out.flush();
                    }else if(request.equals("quit")){
                        System.out.println(client+" is finished.");
                        request = null;
                        shutdown();
                    }
                }
            }catch (Exception e){
                e.printStackTrace();
            }
        }

        private void receiveFile() throws Exception{//data transfer with chunking
            int bytes = 0;
            long size = in.readLong();// read file size
            String title = in.readUTF();// read file name
            //String type = title.substring(title.lastIndexOf('.'+1));//determine data type

            FileOutputStream fileOutputStream = new FileOutputStream(title);
            byte[] buffer = new byte[512*1024]; //512 * 2^10 (512KByte chunk size)
            while (size > 0 && (bytes = in.read(buffer, 0, (int)Math.min(buffer.length, size))) != -1) {
                fileOutputStream.write(buffer,0,bytes);
                size -= bytes;      // read upto file size
            }
            fileOutputStream.close();
            System.out.println("Received file: "+title);
        }

        public void shutdown(){
            try {
                in.close();
                out.close();
                if(!client.isClosed()){
                    client.close();
                }
            }catch (Exception e){
                //
            }
        }
    }

    class SubscriberHandler implements Runnable{

        private Socket client;
        private DataOutputStream out;
        private DataInputStream in;

        public SubscriberHandler(Socket client){
            this.client = client;
        }

        @Override
        public void run() {
            try{
                in = new DataInputStream(client.getInputStream());
                out = new DataOutputStream(client.getOutputStream());

            }catch(Exception e){
                e.printStackTrace();
            }

        }
        public void shutdown(){
            try {
                in.close();
                out.close();
                if(!client.isClosed()){
                    client.close();
                }
            }catch (Exception e){
                //
            }
        }
    }

    class BrokerHandler implements Runnable {
        private Socket socket;

        public BrokerHandler(Socket socket) {
            this.socket = socket;
        }

        public void run() {

        }
    }
}
