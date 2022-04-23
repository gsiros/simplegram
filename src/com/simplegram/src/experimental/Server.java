package com.simplegram.src.experimental;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;

public class Server{

    private ArrayList<ConnectionHandler> connections;
    private boolean daemon;
    private ServerSocket serverSocket;

    public Server(){
        connections = new ArrayList<>();
        daemon = true;
    }

    public void startBroker(){
        try{
            serverSocket = new ServerSocket(5000);//initialize server
            System.out.println("Listening to port:5000");
            System.out.println("Press sd at any moment to shut down server");

            new Thread(new Runnable() {//Constantly accept connections until Shutdown
                @Override
                public void run() {
                    while(daemon){
                        try{
                            Socket clientSocket = serverSocket.accept();
                            System.out.println(clientSocket+" connected.");
                            ConnectionHandler handler = new ConnectionHandler(clientSocket);
                            Thread thread = new Thread(handler);
                            connections.add(handler);
                            thread.start();
                        }catch (Exception e){
                            //
                        }
                    }
                }
            }).start();

            new Thread(new Runnable() {//accept command line inputs from admin (mainly for shutdown)
                @Override
                public void run() {
                    try {
                        String command;
                        BufferedReader inputReader = new BufferedReader(new InputStreamReader(System.in));
                        while (daemon){
                            command = inputReader.readLine();
                            if (command.equals("sd")){
                                shutdown();
                            }
                        }
                    }catch (Exception e){
                        //
                    }
                }
            }).start();


        } catch (Exception e){
            e.printStackTrace();
        }
    }

    public void shutdown(){
        try {
            daemon = false;
            System.out.println("Shutting down connections..");
            for(ConnectionHandler con: connections){//Close all client sockets
                con.shutdown();
            }
            System.out.println("Shutting down server..");
            if(!serverSocket.isClosed()){//close the server socket
                serverSocket.close();
            }
            System.out.println("Done");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    class ConnectionHandler implements Runnable{//actions for Server

        private Socket client;
        private DataOutputStream out;
        private DataInputStream in;


        public ConnectionHandler(Socket client){
            this.client = client;
        }

        @Override
        public void run() {
            try{
                in = new DataInputStream(client.getInputStream());
                out = new DataOutputStream(client.getOutputStream());
                out.writeUTF("Successfully connected to Server. Please issue a command:");
                out.flush();
                String request;
                while (!client.isClosed()){
                    request = in.readUTF();
                    if(request == null){
                        ;//do nothing
                    }else if(request.equals("send")){
                        receiveFile();
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
            int size = in.readInt();// amount of expected chunks
            String title = in.readUTF();// read file name
            //String type = title.substring(title.lastIndexOf('.'+1));//determine data type

            FileOutputStream fileOutputStream = new FileOutputStream(title);
            byte[] buffer = new byte[512*1024]; //512 * 2^10 (512KByte chunk size)
            //boolean check = false;
            //while (check == false){
            //    check = in.readBoolean();
            //}
            while (size>0) {
                in.readFully(buffer, 0, 512*1024);
                fileOutputStream.write(buffer,0,512*1024);
                size --;
                System.out.println(size);
                //out.writeBoolean(true);
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

    public static void main(String[] args) {
        Server server = new Server();
        server.startBroker();
    }
}