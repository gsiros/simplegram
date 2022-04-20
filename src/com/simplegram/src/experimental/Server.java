package com.simplegram.src.experimental;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;

public class Server {

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
            System.out.println("listening to port:5000");

            while(daemon){//Constantly accept connections until Shutdown
                Socket clientSocket = serverSocket.accept();
                System.out.println(clientSocket+" connected.");
                ConnectionHandler handler = new ConnectionHandler(clientSocket);
                Thread thread = new Thread(handler);
                connections.add(handler);
                thread.start();
            }



        } catch (Exception e){
            e.printStackTrace();
        }
    }

    public void shutdown(){
        try {
            daemon = false;
            if(!serverSocket.isClosed()){//close the server socket
                serverSocket.close();
            }
            for(ConnectionHandler con: connections){//Close all client sockets
                con.shutdown();
            }
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
                out.writeUTF("Hello There! Which among us cock shall it be today?");
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

        private void receiveFile() throws Exception{//data tansfer with chunking
            int bytes = 0;

            long size = in.readLong();// read file size
            String title = in.readUTF();// read file name
            //String type = title.substring(title.lastIndexOf('.'+1));//determine data type

            FileOutputStream fileOutputStream = new FileOutputStream(title);
            byte[] buffer = new byte[512*1024];
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

    public static void main(String[] args) {
        Server server = new Server();
        server.startBroker();
    }
}