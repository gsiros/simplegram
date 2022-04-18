package com.simplegram.src.ecperimental;

import java.io.*;
import java.net.Socket;

public class Client {
    private Socket client;
    private DataInputStream in;
    private DataOutputStream out;
    private boolean daemon;// we put it here (and not in Handler) to be able to shutdown from Client

    public Client(){
        daemon = true;
    }

    public void ClientStart() {
        try {
            client = new Socket("localhost", 5000);
            out = new DataOutputStream(client.getOutputStream());
            in = new DataInputStream(client.getInputStream());

            InputHandler handler = new InputHandler();
            Thread t = new Thread(handler);
            t.start();

            String serverResponse;
            while (!client.isClosed()){
                serverResponse = in.readUTF();
                System.out.println(serverResponse);
            }
        } catch (IOException e){
            //
        }
    }

    public void shutdown(){
        daemon = false;
        try{
            in.close();
            out.close();
            if (!client.isClosed()){
                client.close();
            }
        }catch (IOException e){
            //
        }
    }

    class InputHandler implements Runnable{

        @Override
        public void run() {
            try {
                BufferedReader inputReader = new BufferedReader(new InputStreamReader(System.in));
                while (daemon){
                    String input = inputReader.readLine();
                    switch (input){
                        case ("send"):{
                            out.writeUTF("send");
                            out.flush();
                            System.out.println("Give the name of the file");
                            String filename = inputReader.readLine();
                            sendFile(filename);
                            System.out.println(in.readUTF());
                            break;
                        }
                        case ("quit"): {
                            out.writeUTF("quit");
                            out.flush();
                            inputReader.close();
                            shutdown();
                            break;
                        }
                        default:{
                            System.out.println("Not a valid input");
                        }
                    }
                }
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        private void sendFile(String path) throws Exception{
            int bytes = 0;
            File file = new File(path);
            FileInputStream fileInputStream = new FileInputStream(file);

            // send file size
            out.writeLong(file.length());
            out.flush();
            //send file type
            out.writeUTF(path.substring(path.lastIndexOf("/")+1));
            out.flush();
            // break file into chunks
            byte[] buffer = new byte[512*1024];
            while ((bytes=fileInputStream.read(buffer))!=-1){
                out.write(buffer,0,bytes);
                out.flush();
            }
            fileInputStream.close();
        }

    }

    public static void main(String[] args) {
        Client client = new Client();
        client.ClientStart();
    }
}
