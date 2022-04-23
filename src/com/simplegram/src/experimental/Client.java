package com.simplegram.src.experimental;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;


public class Client{
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

            String serverResponse;//listen for server responses
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
                    if(input.equals("send")){
                        out.writeUTF("send");
                        out.flush();
                        System.out.println("Give the name of the file");
                        String filename = inputReader.readLine();
                        sendFile(filename);
                        System.out.println("Next Command:");
                    }else if(input.equals("quit")){
                        out.writeUTF("quit");
                        out.flush();
                        inputReader.close();
                        shutdown();
                    }else{
                        System.out.println("Not a valid input");
                    }
                }
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        private void sendFile(String path){
            try {
                int bytes = 0;
                File file = new File(path);
                FileInputStream fileInputStream = new FileInputStream(file);
                ArrayList<byte[]> chunks = new ArrayList<byte[]>();


                // break file into chunks

                byte[] buffer = new byte[512 * 1024];
                while ((bytes = fileInputStream.read(buffer)) != -1) {
                    chunks.add(buffer.clone());

                }
                // send file size
                out.writeInt(chunks.size());
                out.flush();
                //send file type
                out.writeUTF(path.substring(path.lastIndexOf("/") + 1));
                out.flush();
                //boolean check = false;
                //out.writeBoolean(true);
                for (int i = 0; i < chunks.size(); i++) {
                    System.out.println("Sending chunk #" + i);
                    out.write(chunks.get(i), 0, 512 * 1024);
                    out.flush();
                    //while (!check){check = in.readBoolean();}
                    //check = false;
                }
                fileInputStream.close();
            }catch (Exception e){}
        }


    }

    public static void main(String[] args) {
        Client client = new Client();
        client.ClientStart();
    }
}
