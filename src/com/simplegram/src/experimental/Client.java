package com.simplegram.src.experimental;

import com.simplegram.src.Message;
import com.simplegram.src.logging.TerminalColors;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;


public class Client{
    private Socket client;
    private ObjectInputStream in;
    private ObjectOutputStream out;
    private boolean daemon;// we put it here (and not in Handler) to be able to shutdown from Client

    public Client(){
        daemon = true;
    }

    public void ClientStart() {
        try {
            client = new Socket("localhost", 5000);
            out = new ObjectOutputStream(client.getOutputStream());
            in = new ObjectInputStream(client.getInputStream());

            InputHandler handler = new InputHandler();
            Thread t = new Thread(handler);
            t.start();

            String serverResponse;//listen for server responses
            while (!client.isClosed()){
                //serverResponse = in.readUTF();
                //System.out.println(TerminalColors.ANSI_BLUE+serverResponse+TerminalColors.ANSI_RESET);
            }
        } catch (IOException e){
            e.printStackTrace();
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
                System.out.println("Chose action: SUB, PUSH, PULL:");
                while (daemon) {
                    String input = inputReader.readLine();
                    if (input.equals("SUB")) {

                        out.writeUTF("SUB");
                        out.flush();

                        System.out.println("Username?");
                        out.writeUTF(inputReader.readLine());
                        out.flush();


                        System.out.println("Topic to send to?");
                        out.writeUTF(inputReader.readLine());
                        out.flush();


                        // server reply
                        System.out.println(in.readUTF());

                        System.out.println("Next Command:");
                    } else if (input.equals("PUSH")) {

                        out.writeUTF("PUSH");
                        out.flush();

                        System.out.println("Username?");
                        String user = inputReader.readLine();
                        out.writeUTF(user);
                        out.flush();

                        System.out.println("Topic to send to?");
                        out.writeUTF(inputReader.readLine());
                        out.flush();

                        out.writeUTF("MSG");
                        out.flush();

                        System.out.println("Message to send:");
                        Message m2send = new Message(user,inputReader.readLine());
                        out.writeObject(m2send);
                        out.flush();

                        // server reply
                        System.out.println(in.readUTF());

                        System.out.println("Next Command:");

                        //System.out.println("Give the name of the file");
                        //String filename = inputReader.readLine();
                        //sendFile(filename);
                        //System.out.println("Next Command:");
                    } else if(input.equals("PULL")){

                        out.writeUTF("PULL");
                        out.flush();

                        System.out.println("Username?");
                        out.writeUTF(inputReader.readLine());
                        out.flush();

                        do {
                            String topic_name = in.readUTF();
                            if(topic_name.equals("---"))
                                break;
                            String val_type = in.readUTF();
                            Message m = (Message) in.readObject();
                            System.out.println(TerminalColors.ANSI_GREEN+m.getSentFrom()+"@"+topic_name+": "+m.getMsg()+TerminalColors.ANSI_RESET);
                        } while(!in.readUTF().equals("---"));

                        System.out.println("Next Command:");

                    } else if(input.equals("quit")){
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
