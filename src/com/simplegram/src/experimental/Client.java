package com.simplegram.src.experimental;

import com.simplegram.src.Message;
import com.simplegram.src.MultimediaFile;
import com.simplegram.src.Value;
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
            client = new Socket("localhost", 5001);
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
                System.out.println("Username?");
                String user = inputReader.readLine();

                System.out.println("Chose action: SUB, PUSH, PULL:");
                while (daemon) {
                    String input = inputReader.readLine();
                    if (input.equals("SUB")) {

                        out.writeUTF("SUB");
                        out.flush();

                        out.writeUTF(user);
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

                        out.writeUTF(user);
                        out.flush();

                        System.out.println("Topic to send to?");
                        out.writeUTF(inputReader.readLine());
                        out.flush();

                        System.out.println("Message type? [MSG/MULTIF]");
                        String type = inputReader.readLine();
                        out.writeUTF(type);
                        out.flush();

                        if(type.equals("MSG")){
                            System.out.println("Message to send:");
                            Message m2send = new Message(user,inputReader.readLine());
                            out.writeObject(m2send);
                            out.flush();
                        } else if (type.equals("MULTIF")) {

                            System.out.println("File path to send:");
                            String path = inputReader.readLine();
                            sendFile(user, path);
                        }



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

                        out.writeUTF(user);
                        out.flush();

                        do {
                            String topic_name = in.readUTF();
                            if(topic_name.equals("---"))
                                break;
                            String val_type = in.readUTF();
                            Value v = null;
                            if(val_type.equals("MSG")){
                                v = (Message) in.readObject();
                            } else if(val_type.equals("MULTIF")) {
                                v = receiveFile();
                            }

                            System.out.println(TerminalColors.ANSI_GREEN+v.getSentFrom()+"@"+topic_name+": "+v+TerminalColors.ANSI_RESET);
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

        private void sendFile(String user, String path){
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

                // Create MultiMedia Object
                MultimediaFile mf2send = new MultimediaFile(
                        user,
                        path.substring(path.lastIndexOf("/") + 1),
                        chunks.size(),
                        new ArrayList<byte[]>()
                );

                out.writeObject(mf2send);
                out.flush();

                for (int i = 0; i < chunks.size(); i++) {
                    out.write(chunks.get(i), 0, 512 * 1024);
                    out.flush();

                }
                fileInputStream.close();
            }catch (Exception e){}
        }

        private MultimediaFile receiveFile() throws Exception{//data transfer with chunking
            MultimediaFile mf_rcv = (MultimediaFile) in.readObject();

            int size = mf_rcv.getFileSize();// amount of expected chunks
            String filename = mf_rcv.getFilename();// read file name

            FileOutputStream fileOutputStream = new FileOutputStream(filename);
            byte[] buffer = new byte[512*1024]; //512 * 2^10 (512KByte chunk size)

            while (size>0) {
                in.readFully(buffer, 0, 512*1024);
                fileOutputStream.write(buffer,0,512*1024);
                size --;
            }
            fileOutputStream.close();
            return mf_rcv;
        }

    }

    public static void main(String[] args) {
        Client client = new Client();
        client.ClientStart();
    }
}
