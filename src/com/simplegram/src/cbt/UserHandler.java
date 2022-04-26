package com.simplegram.src.cbt;

import com.simplegram.src.Message;
import com.simplegram.src.MultimediaFile;
import com.simplegram.src.Topic;
import com.simplegram.src.Value;
import com.simplegram.src.logging.TerminalColors;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;

public class UserHandler extends Thread {

    private Socket userConSocket;
    private ObjectOutputStream out;
    private ObjectInputStream in;
    private HashMap<String, Topic> topics;

    public UserHandler(Socket userConSocket, HashMap<String, Topic> topics){
        this.userConSocket = userConSocket;
        this.topics = topics;
    }

    @Override
    public void run() {
        // TODO: handle user actions

        /**
         * PUSH/SUB/PULL
         */

        try{
            this.in = new ObjectInputStream(this.userConSocket.getInputStream());
            this.out = new ObjectOutputStream(this.userConSocket.getOutputStream());

            String request;
            while (!this.userConSocket.isClosed()) {
                request = this.in.readUTF();

                if (request == null) {
                    //do nothing
                } else if (request.equals("PUSH")) {
                    // TODO: handle PUSH action.
                    // Get user name:
                    String user_name = this.in.readUTF();
                    // Get topic name:
                    //TODO: CHECK TOPIC HASH IF I AM RESPONSIBLE
                    String topic_name = this.in.readUTF();
                    // Check if user is allowed to push in this topic.
                    if (this.topics.get(topic_name).isSubbed(user_name)) {

                        out.writeUTF("OK");
                        out.flush();

                        String val_type = this.in.readUTF();
                        Value incoming_value = null;
                        if (val_type.equals("MSG")) {
                            incoming_value = (Message) this.in.readObject();
                        } else if (val_type.equals("MULTIF")) {
                            // GET FILE INFO.
                            incoming_value = receiveFile();
                        }
                        synchronized (this.topics) {
                            this.topics.get(topic_name).addMessage(incoming_value);
                        }


                        System.out.println(TerminalColors.ANSI_PURPLE + "USR: " + user_name + " pushed value: '" + incoming_value + "' in topic: " + topic_name + " ." + TerminalColors.ANSI_RESET);
                    } else {
                        out.writeUTF("NOK");
                        out.flush();
                    }

                    out.writeUTF("END");
                    out.flush();


                } else if (request.equals("SUB")) {
                    // TODO: handle SUB action.
                    String user_name = this.in.readUTF();
                    String topic_name = this.in.readUTF();
                    synchronized (this.topics) {
                        if (this.topics.keySet().contains(topic_name)) {
                            this.topics.get(topic_name).addUser(user_name);
                            this.out.writeUTF("OK");
                            this.out.flush();
                            System.out.println(TerminalColors.ANSI_PURPLE + "USR: " + user_name + " subscribed to topic: " + topic_name + "." + TerminalColors.ANSI_RESET);
                        } else {

                            this.topics.put(topic_name, new Topic(topic_name));
                            this.topics.get(topic_name).addUser(user_name);

                            this.out.writeUTF("NOK");
                            this.out.flush();
                        }
                    }
                }else if (request.equals("UNSUB")) {

                    String user_name = this.in.readUTF();
                    String topic_name = this.in.readUTF();
                    synchronized (this.topics) {
                        if (this.topics.keySet().contains(topic_name)) {
                            this.topics.get(topic_name).removeUser(user_name);
                            this.out.writeUTF("OK");
                            this.out.flush();
                            System.out.println(TerminalColors.ANSI_PURPLE + "USR: " + user_name + " unsubscribed to topic: " + topic_name + "." + TerminalColors.ANSI_RESET);
                        } else {
                            this.out.writeUTF("NOK");
                            this.out.flush();
                        }
                    }
                } else if (request.equals("PULL")) {
                    // TODO: handle PULL action.
                    HashMap<String, ArrayList<Value>> unreads = new HashMap<String, ArrayList<Value>>();
                    String user_name = this.in.readUTF();

                    System.out.println(TerminalColors.ANSI_PURPLE + "USR: " + user_name + " issued a pull request!" + TerminalColors.ANSI_RESET);
                    synchronized (this.topics) {
                        for (Topic t : this.topics.values()) {
                            if (t.isSubbed(user_name)) {
                                unreads.put(t.getName(), t.getLatestMessagesFor(user_name));
                            }
                        }
                    }

                    for (String topic_name : unreads.keySet())
                        for (Value v : unreads.get(topic_name))
                            this.send(topic_name, v);

                    this.out.writeUTF("---");
                    this.out.flush();
                }else if(request.equals("QUIT")){
                    System.out.println(this.userConSocket+" is finished.");
                    request = null;
                    this.shutdown();
                }
            }
        }catch (Exception e){
            //e.printStackTrace();
            this.shutdown();
        }

    }

    private void send(String topic_name, Value v) {

        // TODO: send the message to user node...
        try {
            // TOPIC NAME
            this.out.writeUTF(topic_name);
            this.out.flush();
            if(v instanceof Message) {
                Message m = (Message) v;
                // TYPE
                this.out.writeUTF("MSG");
                this.out.flush();
                // MESSAGE
                this.out.writeObject(m);
                this.out.flush();
            } else {
                MultimediaFile mf = (MultimediaFile) v;
                // TYPE
                this.out.writeUTF("MULTIF");
                this.out.flush();
                // MULTIMEDIA FILE
                this.sendFile(mf);
            }
            this.out.writeUTF("EOV"); //  End Of Value
            this.out.flush();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private void sendFile(MultimediaFile mf2send){
        try {
            int bytes = 0;

            ArrayList<byte[]> chunks = mf2send.getChunks();

            // Create MultiMedia Object
            MultimediaFile mf2send_empty = new MultimediaFile(
                    mf2send.getSentFrom(),
                    mf2send.getFilename(),
                    chunks.size(),
                    new ArrayList<byte[]>()
            );

            out.writeObject(mf2send_empty);
            out.flush();

            for (int i = 0; i < chunks.size(); i++) {
                System.out.println("Sending chunk #" + i);
                out.write(chunks.get(i), 0, 512 * 1024);
                out.flush();

            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    private MultimediaFile receiveFile() throws Exception{//data transfer with chunking
        int bytes = 0;
        MultimediaFile mf_rcv = (MultimediaFile) in.readObject();

        int size = mf_rcv.getFileSize();// amount of expected chunks
        String filename = mf_rcv.getFilename();// read file name
        //String type = title.substring(title.lastIndexOf('.'+1));//determine data type

        byte[] buffer = new byte[512*1024]; //512 * 2^10 (512KByte chunk size)
        //boolean check = false;
        //while (check == false){
        //    check = in.readBoolean();
        //}
        while (size>0) {
            in.readFully(buffer, 0, 512*1024);
            mf_rcv.getChunks().add(buffer.clone());
            size --;
            System.out.println(size);
            //out.writeBoolean(true);
        }
        System.out.println("Received file: "+mf_rcv.getFilename());
        return mf_rcv;
    }

    private void shutdown(){
        try {
            in.close();
            out.close();
            if(!this.userConSocket.isClosed()){
                this.userConSocket.close();
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
