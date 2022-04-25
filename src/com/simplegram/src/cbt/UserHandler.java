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
            while (!this.userConSocket.isClosed()){
                request = this.in.readUTF();

                if(request == null){
                    //do nothing
                }else if(request.equals("PUSH")){
                    // TODO: handle PUSH action.
                    // Get user name:
                    String user_name = this.in.readUTF();
                    // Get topic name:
                    String topic_name = this.in.readUTF();
                    // Check if user is allowed to post in this sub.
                    if(this.topics.get(topic_name).isSubbed(user_name)){
                        String val_type = this.in.readUTF();
                        Value incoming_value = null;
                        if(val_type.equals("MSG")){
                            incoming_value = (Message) this.in.readObject();
                        } else if (val_type.equals("MULTIF")){
                            // GET FILE INFO.
                            incoming_value = (MultimediaFile) this.in.readObject();
                            // TODO: receive file chunks...
                        }
                        synchronized (this.topics){
                            this.topics.get(topic_name).addMessage(incoming_value);
                        }

                        out.writeUTF("OK");
                        out.flush();
                        System.out.println(TerminalColors.ANSI_PURPLE+"USR: "+user_name+" pushed value: '"+incoming_value+"' in topic: "+topic_name+" ."+TerminalColors.ANSI_RESET);
                    } else {
                        out.writeUTF("NOK");
                        out.flush();
                    }


                } else if(request.equals("SUB")){
                    // TODO: handle SUB action.
                    String user_name = this.in.readUTF();
                    String topic_name = this.in.readUTF();
                    synchronized (this.topics){
                        if(this.topics.keySet().contains(topic_name)) {
                            this.topics.get(topic_name).addUser(user_name);
                            this.out.writeUTF("OK");
                            this.out.flush();
                            System.out.println(TerminalColors.ANSI_PURPLE+"USR: "+user_name+" subscribed to topic: "+topic_name+"."+TerminalColors.ANSI_RESET);
                        } else {
                            this.out.writeUTF("NOK");
                            this.out.flush();
                        }
                    }
                } else if(request.equals("PULL")){
                    // TODO: handle PULL action.
                    HashMap<String, ArrayList<Value>> unreads = new HashMap<String, ArrayList<Value>>();
                    String user_name = this.in.readUTF();

                    System.out.println(TerminalColors.ANSI_PURPLE+"USR: "+user_name+" issued a pull request!"+TerminalColors.ANSI_RESET);
                    synchronized (this.topics){
                        for (Topic t : this.topics.values()){
                            if(t.isSubbed(user_name)){
                                unreads.put(t.getName() ,t.getLatestMessagesFor(user_name));
                            }
                        }
                    }

                    for(String topic_name : unreads.keySet())
                        for(Value v : unreads.get(topic_name))
                            this.send(topic_name, v);

                    this.out.writeUTF("---");
                    this.out.flush();
                } else if(request.equals("QUIT")){
                    System.out.println(this.userConSocket+" is finished.");
                    request = null;
                    this.shutdown();
                }
            }
        }catch (Exception e){
            e.printStackTrace();
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
                System.out.println("HERE!");
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
                //this.sendFile(mf);
            }
            this.out.writeUTF("EOV");
            this.out.flush();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private void sendFile(MultimediaFile mf) throws Exception {

        int bytes = 0;
        File file = new File(mf.getSourceDirectory());
        FileInputStream fileInputStream = new FileInputStream(file);

        // send file size
        this.out.writeLong(file.length());
        this.out.flush();
        //send file type
        this.out.writeUTF(mf.getSourceDirectory().substring(mf.getSourceDirectory().lastIndexOf("/") + 1));
        this.out.flush();
        // break file into chunks
        byte[] buffer = new byte[512 * 1024];
        while ((bytes = fileInputStream.read(buffer)) != -1) {
            this.out.write(buffer, 0, bytes);
            this.out.flush();
        }
        fileInputStream.close();
    }


    private void receiveFile() throws Exception{ //data transfer with chunking
        int bytes = 0;
        long size = this.in.readLong();// read file size
        String title = this.in.readUTF();// read file name
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
