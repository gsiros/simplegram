package com.simplegram.src.frs;

import com.simplegram.src.*;
import com.simplegram.src.logging.TerminalColors;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;

public class FRSRequestHandler extends Thread {
    private Socket frsReqSocket;
    private ObjectOutputStream out;
    private ObjectInputStream in;
    private HashMap<String, Topic> topics;
    private ArrayList<InetAddress> brokerAddresses;
    
    public FRSRequestHandler(
            Socket frsReqSocket,
            ArrayList<InetAddress> brokerAddresses,
            HashMap<String, Topic> topics
    ){
        this.brokerAddresses = brokerAddresses;
        this.frsReqSocket = frsReqSocket;
        this.topics = topics;
    }

    /**
     * This method handles all types of UserNode requests; SUB/UNSUB/PUSH/PULL
     * It updates all the data structures of the broker.
     */
    @Override
    public void run() {

        try{
            this.in = new ObjectInputStream(this.frsReqSocket.getInputStream());
            this.out = new ObjectOutputStream(this.frsReqSocket.getOutputStream());

            String request;
            while (!this.frsReqSocket.isClosed()) {
                request = this.in.readUTF();

                if (request == null) {
                    //do nothing
                } else if (request.equals("PUSH")) {
                    // Get user name:
                    String user_name = this.in.readUTF();
                    // Get topic name:
                    String topic_name = this.in.readUTF();
                    // I AM RESPONSIBLE.
                    // SEND 'CONFIRM'
                    // CONTINUE...

                    this.out.writeUTF("CONFIRM");
                    this.out.flush();

                    // Check if user is allowed to push in this topic.
                    if (this.topics.get(topic_name).isSubbed(user_name)) {

                        out.writeUTF("OK");
                        out.flush();

                        String val_type = this.in.readUTF();
                        Value incoming_value = null;
                        if (val_type.equals("MSG")) {
                            incoming_value = (Message) this.in.readObject();
                        } else if (val_type.equals("MULTIF") || val_type.equals("STORY")) {
                            // GET FILE INFO.
                            incoming_value = receiveFile(val_type);
                        }

                        synchronized (this.topics) {
                            if (val_type.equals("STORY")){
                                this.topics.get(topic_name).addStory((Story) incoming_value);
                            } else {
                                this.topics.get(topic_name).addMessage(incoming_value);
                            }
                        }


                        System.out.println(TerminalColors.ANSI_PURPLE + "USR: " + user_name + " pushed value: '" + incoming_value + "' in topic: " + topic_name + " ." + TerminalColors.ANSI_RESET);
                    } else {
                        out.writeUTF("NOK");
                        out.flush();
                    }

                    out.writeUTF("END");
                    out.flush();


                } else if (request.equals("SUB")) {
                    String user_name = this.in.readUTF();
                    String topic_name = this.in.readUTF();


                    // I AM RESPONSIBLE.
                    // SEND 'CONFIRM'
                    // CONTINUE...
                    this.out.writeUTF("CONFIRM");
                    this.out.flush();


                    synchronized (this.topics) {
                        if (this.topics.keySet().contains(topic_name)) {
                            this.topics.get(topic_name).addUser(user_name);
                            System.out.println(TerminalColors.ANSI_PURPLE + "USR: " + user_name + " subscribed to topic: " + topic_name + "." + TerminalColors.ANSI_RESET);
                        } else {
                            this.topics.put(topic_name, new Topic(topic_name));
                            this.topics.get(topic_name).addUser(user_name);
                            this.topics.get(topic_name).setAssignedBrokerID(
                                    //TODO: check again for bugs...
                                this.brokerAddresses.indexOf(this.frsReqSocket.getInetAddress())
                            );
                            System.out.println(TerminalColors.ANSI_PURPLE + "USR: " + user_name + " created topic: " + topic_name + "!" + TerminalColors.ANSI_RESET);
                        }
                    }

                    this.out.writeUTF("OK");
                    this.out.flush();


                }else if (request.equals("UNSUB")) {

                    String user_name = this.in.readUTF();
                    String topic_name = this.in.readUTF();


                    // I AM RESPONSIBLE.
                    // SEND 'CONFIRM'
                    // CONTINUE...
                    this.out.writeUTF("CONFIRM");
                    this.out.flush();


                    synchronized (this.topics) {
                        if (this.topics.keySet().contains(topic_name)) {
                            this.topics.get(topic_name).removeUser(user_name);
                            this.out.writeUTF("OK");
                            this.out.flush();
                            System.out.println(TerminalColors.ANSI_PURPLE + "USR: " + user_name + " unsubscribed from topic: " + topic_name + "." + TerminalColors.ANSI_RESET);
                        } else {
                            this.out.writeUTF("NOK");
                            this.out.flush();
                        }
                    }



                } else if (request.equals("PULL")) {
                    // TODO: IMPLEMENT...
                    HashMap<String, ArrayList<Value>> unreads = new HashMap<String, ArrayList<Value>>();
                    String user_name = this.in.readUTF();

                    System.out.println(TerminalColors.ANSI_PURPLE + "USR: " + user_name + " issued a pull request!" + TerminalColors.ANSI_RESET);
                    synchronized (this.topics) {
                        for (Topic t : this.topics.values()) {
                            if (t.isSubbed(user_name)) {
                                unreads.put(t.getName(), t.getLatestFor(user_name));
                            }
                        }
                    }

                    for (String topic_name : unreads.keySet())
                        for (Value v : unreads.get(topic_name))
                            this.send(topic_name, v);

                    this.out.writeUTF("---");
                    this.out.flush();
                }else if(request.equals("QUIT")){
                    System.out.println(this.frsReqSocket+" is finished.");
                    request = null;
                    this.shutdown();
                }
            }
        }catch (Exception e){
            e.printStackTrace();
            this.shutdown();
        }

    }

    /**
     * This method is used during the usernode's pull request in order
     * to send an unread value from a topic.
     * @param topic_name the topic name of the unread value
     * @param v the unread value of the topic
     */
    private void send(String topic_name, Value v) {

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
            } else if (v instanceof Story){
                Story story = (Story) v;
                // TYPE
                this.out.writeUTF("STORY");
                this.out.flush();
                // MULTIMEDIA FILE
                this.sendFile(story);
            }else if (v instanceof MultimediaFile){
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

    /**
     * This method is used by the 'send' method in order to send a multimedia
     * file in CHUNKS and not whole.
     * @param mf2send the MultimediaFile object to send
     */
    private void sendFile(MultimediaFile mf2send){
        try {
            int bytes = 0;

            ArrayList<byte[]> chunks = mf2send.getChunks();

            MultimediaFile mf2send_empty;

            if(mf2send instanceof Story){
                mf2send_empty = new Story(
                        mf2send.getDateSent(),
                        mf2send.getSentFrom(),
                        mf2send.getFilename(),
                        chunks.size(),
                        new ArrayList<byte[]>() // empty file
                );
            } else {
                mf2send_empty = new MultimediaFile(
                        mf2send.getDateSent(),
                        mf2send.getSentFrom(),
                        mf2send.getFilename(),
                        chunks.size(),
                        new ArrayList<byte[]>() // empty file
                );
            }

            // ATTENTION: Only the MultimediaFile's metadata are
            // sent as an object. NOT THE CHUNKS. The chunks are
            // sent separately.
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

    /**
     * This method is used by UserHandler in order to receive the chunks of a
     * multimediafile.
     * @param val_type
     * @return MultimediaFile
     * @throws Exception
     */
    private MultimediaFile receiveFile(String val_type) throws Exception{//data transfer with chunking

        MultimediaFile mf_rcv;

        /*
         * ATTENTION! ONLY the metadata of the multimedia object is
         * received with readObject().
         * The chunks are received separately.
         * */
        if(val_type.equals("MULTIF")){
            mf_rcv = (MultimediaFile) in.readObject();
        } else {
            mf_rcv = (Story) in.readObject();

        }

        int size = mf_rcv.getFileSize();// amount of expected chunks

        byte[] buffer = new byte[512*1024]; //512 * 2^10 (512KByte chunk size)

        while (size>0) {
            in.readFully(buffer, 0, 512*1024);
            mf_rcv.getChunks().add(buffer.clone());
            size --;
            System.out.println(size);
        }
        System.out.println("Received file: "+mf_rcv.getFilename());
        return mf_rcv;
    }

    /**
     * This method is used in order to shutdown the connection
     * between the Broker and the UserNode.
     */
    private void shutdown(){
        try {
            in.close();
            out.close();
            if(!this.frsReqSocket.isClosed()){
                this.frsReqSocket.close();
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    
}
