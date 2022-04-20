package com.simplegram.src;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;

public class UserNode implements Publisher, Consumer {

    // <topicName , arraylist of values (msgs, etc)
    HashMap<String, ArrayList<Value>> topics;

    // The list of brokers that the node requires
    // in order to publish and consume data.
    ArrayList<Broker> brokers;
    private Socket socket;
    private BufferedReader bufferedReader;
    private BufferedWriter bufferedWriter;
    private String username;

    public UserNode(Socket socket) {
        try {
            this.socket = socket;
            this.bufferedWriter = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
            this.bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            // kapou edw read to username isws
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }


    @Override
    public void push(String topicName, Value val) {

    }


}
