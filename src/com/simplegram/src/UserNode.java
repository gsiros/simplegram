package com.simplegram.src;

import java.util.ArrayList;
import java.util.HashMap;

public class UserNode implements Publisher, Consumer {

    // <topicName , arraylist of values (msgs, etc)
    HashMap<String, ArrayList<Value>> topics;

    // The list of brokers that the node requires
    // in order to publish and consume data.
    ArrayList<Broker> brokers;


    @Override
    public void pull(String topicName) {

    }

    @Override
    public void push(String topicName, Value val) {

    }
}
