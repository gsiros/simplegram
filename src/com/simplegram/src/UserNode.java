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
    //TODO: move pull to Broker
    public void pull(String topicName) { //ask the broker for the message with the latest index in the user's

    }

    @Override
    public void push(String topicName, Value val) {

    }
}
