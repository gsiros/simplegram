package com.simplegram.suggested;

import java.util.ArrayList;

public interface Publisher extends Node {
    ProfileName profileName = null;

    ArrayList<Value> generateChunks(MultimediaFile file);
    // -GS controversial 'void' return type.
    // It is a getter so it should probably return List<Broker>?
    void getBrokerList();
    Broker hashTopic(String topic);
    void notifyBrokersNewMessage(String message);
    void notifyFailure(Broker broker);
    void push(String str, Value val);

}
