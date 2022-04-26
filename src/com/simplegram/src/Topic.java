package com.simplegram.src;

import java.util.ArrayList;
import java.util.HashMap;

public class Topic {
    private String name;
    private ArrayList<String> subscribers;
    private HashMap<String, Integer> indexInTopic;
    private ArrayList<Value> messageQueue;

    public Topic(String name) {
        this.name = name;
        this.subscribers = new ArrayList<String>();
        this.messageQueue = new ArrayList<Value>();
        this.indexInTopic = new HashMap<String, Integer>();
    }

    public void addUser(String user) {
        this.subscribers.add(user);
        this.indexInTopic.put(user, 0);
    }

    public void removeUser(String user) {
        this.subscribers.remove(subscribers.indexOf(user));
        this.indexInTopic.remove(user);
    }

    public void addMessage(Value message) {
        this.messageQueue.add(message);
    }

    public ArrayList<Value> getMessageQueue() {
        return this.messageQueue;
    }

    public ArrayList<String> getSubscribers() {
        return this.subscribers;
    }

    public boolean isSubbed(String user_name) {
        return this.subscribers.contains(user_name);
    }

    public int getIndexOfUser(String user){
        return this.indexInTopic.get(user);
    }

    public ArrayList<Value> getLatestMessagesFor(String user){
        ArrayList<Value> msgs = new ArrayList<Value>();
        int index = this.indexInTopic.get(user);
        for(int i = index; i<this.messageQueue.size(); i++){
            msgs.add(this.messageQueue.get(i));
        }

        this.indexInTopic.put(user, this.messageQueue.size());
        return msgs;
    }

    public String getName() {
        return this.name;
    }
}
