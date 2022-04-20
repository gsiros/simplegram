package com.simplegram.src;

import java.util.ArrayList;

public class Topic {
    private ArrayList<UserNode> subscribers;
    private ArrayList<Value> messageQueue;

    public Topic() {
        this.subscribers = new ArrayList<UserNode>();
        this.messageQueue = new ArrayList<Value>();
    }

    public void addUser(UserNode user) {
        this.subscribers.add(user);
    }

    public void removeUser(UserNode user) {
        this.subscribers.remove(subscribers.indexOf(user));
    }

    public void addMessage(Value message) {
        this.messageQueue.add(message);
    }

    public ArrayList<Value> getMessageQueue() {
        return this.messageQueue;
    }

    public ArrayList<UserNode> getSubscribers() {
        return this.subscribers;
    }

}
