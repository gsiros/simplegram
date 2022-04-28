package com.simplegram.src;

import com.simplegram.src.ibc.BrokerConnection;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;

public class Topic {
    private String name;
    private ArrayList<String> subscribers;
    private HashMap<String, Integer> indexInTopic;
    private HashMap<String, Integer> indexInStories;
    private ArrayList<Value> messageQueue;
    private ArrayList<Story> storyQueue;
    private int assignedBrokerID;

    public Topic(String name) {
        this.name = name;
        this.subscribers = new ArrayList<String>();
        this.messageQueue = new ArrayList<Value>();
        this.storyQueue = new ArrayList<Story>();
        this.indexInTopic = new HashMap<String, Integer>();
        this.indexInStories = new HashMap<String, Integer>();
        this.assignedBrokerID = -1;
    }

    public int getAssignedBrokerID() {
        return this.assignedBrokerID;
    }

    public void setAssignedBrokerID(int newAssignedBrokerID) {
        this.assignedBrokerID = newAssignedBrokerID;
    }

    public void addUser(String user) {
        this.subscribers.add(user);
        this.indexInTopic.put(user, 0);
        this.indexInStories.put(user, 0);
    }

    public void removeUser(String user) {
        this.subscribers.remove(user);
        this.indexInTopic.remove(user);
        this.indexInStories.remove(user);
    }

    public void addMessage(Value message) {
        this.messageQueue.add(message);
    }

    public void addStory(Story story){
        this.storyQueue.add(story);
    }

    public ArrayList<Value> getMessageQueue() {
        return this.messageQueue;
    }

    public ArrayList<Story> getStoryQueue() {
        return this.storyQueue;
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

    public ArrayList<Value> getLatestFor(String user){
        ArrayList<Value> msgs = new ArrayList<Value>();
        int currIndexInTopic = this.indexInTopic.get(user);
        int currIndexInStories = this.indexInStories.get(user);
        for(int i = currIndexInTopic; i<this.messageQueue.size(); i++){
            msgs.add(this.messageQueue.get(i));
        }
        for(int j = currIndexInStories; j<this.storyQueue.size(); j++){
            if(!this.storyQueue.get(j).hasExpired())
                msgs.add(this.storyQueue.get(j));
        }
        this.indexInTopic.put(user, this.messageQueue.size());
        this.indexInStories.put(user, this.messageQueue.size());
        return msgs;
    }

    public String getName() {
        return this.name;
    }

    public void cleanStories(){
        if(this.storyQueue.size()!=0 && this.storyQueue.get(0).hasExpired()) {
            this.storyQueue.remove(0);
            System.out.println("1 story removed!");
        }
    }
}
