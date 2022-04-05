package com.simplegram.suggested;

import java.util.List;

public interface Broker extends Node {
    List<Consumer> registeredUsers = null;
    List<Publisher> registeredPublishers = null;

    Consumer acceptConnection(Consumer con);
    Publisher acceptConnection(Publisher pub);
    void calculateKeys();
    void filterConsumers(String str);
    void notifyBrokersOnChanges();
    void notifyPublisher(String str);
    void pull(String str);

}
