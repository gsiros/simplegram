package com.simplegram.src;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;

public class Broker {

    private ArrayList<UserNode> connectedUsers;
    private HashMap<String, Topic> topics;
    //HashMap<String, ArrayList<Value>> messageQueue; //messages to be sent <topicname, message>
    //TODO:list of other brokers

    private ServerSocket userServiceProviderSocket;
    private ServerSocket brokerServiceProviderSocket;

    private Socket brokerConnection;
    private Socket userConnection;

    // init broker to become ready for
    // new connections with usernodes
    public Broker(){}

    void init(){}





    // begin the main functionality,
    // start accepting connections.
    void start(){
        try {

            // User listening on port 4444
            userServiceProviderSocket = new ServerSocket(4444);
            // Broker listening on port 4445
            brokerServiceProviderSocket = new ServerSocket(4445);

            while (true) {

                // TODO: HANDLE CONNECTIONS; PART 1
                brokerConnection = brokerServiceProviderSocket.accept();
                Thread t_b = new ActionsForBroker(brokerConnection);
                t_b.start();


                // TODO: HANDLE CONNECTIONS; PART 2
                // Accept incoming connection FROM USERNODE.
                userConnection = userServiceProviderSocket.accept();
                Thread t = new ActionsForUser(userConnection);
                t.start();

            }
        } catch (IOException ioException) {
            ioException.printStackTrace();
        } finally {
            try {
                userServiceProviderSocket.close();
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        }
    }
    
    public void createTopic(String topicName) {
        Topic topic = new Topic();
        topics.put(topicName, topic);
    }

    public void removeTopic(String topicName) {
        topics.remove(topicName);
    }

    public void pull(String topicName) { //update all subscribers of Topic("topicName")
        Topic topic = topics.get(topicName);
        for (UserNode usrNode : topic.getSubscribers()){
            int lastMsgIndex = connectedUsers.get(connectedUsers.indexOf(usrNode)).getMessageLists().get(topicName).size(); // ask the user for the index of the latest message in the user's msgQueue on said topic
            while (lastMsgIndex <= topic.getMessageQueue().size()){
                Value msg = topic.getMessageQueue().get(lastMsgIndex);//message to be delivered
                //send msg to user
                //ask for new messageListSize and repeat loop
            }
        }


    }

}
