package com.simplegram.src;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;

public class Broker {

    ArrayList<UserNode> connectedUsers;
    HashMap<String, Topic> topics;
    //HashMap<String, ArrayList<Value>> messageQueue; //messages to be sent <topicname, message>
    //TODO:list of other brokers

    ServerSocket userServiceProviderSocket;
    ServerSocket brokerServiceProviderSocket;

    Socket brokerConnection;
    Socket userConnection;

    // init broker to become ready for
    // new connections with usernodes
    public Broker(){

    }

    void init() {

    }
    public void pull(String topicName) { //update all subscribers of Topic("topicName")
        Topic topic = topics.get(topicName);
        for (int i = 0; i< topic.users.size(); i++){
            int messageListSize ; // ask the user for the index of the latest message in the user's msgQueue on said topic
            while (messageListSize < topic.values.size()){
                Value msg = topic.values.get(messageListSize);//message to be delivered
                //send msg to user
                //ask for new messageListSize and repeat loop
            }
        }


    }

    // begin the main functionality,
    // start accepting connections.
    void start(){
        try {
            userServiceProviderSocket = new ServerSocket(4444);
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

}
