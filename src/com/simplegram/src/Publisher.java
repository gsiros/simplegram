package com.simplegram.src;

public interface Publisher {
    /*
    * push sends to the responsible for the topic
    * broker a value (message, multimedia file, etc)
    * */
    void push(String topicName, Value val);
}
