package com.simplegram.suggested;

public interface Consumer {

    void disconnect(String str);
    void register(String str); //maybe registers to topic?
    void showConversationData(String str, Value val);
}
