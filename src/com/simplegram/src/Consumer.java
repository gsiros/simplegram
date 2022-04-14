package com.simplegram.src;

public interface Consumer {
    // Consumer has pull.
    /*
    *  pull will internally update the target
    *  topic value list with the latest changes.
    *
    * */
    void pull(String topicName);
}
