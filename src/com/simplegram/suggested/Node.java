package com.simplegram.suggested;

import java.util.List;

public interface Node {
    List<Broker> brokers = null;

    void connect();
    void disconnect();
    void init(int brokerNum);
    void updateNodes();
}
