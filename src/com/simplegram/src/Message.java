package com.simplegram.src;

import java.io.Serializable;

public class Message extends Value implements Serializable {
    private String msg;

    public Message(String sentFrom, String msg) {
        super(sentFrom);
        this.msg = msg;

    }

    public String getMsg() {
        return msg;
    }

    @Override
    public String toString() {
        return this.msg;
    }
}
