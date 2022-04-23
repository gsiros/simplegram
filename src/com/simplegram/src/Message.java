package com.simplegram.src;

import java.io.Serializable;

public class Message extends Value implements Serializable {
    private String msg;

    public Message(String msg) {
        super();
        this.msg = msg;

    }

    public String getMsg() {
        return msg;
    }


}
