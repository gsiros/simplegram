package com.simplegram.src;

import java.io.Serializable;
import java.util.Date;

public class Value implements Serializable {

    private Date dateSent;
    private String sentFrom;

    public Value(String sentFrom){
        this.sentFrom = sentFrom;
        this.dateSent = new Date();
    }

    public Date getDateSent() {
        return dateSent;
    }

    public String getSentFrom() {
        return sentFrom;
    }
}
