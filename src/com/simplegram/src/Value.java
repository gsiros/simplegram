package com.simplegram.src;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.Date;

public class Value implements Serializable {

    private LocalDateTime dateSent;
    private String sentFrom;

    public Value(String sentFrom){
        this.sentFrom = sentFrom;
        this.dateSent = LocalDateTime.now();
    }

    public Value(LocalDateTime dateSent, String sentFrom) {
        this.sentFrom = sentFrom;
        this.dateSent = dateSent;
    }


    public LocalDateTime getDateSent() {
        return dateSent;
    }

    public String getSentFrom() {
        return sentFrom;
    }
}
