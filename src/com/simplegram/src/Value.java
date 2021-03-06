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

    /**
     * This method is used to get the time and date of the value.
     * @return the time and date when the value was created.
     */
    public LocalDateTime getDateSent() {
        return dateSent;
    }

    /**
     * This method is used to get user that pushed the value to the topic.
     * @return the user that pushed the value.
     */
    public String getSentFrom() {
        return sentFrom;
    }
}
