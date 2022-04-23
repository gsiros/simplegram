package com.simplegram.src;

import java.io.Serializable;
import java.util.Date;

public class Value implements Serializable {

    private Date dateSent;

    public Value(){
        this.dateSent = new Date();
    }

    public Date getDateSent() {
        return dateSent;
    }
}
