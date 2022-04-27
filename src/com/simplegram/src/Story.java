package com.simplegram.src;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.ArrayList;

public class Story extends MultimediaFile implements Serializable {


    public Story(String sentFrom, String filename, int fileSize, ArrayList<byte[]> chunks) {
        super(sentFrom, filename, fileSize, chunks);
    }


    public Story(LocalDateTime dateSent, String sentFrom, String filename, int fileSize, ArrayList<byte[]> chunks) {
        super(dateSent, sentFrom, filename, fileSize, chunks);
    }

    public boolean hasExpired(){

        /**
         * If one day has passed since the creation
         * of the story then the story expires and
         * can no longer be pulled by others.
         *
         * In pseudocode:
         *
         *  IF dateNow > dateCreated + 1day THEN:
         *      DELETE;
         *  ELSE:
         *      KEEP;
         */

        return (this.getDateSent().plusSeconds(1)).compareTo(LocalDateTime.now()) < 0;
    }

    @Override
    public String toString() {
        return super.toString() + " | hasExpired: "+hasExpired();
    }
}
