package com.simplegram.src;

import java.io.Serializable;

public class MultimediaFile implements Serializable {
    private String destinationDirectory;
    private String sourceDirectory;
    private String filename;
    private long fileSize;
    private byte[] fileData;
    private String status;

    public MultimediaFile() {

    }
}

