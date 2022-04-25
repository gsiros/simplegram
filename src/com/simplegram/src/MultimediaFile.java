package com.simplegram.src;

import java.io.Serializable;
import java.util.ArrayList;

public class MultimediaFile extends Value implements Serializable {


    private String filename;
    private int fileSize;
    private int chunkSize;
    private ArrayList<byte[]> chunks;

    public MultimediaFile(
            String sentFrom,
            String filename,
            int fileSize,
            ArrayList<byte[]> chunks
            ) {
        super(sentFrom);
        this.filename = filename;
        this.fileSize = fileSize;
        this.chunks = chunks;
        this.fileSize = fileSize;
    }

    public String getFilename() {
        return filename;
    }

    public int getFileSize() {
        return fileSize;
    }

    public ArrayList<byte[]> getChunks() {
        return chunks;
    }

    @Override
    public String toString() {
        return this.filename;
    }
}

