package com.simplegram.src;

import com.simplegram.src.logging.TerminalColors;

import java.util.HashMap;

public class StoryChecker extends Thread {

    private HashMap<String, Topic> topics;

    public StoryChecker(HashMap<String, Topic> topics){
        this.topics = topics;
    }

    @Override
    public void run() {

        try{

            while(true){
                System.out.println(TerminalColors.ANSI_CYAN+"Cleaning expired stories..."+TerminalColors.ANSI_RESET);
                synchronized (this.topics) {
                    for(Topic topic : this.topics.values()){
                        topic.cleanStories();
                    }
                }
                Thread.sleep(1000);
            }

        } catch(Exception e) {

        }



    }
}