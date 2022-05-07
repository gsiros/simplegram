package com.simplegram.src;

public class DemoUserNode {
    public static void main(String[] args) {
        UserNode un = new UserNode("george");
        un.init("addrTERMINAL.txt");
        un.userStart();
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        un.pull();
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        un.sub("test");
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        un.pushMessage("test", "Hello World!");
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        un.unsub("test");
    }
}
