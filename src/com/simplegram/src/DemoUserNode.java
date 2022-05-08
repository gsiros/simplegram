package com.simplegram.src;

import java.util.Scanner;

public class DemoUserNode {
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        System.out.print("Username: ");
        String username = sc.nextLine();
        UserNode un = new UserNode(username);
        System.out.println("Starting UserNode...");
        un.init("addrTERMINAL.txt");
        un.userStart();
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        un.pull();

        while(true){
            int actionID;
            System.out.println("Select action to perform:\n1. SUB\n2. UNSUB\n3. PUSH\n4. SHOW UNREADS");
            actionID = sc.nextInt();
            switch(actionID){
                case 1: // SUB
                    System.out.println("Topic to subscribe to: ");
                    sc.nextLine();
                    String topictosubto = sc.nextLine();
                    un.sub(topictosubto);
                    break;
                case 2:
                    System.out.print("Topic to unsubscribe from: ");
                    sc.nextLine();
                    un.unsub(sc.nextLine());
                    break;
                case 3:
                    System.out.print("Topic to push to: ");
                    sc.nextLine();
                    String topic = sc.nextLine();
                    System.out.println("Type of value to push:\n1. MSG\n2. MULTIF\n3. STORY");
                    int typeID = sc.nextInt();
                    switch(typeID){
                        case 1:
                            System.out.println("Text to push:");
                            sc.nextLine();
                            String text = sc.nextLine();
                            un.pushMessage(topic,text);
                            break;
                        case 2:
                            System.out.println("Path of multimedia file to push:");
                            sc.nextLine();
                            String path = sc.nextLine();
                            un.pushMultimediaFile(topic, path);
                            break;
                        case 3:
                            System.out.println("Path of story file to push:");
                            sc.nextLine();
                            String storypath = sc.nextLine();
                            un.pushStory(topic, storypath);
                            break;
                    }
                    break;
                case 4:
                    System.out.println("Displaying unread values from all subscribed topics for user "+username+":");
                    un.printUnreads();
                    break;
                default:
                    System.out.println("Invalid action, try again.");
                    break;
            }
        }

    }
}
