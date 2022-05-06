package com.simplegram.src.ibc;

import java.io.IOException;
import java.net.*;

/**
 * This class sends 'ALIVE' packets to a broker.
 */
public class SendHandler extends Thread {

    private DatagramSocket socket;
    private InetAddress rcver_addr;

    public SendHandler(DatagramSocket socket, InetAddress rcver_addr){
        this.socket = socket;
        this.rcver_addr = rcver_addr;
    }

    @Override
    public void run() {
        while(true) {

            final String msg = "ALIVE";
            byte[] msg_bytes = msg.getBytes();

            DatagramPacket dp = new DatagramPacket(
                    msg_bytes,
                    msg_bytes.length,
                    rcver_addr,
                    4444
            );

            System.out.println("Sending ALIVE msg...");
            try {
                socket.send(dp);
            } catch (SocketException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}

