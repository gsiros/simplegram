package com.simplegram.src.ibc;

import com.simplegram.src.logging.TerminalColors;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.HashMap;

/**
 * This class handles the 'ALIVE' messages from other brokers.
 */
public class ReceiveHandler extends Thread {

    private DatagramSocket socket;
    private HashMap<InetAddress, BrokerConnection> connections;

    public ReceiveHandler(DatagramSocket socket, HashMap<InetAddress, BrokerConnection> connections){
        this.socket = socket;
        this.connections = connections;
    }

    @Override
    public void run() {

        while(true){

            // Prepare the byte array for the incoming segment.
            byte[] rcv_msg = new byte[512];
            // Prepare the data structure for the segment.
            DatagramPacket rcv_dp = new DatagramPacket(
                    rcv_msg,
                    rcv_msg.length
            );

            try {
                // Listen for incoming segment.
                socket.receive(rcv_dp);
                // Unpack segment.
                String data = new String(rcv_dp.getData());
                System.out.println("'"+data+"' message received from "+rcv_dp.getAddress().toString());

                synchronized (connections) {
                    connections.get(rcv_dp.getAddress()).setActive();
                    System.out.println(TerminalColors.ANSI_GREEN+"Broker "+rcv_dp.getAddress().toString()+" is active!"+TerminalColors.ANSI_RESET);
                }


            } catch (SocketException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }


    }
}

