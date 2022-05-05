package com.simplegram.src;

import com.simplegram.src.ibc.BrokerConnection;
import com.simplegram.src.logging.TerminalColors;

import java.net.InetAddress;
import java.util.HashMap;

public class ConnectionChecker extends Thread {
    // Broker connections data structure.
    private HashMap<InetAddress, BrokerConnection> connections;

    public ConnectionChecker(HashMap<InetAddress, BrokerConnection> connections){
        this.connections = connections;
    }

    /**
     * This method is a daemon that periodically checks every second if an interval
     * of 5 seconds has passed since the last time that the broker has received a
     * 'ALIVE' message from its peers. If such an interval has passed and no other
     * 'ALIVE' message has been received in the meantime, the broker declares its
     * peer as dead.
     */
    @Override
    public void run() {
        try {
            while (true) {
                Thread.sleep(1000);
                System.out.println(TerminalColors.ANSI_BLUE+"Checking for inactive connections..."+TerminalColors.ANSI_RESET);
                synchronized (this.connections) {
                    long now = System.currentTimeMillis();
                    for (InetAddress ia : this.connections.keySet()) {
                        BrokerConnection bc = this.connections.get(ia);
                        if (bc.isActive() && now - bc.getLastTimeActive() > 5000) {
                            bc.setDead();
                            System.out.println(TerminalColors.ANSI_RED+"Broker " + ia.toString() + " is now dead!"+TerminalColors.ANSI_RESET);
                            /* TODO: Handle fault tolerance?*/
                        }

                    }
                }
            }
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}

