package com.simplegram.src;

import com.simplegram.src.ibc.BrokerConnection;
import com.simplegram.src.logging.TerminalColors;

import java.net.InetAddress;
import java.util.HashMap;

public class ConnectionChecker extends Thread {

    private HashMap<InetAddress, BrokerConnection> connections;

    public ConnectionChecker(HashMap<InetAddress, BrokerConnection> connections){
        this.connections = connections;
    }

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
                            /* TODO: Handle fault tolerance.*/
                            /* Step 1: define the new broker responsible
                                     for the sleeper.
                                THIS WILL BE DONE BY CONVENTION THAT IS
                                NOT COMMUNICATION-SPECIFIC (by id maybe?)
                            */
                            //if (bc.getBrokerAddress())

                        }

                    }
                }
            }
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}

