package com.flink.stream.state.prodevent;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Random;

public class StateProducer {

    public static void main(String args[]) throws IOException {
        // Creating a socket server for data producing in port
        ServerSocket listener = new ServerSocket(9090);
        //Waiting for lister to connect to the opened port
        Socket socket = listener.accept();
        System.out.println("Got new connection: " + socket.toString());
        try {
            /**
             * generate data  and write all data to opened port with sleep duration
             */
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            Random rand = new Random();
            for (int val = 0; val < 100; val++) {
                int ran = rand.nextInt(10);
                String str = (ran % 2) + 1 + "," + ran;
                System.out.println(str);
                out.println(str);
                Thread.sleep(50);
            }
            Thread.sleep(5000);
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            socket.close();
            listener.close();
        }
    }
}
