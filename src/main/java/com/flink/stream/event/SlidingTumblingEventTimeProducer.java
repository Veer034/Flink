package com.flink.stream.event;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Random;

public class SlidingTumblingEventTimeProducer {
    public static void main(String[] args) throws IOException {
        // Creating a socket server for data producing in port
        ServerSocket listener = new ServerSocket(9090);
        try {
            //Waiting for lister to connect to the opened port
            Socket socket = listener.accept();
            System.out.println("Got new connection: " + socket.toString());
            try {
                /**
                 * generate data and write all data to opened port with sleep duration
                 */
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                Random rand = new Random();
                while (true) {
                    int i = rand.nextInt(100);
                    String s = "" + System.currentTimeMillis() + "," + i;
                    System.out.println(s);
                    out.println(s);
                    Thread.sleep(50);
                }

            } finally {
                socket.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            listener.close();
        }
    }
}

