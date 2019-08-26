package com.flink.stream.event;

import java.util.*;
import java.io.*;
import java.net.*;

public class SessionWindowEventTimeProducer {
    public static void main(String[] args) throws IOException{
        // Creating a socket server for data producing in port
        ServerSocket listener = new ServerSocket(9090);
        try{
            //Waiting for lister to connect to the opened port
            Socket socket = listener.accept();
            System.out.println("Got new connection: " + socket.toString());
            try {
                /**
                 * generate data  and write all data to opened port with sleep duration
                 */
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                Random rand = new Random();
                int count = 0;
                while (true){
                    count++;
                    int i = rand.nextInt(100);
                    String s = "" + System.currentTimeMillis() + "," + i;
                    System.out.println(s);
                    out.println(s);
                    // creating 10 data in one session
                    if (count >= 10){
                        System.out.println("*********************");
                        //higher sleep time for breaking the session into many parts
                        Thread.sleep(1000);
                        count = 0;
                    }else
                        Thread.sleep(50);
                }
            } finally{
                socket.close();
            }
        } catch(Exception e ){
            e.printStackTrace();
        } finally{
            listener.close();
        }
    }
}
