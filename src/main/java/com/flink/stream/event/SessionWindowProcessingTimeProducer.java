package com.flink.stream.event;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class SessionWindowProcessingTimeProducer {
    public static void main(String[] args) throws IOException {
        // Creating a socket server for data producing in port
        ServerSocket listener = new ServerSocket(9090);
        try {
            //Waiting for lister to connect to the opened port
            Socket socket = listener.accept();
            System.out.println("Got new connection: " + socket.toString());
            // input path can be made dynamic by passing value in ParameterTool class as done in join & connector package
            String input = "//Users/ranvsing/CodeBase/Flink/input/salaryrange";
            /**
             * Read data from the file and write all data to opened port with sleep duration
             */
            BufferedReader br = new BufferedReader(new FileReader(input));
            try {
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                String line;
                int count = 0;
                while ((line = br.readLine()) != null) {
                    System.out.println(line);
                    count++;
                    out.println(line);
                    // creating 10 data in one session
                    if (count >= 10) {
                        count = 0;
                        //higher sleep time for breaking the session into many parts
                        Thread.sleep(1000);
                    } else
                        Thread.sleep(50);
                }
                //Sleep thread as data might get lost in consumer side if not consumed in time.
                Thread.sleep(5000);
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

