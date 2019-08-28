package com.flink.stream.state.prodevent;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class BroadcastProducer {
    public static void main(String[] args) throws IOException {
        // Creating a socket server for data producing in port
        ServerSocket listener = new ServerSocket(9090);
        //Waiting for lister to connect to the opened port
        Socket socket = listener.accept();
        // input path can be made dynamic by passing value in ParameterTool class as done in join & connector package
        String input = "//Users/ranvsing/CodeBase/Flink/input/salaryrange_fault";

        System.out.println("Got new connection: " + socket.toString());
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(input));
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            String line;
            while ((line = br.readLine()) != null) {
                out.println(line);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            listener.close();
            socket.close();
            if (br != null)
                br.close();
        }
    }
}
