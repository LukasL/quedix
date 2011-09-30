package org.test;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

public class PortTester {

    public static void main(String[] args) {
        int i = 1980;
        while(i < 2000) {
            try {
                Socket s = new Socket("localhost", i);
                System.out.println("Port is used: " + i);
                s.close();
            } catch (UnknownHostException e) {
                e.printStackTrace();
            } catch (IOException e) {
            }
            i++;
        }

    }

}
