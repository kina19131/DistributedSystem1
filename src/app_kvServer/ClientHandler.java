package app_kvServer;

import java.net.Socket;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.IOException;
import java.util.logging.Logger;
import java.util.logging.Level;

public class ClientHandler implements Runnable {
    private Socket clientSocket;
    private KVServer server; 

    public ClientHandler(Socket socket) {
        this.clientSocket = socket;
        this.server = server; 
    }

    @Override
    public void run() {
        try (BufferedReader input = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
             PrintWriter output = new PrintWriter(clientSocket.getOutputStream(), true)) {

            String request = input.readLine();
            String[] tokens = request.split(" ", 3);
            String command = tokens[0];
            String key = tokens[1];
            String value = tokens.length == 3 ? tokens[2] : null;
            String response = "";

            if ("put".equals(command)) {
                server.putKV(key, value);
                response = "Put request processed";
            } else if ("get".equals(command)) {
                String result = server.getKV(key);
                response = result != null ? "Value: " + result : "Key not found";
            } else {
                response = "error: invalid command";
            }

            output.println(response);

        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Error in ClientHandler", e);
        } finally {
            try {
                clientSocket.close();
            } catch (IOException e) {
                LOGGER.log(Level.SEVERE, "Error closing client socket", e);
            }
        }
    }
}
