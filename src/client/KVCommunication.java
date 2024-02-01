package client; 

import java.io.PrintWriter;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;

import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;
import shared.messages.SimpleKVMessage;


public class KVCommunication implements Runnable {
    
    private Logger logger = Logger.getRootLogger();
    private boolean running;
    
    private Socket clientSocket;
    private OutputStream output;
    private InputStream input;
    
    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 1024 * BUFFER_SIZE;

    /**
     * Initialize KVCommunication with address and port of KVServer
     * @param serverAddress the address of the KVServer
     * @param serverPort the port of the KVServer
     */
    public KVCommunication(String serverAddress, int serverPort) throws UnknownHostException, Exception {
        clientSocket = new Socket(serverAddress, serverPort);
        // clientSocket.setSoTimeout(1000);
        setRunning(true);
        logger.info("Connection established.");
    }

    /**
     * Initializes and starts the client connection. 
     * Loops until the connection is closed or aborted by the client.
     */
    public void run() {
        try {
            output = clientSocket.getOutputStream();
            input = clientSocket.getInputStream();

            while(isRunning()) {
                try {
                    KVMessage latestMsg = receiveMessage();
                    logger.info("Received message: " + latestMsg);
                } catch (IOException ioe) {
                    if(isRunning()) {
                        logger.error("Connection lost!");
                        try {
                            tearDownConnection();
                        } catch (IOException e) {
                            logger.error("Unable to close connection!");
                        }
                    }
                }               
            }
        } catch (IOException ioe) {
            logger.error("Connection could not be established!");
        } finally {
            if(isRunning()) {
                closeConnection();
            }
        }
    }

    /**
     * For sending message to the KV server.
     */
    public KVMessage sendMessage(StatusType status, String key, String value) throws IOException {
        // Ensure the output stream is set up
        if (output == null) {
            throw new IOException("Output stream not initialized");
        }

        // Convert the message to a string format based on your protocol
        String messageToSend = formatMessage(status, key, value);
        
        // Send the message
        sendFormattedMessage(messageToSend);

        System.out.println("Sent message: " + messageToSend); // MODIFIED: Added logging for sent messages
        return receiveMessage(); 

        // // Receive the response from the server and parse it
        // String response = receiveFormattedMessage();
        // return parseMessage(response);
    }

    /**
     * For receiving message from the KV server.
     */
    public KVMessage receiveMessage() throws IOException {
        // Ensure the input stream is set up
        if (input == null) {
            throw new IOException("Input stream not initialized");
        }
    
        // Read the response and parse it
        String response = receiveFormattedMessage();
        System.out.println("Received raw message: " + response); // MODIFIED: Added logging for raw received message
        return parseMessage(response);
    }
    
    

    // Helper method to format the message to be sent
    private String formatMessage(StatusType status, String key, String value) {
        return status.name() + " " + key + " " + (value != null ? value : "") + "\n";
    }


    // Helper method to send the formatted message
    private void sendFormattedMessage(String message) throws IOException {
        PrintWriter printWriter = new PrintWriter(output, true);
        printWriter.println(message);
        // System.out.println("Sent message: " + message); // MODIFIED: Log the message being sent
    }

    // Helper method to receive the formatted message
    private String receiveFormattedMessage() throws IOException {
        System.out.println("Waiting for server response...");
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        String response = reader.readLine();
        System.out.println("Raw response received: " + response); // Debugging the raw response
        return response;
    }



    public void closeConnection() {
        logger.info("try to close connection ...");
        
        try {
            tearDownConnection();
        } catch (IOException ioe) {
            logger.error("Unable to close connection!");
        }
    }
    
    private void tearDownConnection() throws IOException {
        setRunning(false);
        logger.info("tearing down the connection ...");
        if (clientSocket != null) {
            input.close();
            output.close();
            clientSocket.close();
            clientSocket = null;
            logger.info("connection closed!");
        }
    }

    public boolean isRunning() {
        return running;
    }
    
    public void setRunning(boolean run) {
        running = run;
    }

    // private KVMessage parseMessage(String response) {
    //     try {
    //         String[] parts = response.split(" ", 3);
    //         StatusType statusType = StatusType.valueOf(parts[0]);
    //         String key = parts[1];
    //         String value = parts.length > 2 ? parts[2] : null;
            
    //         // System.out.println("Parsed message - Status: " + statusType + ", Key: " + key + ", Value: " + value); // MODIFIED: Log parsed message details
    //         return new SimpleKVMessage(statusType, key, value);
    //     } catch (Exception e) {
    //         // System.out.println("Error parsing message: " + response); // MODIFIED: Log parsing errors
    //         e.printStackTrace();
    //         return null;
    //     }
    // }

    private KVMessage parseMessage(String response) {
        if (response == null || response.isEmpty()) {
            logger.error("Received empty or null response");
            return null;
        }
        try {
            String[] parts = response.split(" ", 3);
            StatusType statusType = StatusType.valueOf(parts[0]);
            String key = parts.length > 1 ? parts[1] : "";
            String value = parts.length > 2 ? parts[2] : "";
            System.out.println("Parsed message - Status: " + statusType + ", Key: " + key + ", Value: " + value);
            return new SimpleKVMessage(statusType, key, value);
        } catch (Exception e) {
            System.out.println("Error parsing message: " + response + ". Exception: " + e.getMessage()); // Instead of logger.error
            return null;
        }
    }

    public void connect() throws IOException {
        output = clientSocket.getOutputStream();
        input = clientSocket.getInputStream();
    }
}
