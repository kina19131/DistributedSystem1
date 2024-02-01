package app_kvServer;

import shared.messages.SimpleKVMessage;
import shared.messages.KVMessage.StatusType;

import java.net.Socket;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.IOException;
import java.util.logging.Logger;
import java.util.logging.Level;

import shared.messages.KVMessage;


public class ClientHandler implements Runnable {
    private Socket clientSocket;
    private KVServer server; 

    private static final Logger LOGGER = Logger.getLogger(ClientHandler.class.getName());

    public ClientHandler(Socket socket, KVServer server) {
        this.clientSocket = socket;
        this.server = server; 
    }

    @Override
    public void run() {
        try (BufferedReader input = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            PrintWriter output = new PrintWriter(clientSocket.getOutputStream(), true)) {

            String requestString;
            while ((requestString = input.readLine()) != null) {
                SimpleKVMessage responseMessage;
                
                if (!requestString.isEmpty()){
                    SimpleKVMessage requestMessage = parseRequest(requestString); 
                    
                    switch(requestMessage.getStatus()){
                        case PUT:
                            try {
                                boolean isUpdate = server.inCache(requestMessage.getKey());
                                server.putKV(requestMessage.getKey(), requestMessage.getValue());
                                StatusType responseType = isUpdate ? StatusType.PUT_UPDATE : StatusType.PUT_SUCCESS;
                                responseMessage = new SimpleKVMessage(responseType, requestMessage.getKey(), null);
                            } catch (Exception e) {
                                LOGGER.log(Level.SEVERE, "Error processing put request", e);
                                responseMessage = new SimpleKVMessage(StatusType.PUT_ERROR, null, null);
                            }
                            LOGGER.info("Processed PUT request for key: " + requestMessage.getKey());
                            break;
                        case GET: 
                            try {
                                String response = server.getKV(requestMessage.getKey());
                                StatusType responseType = (response != null) ? StatusType.GET_SUCCESS : StatusType.GET_ERROR;
                                responseMessage = new SimpleKVMessage(responseType, requestMessage.getKey(), response);
                                LOGGER.info("Processed GET request for key: " + requestMessage.getKey() + " with value: " + response);
                            } catch (Exception e) {
                                LOGGER.log(Level.SEVERE, "Error processing get request", e);
                                responseMessage = new SimpleKVMessage(StatusType.GET_ERROR, null, null);
                            }
                            break;
                        default: 
                            responseMessage = new SimpleKVMessage(StatusType.PUT_ERROR, null, null); 
                    }
                    String responseString = formatResponse(responseMessage);
                    LOGGER.info("responseString: "+ responseString);
                    output.println(responseString);
                    output.flush();
                    LOGGER.info("Response sent to client"); // Log the response sent
                }

                if (clientSocket.isClosed()) {
                    LOGGER.info("... Client has closed the connection ...");
                    break;
                }
                // LOGGER.info("... Waiting the Client...\n");
                // break;
            }
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

    private SimpleKVMessage parseRequest(String requestString) {
        LOGGER.info("Received request string: " + requestString);
        if (requestString == null || requestString.trim().isEmpty()) {
            LOGGER.warning("Empty or null request string received");
            return new SimpleKVMessage(StatusType.PUT_ERROR, null, null);
        }
        String[] parts = requestString.split(" ", 3);
        StatusType status;
        try {
            status = StatusType.valueOf(parts[0]);
            LOGGER.info("Parsed status: " + status);
        } catch (IllegalArgumentException e) {
            LOGGER.warning("Invalid Status:" + parts[0]);
            return new SimpleKVMessage(StatusType.PUT_ERROR, null, null);
        }
        String key = parts.length > 1 ? parts[1] : null;
        String value = parts.length > 2 ? parts[2] : "";
        LOGGER.info("Extracted key: " + key + ", value: " + value);
        return new SimpleKVMessage(status, key, value);
    }
       
    private String formatResponse(SimpleKVMessage message) {
        String status = message.getStatus().name();
        // LOGGER.info("FORMAT RESPONSE: " + status);
        // LOGGER.info("FORMAT RESPONSE Value: " + message.getValue());
        String key = (message.getKey() != null) ? message.getKey() : "";
        String value = (message.getValue() != null) ? message.getValue() : "";
        return status + " " + key + " " + value + "\r\n";
    }
    
}
