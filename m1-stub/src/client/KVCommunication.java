package client;

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
        // TODO
        return null;
    }

    /**
	 * For receiving message from the KV server.
	 */
    public KVMessage receiveMessage() throws IOException {
        // TODO
        return null;
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

}
