package client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;

import client.KVCommunication;

import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;

public class KVStore implements KVCommInterface {

	private Logger logger = Logger.getRootLogger();
	private boolean running;
	
	private String serverAddress;
	private int serverPort;

	private KVCommunication kvComm;

	/**
	 * Initialize KVStore with address and port of KVServer
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */
	public KVStore(String address, int port) {
		serverAddress = address;
		serverPort = port;
		logger.info("KVStore initialized.");
	}

	@Override
	public void connect()  throws UnknownHostException, Exception {
		new KVCommunication(serverAddress, serverPort);
		setRunning(true);		
	}

	@Override
	public void disconnect() {
		if (isRunning()) {
			kvComm.closeConnection();
			setRunning(false);
		}
	}

	@Override
	public KVMessage put(String key, String value) throws Exception {
		kvComm.sendMessage(StatusType.PUT, key, value);
		return null;
	}

	@Override
	public KVMessage get(String key) throws Exception {
		kvComm.sendMessage(StatusType.GET, key, null);
		return null;
	}

	public void setRunning(boolean run) {
		running = run;
	}

	public boolean isRunning() {
		return running;
	}

}
