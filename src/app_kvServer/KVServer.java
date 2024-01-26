package app_kvServer;

import java.io.IOException;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

import java.util.Set;
import java.util.Collections;
import java.util.HashSet;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.LinkedList;

import java.util.logging.Logger;
import java.util.logging.Level;


public class KVServer implements IKVServer {
	/**
	 * Start KV Server at given port
	 * @param port given port for storage server to operate
	 * @param cacheSize specifies how many key-value pairs the server is allowed
	 *           to keep in-memory
	 * @param strategy specifies the cache replacement strategy in case the cache
	 *           is full and there is a GET- or PUT-request on a key that is
	 *           currently not contained in the cache. Options are "FIFO", "LRU",
	 *           and "LFU".
	 */

	private ServerSocket serverSocket;
	private boolean running = true; 

	private Set<ClientHandler> activeClientHandlers;

	private Map<String, String> storage;
    private Map<String, String> cache;
    private Queue<String> cacheQueue; // For FIFO caching
	private Map<String, Integer> accessFrequency; // For LFU caching 

    private int cacheSize;
	private IKVServer.CacheStrategy strategy; // Correct type for strategy

	private static final Logger LOGGER = Logger.getLogger(KVServer.class.getName());



	public KVServer(int port, int cacheSize, String strategy) {
        this.port = port;
        this.cacheSize = cacheSize;
        this.strategy = IKVServer.CacheStrategy.valueOf(strategy);

		this.activeClientHandlers = Collections.synchronizedSet(new HashSet<>());

        this.storage = new HashMap<>();

        if (cacheSize > 0) { // Initialize cache for all strategies if cacheSize > 0
            this.cache = new HashMap<>();
            if ("FIFO".equals(strategy)) {
                this.cacheQueue = new LinkedList<>();
            } else if ("LFU".equals(strategy)) {
                this.accessFrequency = new HashMap<>();
            }
        }


		try {
            this.serverSocket = new ServerSocket(port);
            LOGGER.info("Server started on port " + port);
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Error starting server", e);
        }
	}
	
	@Override
	public int getPort(){
		// TODO Auto-generated method stub

		if (serverSocket != null && !serverSocket.isClosed()){
			return serverSocket.getLocalPort();
		}
		return -1; 
	}

	@Override
	public String getHostname() {
		if (serverSocket != null && !serverSocket.isClosed()) {
			return serverSocket.getInetAddress().getHostName();
    	} 
		else {
        	return "Server not initialized or closed";
    	}
	}


	@Override
    public CacheStrategy getCacheStrategy(){
        if ("FIFO".equals(this.strategy)) {
            return IKVServer.CacheStrategy.FIFO;
        } else if ("LRU".equals(this.strategy)) {
            return IKVServer.CacheStrategy.LRU;
        } else if ("LFU".equals(this.strategy)) {
            return IKVServer.CacheStrategy.LFU;
        }
        return IKVServer.CacheStrategy.None;
    }

	@Override
    public int getCacheSize(){
		// TODO Auto-generated method stub
		return this.cacheSize;
	}

	@Override
    public boolean inStorage(String key){
		// TODO Auto-generated method stub
		return storage.containsKey(key);;
	}

	@Override
    public boolean inCache(String key){
		// TODO Auto-generated method stub
		return cache != null && cache.containsKey(key);
	}


	@Override
    public String getKV(String key) throws Exception {
        String value;
        if (inCache(key)) {
            value = cache.get(key);
            LOGGER.fine("Cache hit for key: " + key);
        } else {
            value = storage.get(key);
            LOGGER.fine("Cache miss for key: " + key);
            if (value != null && cache != null) {
                updateCache(key, value);
            }
        }
        return value;
    }


	@Override
    public void putKV(String key, String value) throws Exception{
		// TODO Auto-generated method stub
		storage.put(key, value); 
		if (cache != null) {
			updateCache(key, value);  // Update cache, depending on your cache strategy
		}
	}


	// UPDATING CACHE 
	private void updateCache(String key, String value) {
		if (cache == null) {
			return;
		}
	
		switch (getCacheStrategy()) {
			case FIFO:
				updateCacheFIFO(key, value);
				break;
			case LRU:
				updateCacheLRU(key, value);
				break;
			case LFU:
				updateCacheLFU(key, value);
				break;
			case None:
				// No caching
				break;
		}
	}

	private void updateCacheFIFO(String key, String value) {
        if (!cache.containsKey(key) && cacheQueue.size() >= cacheSize) {
            String oldestKey = cacheQueue.poll();
            cache.remove(oldestKey);
        }
        cache.put(key, value);
        cacheQueue.add(key);
    }

    private void updateCacheLRU(String key, String value) {
        // Remove key to re-insert and maintain order
        if (cache.containsKey(key)) {
            cache.remove(key);
        } 
		else if (cache.size() >= cacheSize) {
            String oldestKey = cache.keySet().iterator().next();
            cache.remove(oldestKey);
        }
        cache.put(key, value);
    }

	private void updateCacheLFU(String key, String value) {
		if (cache.containsKey(key)) {
			cache.put(key, value);
			accessFrequency.put(key, accessFrequency.get(key) + 1);
		} 
		else {
			if (cache.size() >= cacheSize) {
				String leastFrequentKey = Collections.min(accessFrequency.entrySet(), Map.Entry.comparingByValue()).getKey();
				cache.remove(leastFrequentKey);
				accessFrequency.remove(leastFrequentKey);
			}
			cache.put(key, value);
			accessFrequency.put(key, 1);
		}
	}
	

	@Override
	public void clearCache() {
		if (cache != null) {
			cache.clear();
		}
		if (cacheQueue != null) { // For FIFO
			cacheQueue.clear();
		}
		if (accessFrequency != null) { // For LFU
			accessFrequency.clear();
		}
		LOGGER.info("Cache cleared");
	}

	@Override
    public void clearStorage(){
		// TODO Auto-generated method stub
		storage.clear();
    	LOGGER.info("Storage cleared");
	}

	@Override
    public void run(){
		// TODO Auto-generated method stub
		while(running){
			try{
				Socket clientSocket = serverSocket.accept(); 
				ClientHandler clientHandler = new ClientHandler(clientSocket, this);
				activeClientHandlers.add(clientHandler);
				clientHandler.start();
			}
			catch (IOException e){
				LOGGER.log(Level.SEVERE, "Error accepting client connection", e);
			}
		}
	}


	@Override
    public void close(){
		// TODO Auto-generated method stub
		// CLOSE CONNECTION WITH THE CLIENT?
		this.running = false; 
		// Close all active client connections
		// For this, you need to keep track of all active client threads
		// Assuming you have a collection 'activeClientHandlers' to track these
		for (ClientHandler clientHandler : activeClientHandlers) {
			clientHandler.closeConnection(); // closeConnection() needs to be implemented in ClientHandler
		}

		// Close the server socket
		try {
			if (serverSocket != null && !serverSocket.isClosed()) {
				serverSocket.close();
				LOGGER.info("Server socket closed");
			}
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, "Error closing server socket", e);
		}

		// Perform any additional cleanup if necessary
		// For example, saving state to disk, closing database connections, etc.
	}

	@Override
    public void kill(){
		// TODO Auto-generated method stub
		// STOP THE SERVER? 
		this.running = false; 
		try{
			if(serverSocket != null && !serverSocket.isClosed()){
				serverSocket.close(); 
				LOGGER.info("Server Socket Closed")
			}
		}
		catch (IOException e){
			LOGGER.log(Level.SEVERE, "Error closing server socket", e); // 봐봐
		}
	}
}
