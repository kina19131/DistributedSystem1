// package app_kvServer;

// import java.io.BufferedWriter;
// import java.io.FileWriter;
// import java.io.BufferedReader;
// import java.io.FileReader;
// import java.io.File;

// import java.util.ArrayList;
// import java.util.List;

// import java.io.IOException;
// import java.net.InetAddress;
// import java.net.UnknownHostException;
// import java.net.ServerSocket;
// import java.net.Socket;
// import java.util.Collections;
// import java.util.HashMap;
// import java.util.HashSet;
// import java.util.LinkedList;
// import java.util.Map;
// import java.util.Queue;
// import java.util.Set;
// import java.util.logging.Level;
// import java.util.logging.Logger;
// import java.util.Map.Entry;
// import java.net.BindException;

package app_kvServer;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;


import app_kvServer.ClientHandler;


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
	private int port;
	private boolean running;
	private Set<ClientHandler> activeClientHandlers;
	private List<Thread> clientHandlerThreads;
	private Map<String, String> storage;
	private Map<String, String> cache;
	private Queue<String> fifoQueue;
	private Map<String, Integer> accessFrequency;
	private LinkedHashMap<String, String> lruCache;
	private PriorityBlockingQueue<String> lfuQueue;
	private int cacheSize;
	private IKVServer.CacheStrategy strategy;
	private static final Logger LOGGER = Logger.getLogger(KVServer.class.getName());

	public KVServer(int port, int cacheSize, String strategy) {
		this.port = port;
		this.cacheSize = cacheSize;
		this.strategy = IKVServer.CacheStrategy.valueOf(strategy.toUpperCase());

		this.activeClientHandlers = Collections.synchronizedSet(new HashSet<ClientHandler>());
		this.clientHandlerThreads = new ArrayList<Thread>();
		this.storage = new HashMap<String, String>();

		if (IKVServer.CacheStrategy.FIFO.equals(this.strategy)) {
			this.fifoQueue = new LinkedList<String>();
		} else if (IKVServer.CacheStrategy.LFU.equals(this.strategy)) {
			this.accessFrequency = new HashMap<String, Integer>();
			this.lfuQueue = new PriorityBlockingQueue<String>(cacheSize, new Comparator<String>() {
				public int compare(String key1, String key2) {
					int freqCompare = Integer.compare(accessFrequency.get(key1), accessFrequency.get(key2));
					return freqCompare != 0 ? freqCompare : key1.compareTo(key2);
				}
			});
		} else if (IKVServer.CacheStrategy.LRU.equals(this.strategy)) {
			initLRUCache();
		}

		if (cacheSize > 0) {
			this.cache = new HashMap<String, String>();
		}
	}

	private void initLRUCache() {
		this.lruCache = new LinkedHashMap<String, String>(cacheSize, 0.75F, true) {
			protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
				return size() > KVServer.this.cacheSize;
			}
		};
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
    try {
        return InetAddress.getLocalHost().getHostAddress();
    	} 
	catch (UnknownHostException e) {
        LOGGER.log(Level.SEVERE, "Error getting host IP address", e);
        return null;
    	}
	}

	@Override 
	public CacheStrategy getCacheStrategy() {
		LOGGER.info("getCacheStrategy: " + this.strategy);
		try {
			return this.strategy;
		} catch (IllegalArgumentException | NullPointerException e) {
			LOGGER.warning("Invalid or null cache strategy: " + this.strategy);
			return IKVServer.CacheStrategy.None;
		}
	}

	@Override
    public int getCacheSize(){
		// TODO Auto-generated method stub
		return this.cacheSize;
	}

	@Override
    public boolean inStorage(String key){
		// TODO Auto-generated method stub
		return storage.containsKey(key);
	}

	@Override
    public boolean inCache(String key){
		// TODO Auto-generated method stub
		return cache != null && cache.containsKey(key);
	}


	@Override
	public String getKV(String key) throws Exception {
		LOGGER.info("GETKV PROCESSING");
		String value = null; // Initialize value to null
		
		if (cache != null && inCache(key)) {
			value = cache.get(key);
			LOGGER.fine("Cache hit for key: " + key);
		} 

		if (value == null && inStorage(key)){
			value = storage.get(key);
			LOGGER.fine("Storage hit for key: " + key);
		}
		return value;
	}



	@Override
    public void putKV(String key, String value) throws Exception{
		// TODO Auto-generated method stub
		// LOGGER.info("Attempting to put key: " + key + ", value: " + value);
		try{
			if ("null".equals(value)) {
				if (storage.containsKey(key)) {
					storage.remove(key);
					LOGGER.info("Key removed from storage: " + key);
				}
				if (cache != null && cache.containsKey(key)) {
					cache.remove(key);
					if (strategy == CacheStrategy.LFU && accessFrequency.containsKey(key)) {
						accessFrequency.remove(key);
						lfuQueue.remove(key);
					}
					LOGGER.info("Key removed from cache: " + key);
				}
				return; 
			}

			storage.put(key, value); // if key already exists, get new val, will be updated 
									// if key not available, will be put in. 
			LOGGER.info("Storage updated for key: " + key);
			if (cache != null) {
				updateCache(key, value);  
				LOGGER.info("Cache updated for key: " + key);
			}
			saveDataToStorage(); 
		} catch (Exception e){
			LOGGER.severe("Error while putting key: " + key+ " with value: "+ value); 
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
			throw e; 
		}
	}


	// UPDATING CACHE 
	private void updateCache(String key, String value) {
		LOGGER.info("UPDATING CACHE"); 	
		switch (getCacheStrategy()) {
			case FIFO:
				LOGGER.info("Update FIFO: Put Key: " + key + " with value:" + value); 	
				updateCacheFIFO(key, value);
				break;
			case LRU:
				if (lruCache != null) {
					updateCacheLRU(key, value);
				}
				LOGGER.info("LRU: Put Key: " + key + " with value:" + value); 	
				break;
			case LFU:
				updateCacheLFU(key, value);
				LOGGER.info("LFU: Put Key: " + key + " with value:" + value); 	
				break;
			case None:
				// No caching
				break;
		}
	}

	// FIFO: The oldest item is evicted when the cache is full.
	// LRU: The least recently used item is evicted. Your implementation keeps the most recently used items at the end of the cache map.
	// LFU: The least frequently used item is evicted. You use an accessFrequency map to track the access frequency of each key.
	// FIFO Update Cache Method
	private void updateCacheFIFO(String key, String value) {
		if (fifoQueue.size() >= cacheSize && !cache.containsKey(key)) {
			String oldestKey = fifoQueue.poll();
			cache.remove(oldestKey);
		}
		if (!cache.containsKey(key)) {
			fifoQueue.offer(key);
		} else {
			// Handle the key already existing in the cache, if necessary
		}
		cache.put(key, value);
	}

	// LRU Update Cache Method
	private void updateCacheLRU(String key, String value) {
		// Directly interact with lruCache, which is a LinkedHashMap
		lruCache.put(key, value);
	}

	// LFU Update Cache Method
	private void updateCacheLFU(String key, String value) {
		if (cache.size() >= cacheSize && !cache.containsKey(key)) {
			String leastUsedKey = lfuQueue.poll();
			if (leastUsedKey != null) {
				cache.remove(leastUsedKey);
				accessFrequency.remove(leastUsedKey);
			}
		}
		cache.put(key, value);
		accessFrequency.put(key, accessFrequency.getOrDefault(key, 0) + 1);
		if (!lfuQueue.contains(key)) {
			lfuQueue.offer(key);
		}
	}

	
	private String findLeastFrequentKeyLFU() {
        String leastFrequentKey = null;
        int minFreq = Integer.MAX_VALUE;
        for (Map.Entry<String, Integer> entry : accessFrequency.entrySet()) {
            if (entry.getValue() < minFreq) {
                minFreq = entry.getValue();
                leastFrequentKey = entry.getKey();
            }
        }
        return leastFrequentKey;
    }

	@Override
	public void clearCache() {
		if (cache != null) {
			cache.clear();
		}
		if (fifoQueue != null) { // For FIFO
			fifoQueue.clear();
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

	private boolean isRunning() {
        return this.running;
    }


	@Override
	public void run() {
		running = initializeServer();
		if (!initializeServer()) {
            LOGGER.severe("Server initialization failed. Server is not running.");
            return; // Stop the server from running if initialization fails
        }
		if (serverSocket != null) {
			LOGGER.info("KV Server listening on port " + getPort());

			loadDataFromStorage(); // Load data from the file into the storage map if the file exists

			while (isRunning()) {
				try {
					Socket clientSocket = serverSocket.accept();
					LOGGER.info("Connected to client: " + clientSocket.getInetAddress());
					ClientHandler handler = new ClientHandler(clientSocket, this);
					Thread handlerThread = new Thread(handler);
					clientHandlerThreads.add(handlerThread); 
					handlerThread.start();
				} catch (IOException e) {
					if (!running) {
						LOGGER.info("Server is stopping.");
					} else {
						LOGGER.log(Level.SEVERE, "Error accepting client connection", e);
					}
				}
			}
			saveDataToStorage();
		} else {
			LOGGER.severe("Server socket is null.");
		}
	}

	private void loadDataFromStorage() {
		String filePath = "kvstorage.txt"; // Relative path to the file
		File file = new File(filePath);
	
		try {
			if (!file.exists()) {
				// If the file doesn't exist, create an empty file
				boolean created = file.createNewFile();
				if (created) {
					LOGGER.info("Created new " + filePath + " file");
				} else {
					LOGGER.warning("Failed to create " + filePath + " file");
				}
			}
	
			// Now you can open the file for reading
			try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
				String line;
				while ((line = reader.readLine()) != null) {
					String[] parts = line.split(",");
					if (parts.length == 2) {
						storage.put(parts[0], parts[1]);
					}
				}
				LOGGER.info("Loaded data from " + filePath + " file");
			} catch (IOException e) {
				LOGGER.log(Level.SEVERE, "Error loading data from " + filePath + " file", e);
			}
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, "Error creating " + filePath + " file", e);
		}
	}
	
	
	


	public void stopServer() {
		running = false;
		try {
			if (serverSocket != null && !serverSocket.isClosed()) {
				serverSocket.close();
			}
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, "Error closing server socket", e);
		}
	}

	private boolean initializeServer() {
        if (serverSocket == null) {
            try {
                serverSocket = new ServerSocket(port);
                return true;
            } catch (IOException e) {
                LOGGER.log(Level.SEVERE, "Error! Cannot open server socket:", e);
                return false; // Return false if server socket cannot be opened
            }
        }
        return true;
    }



	private void saveDataToStorage() {
		try (BufferedWriter writer = new BufferedWriter(new FileWriter("kvstorage.txt"))) {
			for (Entry<String, String> entry : storage.entrySet()) {
				writer.write(entry.getKey() + "," + entry.getValue());
				writer.newLine();
			}
			LOGGER.info("Storage data saved to file");
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, "Error saving data to storage file", e);
		}
	}
	
	
	@Override
	public void close() {
		try {
			running = false;

			// Close the server socket
			if (serverSocket != null && !serverSocket.isClosed()) {
				serverSocket.close();
			}

			// Wait for client handler threads to complete
			for (Thread thread : clientHandlerThreads) {
				try {
					thread.join(); // Wait for the thread to finish
				} catch (InterruptedException e) {
					// Handle the exception if needed
					LOGGER.warning("Error waiting for client handler thread to complete: " + e.getMessage());
				}
			}

			// Perform any necessary cleanup, like saving data to storage
			saveDataToStorage();
		} catch (IOException e) {
			LOGGER.warning("Error while closing the server: " + e.getMessage());
			e.printStackTrace();
		}
	}

	@Override
    public void kill(){
		// TODO Auto-generated method stub
		// STOP THE SERVER? 
		running = false; 
		try{
			if(serverSocket != null && !serverSocket.isClosed()){
				serverSocket.close(); 
			}
		// Immediately terminate any ongoing processing
        // This might involve interrupting active threads or shutting down a thread pool

		} catch (IOException e) {
			// Handle exceptions, e.g., log them
			e.printStackTrace();
		}
		LOGGER.info("Server Socket Closed");
	}

	public static void main(String[] args) {
		// Default values
		int port = 50005;
		int cacheSize = 10; // Example default cache
		String ipAddress = "127.0.0.1";
		String strategy = "FIFO";
		
		// Parse command line arguments
		for (int i = 0; i < args.length; i++) {
			if ("-p".equals(args[i]) && i + 1 < args.length) {
				port = Integer.parseInt(args[i + 1]);
			}
			if ("-a".equals(args[i]) && i + 1 < args.length) {
				ipAddress = args[i + 1];
			}
		}
	
		// Initialize and start the server
		KVServer server = new KVServer(port, cacheSize, strategy);
    	server.run();
	}
	
}