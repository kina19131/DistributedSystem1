package app_kvServer;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.Map.Entry; 



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

	private Map<String, String> storage;
    private Map<String, String> cache;
    private Queue<String> fifoQueue; // For FIFO caching
	private Map<String, Integer> accessFrequency; // For LFU caching 

    private int cacheSize;
	private IKVServer.CacheStrategy strategy; // Correct type for strategy

	private static final Logger LOGGER = Logger.getLogger(KVServer.class.getName());



	public KVServer(int port, int cacheSize, String strategy) {
        this.port = port;
		this.running = true; 
        this.cacheSize = cacheSize;
        this.strategy = IKVServer.CacheStrategy.valueOf(strategy);

		this.activeClientHandlers = Collections.synchronizedSet(new HashSet<>());

        this.storage = new HashMap<>();

        if (cacheSize > 0) { // Initialize cache for all strategies if cacheSize > 0
            this.cache = new HashMap<>();
            if ("FIFO".equals(strategy)) {
                this.fifoQueue = new LinkedList<>();
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

	// FIFO: The oldest item is evicted when the cache is full.
	// LRU: The least recently used item is evicted. Your implementation keeps the most recently used items at the end of the cache map.
	// LFU: The least frequently used item is evicted. You use an accessFrequency map to track the access frequency of each key.
	private void updateCacheFIFO(String key, String value) {
        if (!cache.containsKey(key) && fifoQueue.size() >= cacheSize) {
            String oldestKey = fifoQueue.poll();
            cache.remove(oldestKey);
        }
        cache.put(key, value);
        fifoQueue.add(key);
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
                String leastFrequentKey = findLeastFrequentKeyLFU(); // Use a separate method for Java 7 compatibility
                cache.remove(leastFrequentKey);
                accessFrequency.remove(leastFrequentKey);
            }
            cache.put(key, value);
            accessFrequency.put(key, 1);
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

	@Override
    public void run() {
        try {
            serverSocket = new ServerSocket(port);
            LOGGER.info("KV Server started on port " + port);
            running = true; // MODIFIED: Ensure running is set to true when server starts

            while (running) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    ClientHandler handler = new ClientHandler(clientSocket, this); // MODIFIED: Ensure this line is correctly written
                    new Thread(handler).start();
                } catch (IOException e) {
                    if (!running) {
                        LOGGER.info("Server stopped.");
                    } else {
                        LOGGER.log(Level.SEVERE, "Error in server run", e);
                    }
                }
            }
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Error starting server", e);
        }
    }


	private void saveDataToStorage() {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter("kvstorage.txt"))) {
            for (Entry<String, String> entry : storage.entrySet()) {
                writer.write(entry.getKey() + "," + entry.getValue());
                writer.newLine();
            }
            LOGGER.info("Storage data saved to file");
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Error saving data to storage", e);
        }
    }
	
	@Override
    public void close(){
		// TODO Auto-generated method stub
		try {
			running = false; 

			if (serverSocket != null && !serverSocket.isClosed()){
				serverSocket.close(); 
			}
			// Optionally, wait for currently processing client handlers to complete
        	// This might involve tracking active threads or using a thread pool

			// Perform any necessary cleanup, like saving data to storage
			saveDataToStorage(); // TO DO
		}
		catch (IOException e){
			e.printStackTrace();
		}
		LOGGER.info("Server closed");
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
}
