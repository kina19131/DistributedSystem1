package testing;

import org.junit.Test;

import app_kvServer.KVServer;
import client.KVStore;
import junit.framework.TestCase;
import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;
import shared.messages.SimpleKVMessage;

import static java.lang.Thread.sleep;

public class PerformanceTest extends TestCase {

	private KVStore kvClient;
    private KVServer kvServer;
	
    private int NUM_OPS = 10000;
	private int CACHE_SIZE = 100;
	private String CACHE_POLICY = "LRU";
	
	public void setUp() {
		kvServer = new KVServer(50005, CACHE_SIZE, CACHE_POLICY);
		kvClient = new KVStore("localhost", 50005);

		try {
			kvClient.connect();
		} catch (Exception e) {
		}
	}

	public void tearDown() {
		kvClient.disconnect();
        kvServer.close();
	}
	
	
	@Test
	public void test80Put20Get() {
		
		KVMessage response = null;
		Exception ex = null;

        long startTime = System.nanoTime();

        for (int i = 0; i < NUM_OPS * 0.8; i++) {
            try {
                response = kvClient.put("key" + i, "value" + i);
                assertEquals(StatusType.PUT_SUCCESS, response.getStatus());
            } catch (Exception e) {
                ex = e;
            }
        }
        for (int i = 0; i < NUM_OPS * 0.2; i++) {
            try {
                response = kvClient.get("key" + i);
                assertEquals(StatusType.GET_SUCCESS, response.getStatus());
            } catch (Exception e) {
                ex = e;
            }
        }
        
        long endTime = System.nanoTime();
		
        System.out.println(NUM_OPS + " operations with 80% PUT, 20% GET: " + (endTime - startTime) + " nanoseconds");

        // assertNull(ex);
        // assertEquals(StatusType.GET_SUCCESS, response.getStatus());
	}
	
	@Test
	public void test50Put50Get() {
		
		KVMessage response = null;
		Exception ex = null;

        long startTime = System.nanoTime();

        for (int i = 0; i < NUM_OPS * 0.5; i++) {
            try {
                response = kvClient.put("key" + i, "value" + i);
                assertEquals(StatusType.PUT_SUCCESS, response.getStatus());
            } catch (Exception e) {
                ex = e;
            }
        }
        for (int i = 0; i < NUM_OPS * 0.5; i++) {
            try {
                response = kvClient.get("key" + i);
                assertEquals(StatusType.GET_SUCCESS, response.getStatus());
            } catch (Exception e) {
                ex = e;
            }
        }
        
        long endTime = System.nanoTime();
		
        System.out.println(NUM_OPS + " operations with 50% PUT, 50% GET: " + (endTime - startTime) + " nanoseconds");

        // assertNull(ex);
        // assertEquals(StatusType.GET_SUCCESS, response.getStatus());
	}
	
    @Test
	public void test20Put80Get() {
		
		KVMessage response = null;
		Exception ex = null;

        long startTime = System.nanoTime();

        for (int i = 0; i < NUM_OPS * 0.2; i++) {
            try {
                response = kvClient.put("key" + i, "value" + i);
                assertEquals(StatusType.PUT_SUCCESS, response.getStatus());
            } catch (Exception e) {
                ex = e;
            }
        }
        for (int i = 0; i < NUM_OPS * 0.2; i++) {
            for (int j = 0; j < 4; j++) {
                try {
                    response = kvClient.get("key" + i);
                    assertEquals(StatusType.GET_SUCCESS, response.getStatus());
                } catch (Exception e) {
                    ex = e;
                }
            }
        }
        
        long endTime = System.nanoTime();
		
        System.out.println(NUM_OPS + " operations with 20% PUT, 80% GET: " + (endTime - startTime) + " nanoseconds");

        // assertNull(ex);
        // assertEquals(StatusType.GET_SUCCESS, response.getStatus());
	}

}
