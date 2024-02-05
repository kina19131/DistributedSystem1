package testing;

import org.junit.Test;
import junit.framework.TestCase;

import client.KVStore;
import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;

import java.io.File;
import java.net.UnknownHostException;



public class AdditionalTest extends TestCase {
	
	private KVStore kvClient;

    public void setUp() {
        kvClient = new KVStore("localhost", 50000);
        try {
            kvClient.connect();
        } catch (Exception e) {
        }
    }

    public void tearDown() {
        kvClient.disconnect();

        // Clean up storage file
        File file = new File("./kvstorage.txt");
        if (file.delete()) { 
            System.out.println("Deleted the file: " + file.getName());
        } else {
            System.out.println("Failed to delete the file.");
        }
    }
	
	@Test
	public void testPutLongKey() {
		String key = "VeryLongKey";  
		String value = "longKeyTestValue";
		KVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.PUT_SUCCESS);
	}


    // Additional Test Case 2: Put a key-value pair with a special character in the key
    @Test
    public void testPutSpecialCharacterKey() {
        String key = "!@#$%^&*()";
        String value = "specialCharacterKeyTestValue";
        KVMessage response = null;
        Exception ex = null;

        try {
            response = kvClient.put(key, value);
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null && response.getStatus() == StatusType.PUT_SUCCESS);
    }

    // Additional Test Case 3: Put large value
    @Test
	public void testPutLargeValue() {
		String key = "thisKey";
		String value = "A".repeat(1000);  
		KVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.PUT_SUCCESS);
	}


    // // Additional Test Case 4: Attempt to get a key that was never inserted
    // @Test
    // public void testGetNonExistentKey() {
    //     String key = "nonExistentKey";
    //     KVMessage response = null;
    //     Exception ex = null;

    //     try {
    //         response = kvClient.get(key);
    //     } catch (Exception e) {
    //         ex = e;
    //     }

    //     assertTrue(ex == null && response.getStatus() == StatusType.GET_ERROR);
    // }

    // Additional Test Case 5: Attempt to put a value with space 
    @Test
	public void testValueWithSpaces() {
		String key = "this_key";
		String value = "this is value with space";
		KVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.PUT_SUCCESS);
	}


    // Additional Test Case 6: Attempt to put an empty value which should delete the value of the key
    @Test
    public void testPutEmptyValue() {
        String key = "this_key";
        String value = "";
        KVMessage response = null;
        Exception ex = null;

        try {
            response = kvClient.put(key, value);
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null && response.getStatus() == StatusType.DELETE_SUCCESS);
    }


    // Additional Test Case 7: Put a key-value pair and then delete using NULL value
    @Test
    public void testPutAndDeleteNull() {
        String key = "test8";
        String value = "putAndDeleteTestValue";
        KVMessage putResponse = null;
        KVMessage deleteResponse = null;
        KVMessage getResponse = null;
        Exception ex = null;

        try {
            putResponse = kvClient.put(key, value);
            deleteResponse = kvClient.put(key, "null");
            getResponse = kvClient.get(key);
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null && putResponse.getStatus() == StatusType.PUT_SUCCESS
                && deleteResponse.getStatus() == StatusType.DELETE_SUCCESS
                && getResponse.getStatus() == StatusType.GET_ERROR);
    }

    
    @Test
    public void testGetAfterDisconnect() {
        // Step 1: Disconnect the client from the server
        kvClient.disconnect();

        // Step 2: Reconnect to the server
        Exception reconnectionEx = null;
        try {
            kvClient.connect();
        } catch (Exception e) {
            reconnectionEx = e;
        }

        // Ensure reconnection was successful before proceeding
        assertTrue("Reconnection failed?", reconnectionEx == null);

        // Step 3: Attempt to get the key
        String key = "VeryLongKey";
        KVMessage response = null;
        Exception ex = null;
        try {
            response = kvClient.get(key);
        } catch (Exception e) {
            ex = e;
        }

        // Step 4: Assert the expected outcome of the get operation
        // Assuming you expect the key to exist and return a successful response
        assertTrue("Get operation failed after reconnection", ex == null && response.getStatus() == StatusType.GET_SUCCESS);
    }


    // @Test
    // public void testPutWithNullKey() {
    //     String key = null;
    //     String value = "testValue";
    //     KVMessage response = null;
    //     Exception ex = null;

    //     try {
    //         response = kvClient.put(key, value);
    //     } catch (Exception e) {
    //         ex = e;
    //     }

    //     assertTrue(ex != null && response.getStatus() == StatusType.PUT_ERROR);
    // }

    @Test
    public void testPutSameValueMultipleTimes() {
        String key = "repeatedValueKey";
        String value = "repeatedValue";
        KVMessage putResponse1 = null;
        KVMessage putResponse2 = null;
        KVMessage putResponse3 = null;
        Exception ex = null;

        try {
            putResponse1 = kvClient.put(key, value);
            putResponse2 = kvClient.put(key, value);
            putResponse3 = kvClient.put(key, value);
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null && putResponse1.getStatus() == StatusType.PUT_SUCCESS
                && putResponse2.getStatus() == StatusType.PUT_UPDATE
                && putResponse3.getStatus() == StatusType.PUT_UPDATE);
    }

    // @Test
    // public void testDeleteNonExistentKeyAfterDisconnect() {
    //     kvClient.disconnect();
    //     String key = "nonExistentKeyAfterDisconnect";
    //     KVMessage response = null;
    //     Exception ex = null;

    //     try {
    //         response = kvClient.put(key, "null");
    //     } catch (Exception e) {
    //         ex = e;
    //     }

    //     assertTrue(ex != null && ex instanceof IllegalStateException);
    // }

    // @Test
    // public void testPutAndGetEmptyKey() {
    //     String key = "";
    //     String value = "valueForEmptyKey";
    //     KVMessage response = null;
    //     Exception ex = null;

    //     try {
    //         response = kvClient.put(key, value);
    //     } catch (Exception e) {
    //         ex = e;
    //     }

    //     assertTrue(ex != null && ex instanceof IllegalArgumentException);
    // }

    // @Test
    // public void testPutWithLargeKey() {
    //     String key = "A".repeat(2000); // A key larger than the limit
    //     String value = "largeKeyTestValue";
    //     KVMessage response = null;
    //     Exception ex = null;

    //     try {
    //         response = kvClient.put(key, value);
    //     } catch (Exception e) {
    //         ex = e;
    //     }

    //     assertTrue(ex != null && ex instanceof IllegalArgumentException);
    // }

    // @Test
    // public void testDeleteKeyWithNullValue() {
    //     String key = "nullValueKey";
    //     KVMessage putResponse = null;
    //     KVMessage deleteResponse = null;
    //     Exception ex = null;

    //     try {
    //         putResponse = kvClient.put(key, "null");
    //         deleteResponse = kvClient.put(key, "null");
    //     } catch (Exception e) {
    //         ex = e;
    //     }

    //     assertTrue(ex == null && putResponse.getStatus() == StatusType.PUT_SUCCESS
    //             && deleteResponse.getStatus() == StatusType.DELETE_SUCCESS);
    // }
   
}
