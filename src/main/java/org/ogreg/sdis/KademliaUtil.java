package org.ogreg.sdis;

import java.util.Random;

import com.google.protobuf.ByteString;

/**
 * Common Kademlia-related utilities.
 * 
 * @author gergo
 */
public class KademliaUtil {
	
	private static final Random Rnd = new Random();

	/**
	 * Generates a random 20-byte identifier used for RPC keys and Node IDs.
	 * 
	 * @return
	 */
	public static ByteString generateId() {
		byte[] bytes = new byte[20];
		Rnd.nextBytes(bytes);
		return ByteString.copyFrom(bytes);
	}
}
