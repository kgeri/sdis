package org.ogreg.sdis;

import java.util.Random;

import org.ogreg.sdis.messages.Kademlia.Message;

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

	/**
	 * @param msg
	 *            the message
	 * 
	 * @return the {@link BinaryKey} from the message.
	 * @throws IllegalArgumentException
	 *             if the key is not set
	 */
	public static BinaryKey ensureHasKey(Message msg) {
		if (msg.hasKey()) {
			return new BinaryKey(msg.getKey().toByteArray());
		} else {
			throw new IllegalArgumentException("Malformed " + msg.getType() + " message: key not set");
		}
	}

	/**
	 * @param msg
	 *            the message
	 * 
	 * @return the data from the message.
	 * @throws IllegalArgumentException
	 *             if the data is not set
	 */
	public static ByteString ensureHasKeyData(Message msg) {
		if (msg.hasData()) {
			return msg.getData();
		} else {
			throw new IllegalArgumentException("Malformed " + msg.getType() + " message: data not set");
		}
	}
}
