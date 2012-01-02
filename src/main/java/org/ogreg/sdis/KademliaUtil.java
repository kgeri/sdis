package org.ogreg.sdis;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
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
	 * A thread local map caching instances of {@link MessageDigest}s for faster access.
	 */
	private static final ThreadLocal<MessageDigest> SHA1Digests = new ThreadLocal<MessageDigest>();

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
	public static ByteString ensureHasData(Message msg) {
		if (msg.hasData()) {
			return msg.getData();
		} else {
			throw new IllegalArgumentException("Malformed " + msg.getType() + " message: data not set");
		}
	}

	/**
	 * Checks the integrity of the <code>data</code> by calculating and comparing its checksum to <code>key</code>.
	 * 
	 * @param key
	 * @param data
	 */
	public static void ensureCorrectData(BinaryKey key, ByteString data) {

	}

	/**
	 * Generates the SHA-1 checksum from the given <code>data</code>.
	 * 
	 * @param data
	 * @return
	 */
	public static BinaryKey checksum(ByteString data) {
		MessageDigest digest = SHA1Digests.get();
		if (digest == null) {
			digest = createSHA1Digest();
			SHA1Digests.set(digest);
		}

		digest.reset();
		digest.update(data.asReadOnlyByteBuffer());
		byte[] key = digest.digest();

		return new BinaryKey(key);
	}

	private static MessageDigest createSHA1Digest() {
		try {
			return MessageDigest.getInstance("SHA-1");
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e);
		}
	}
}
