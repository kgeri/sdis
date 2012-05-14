package org.ogreg.sdis.kademlia;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;
import java.util.Random;

import org.ogreg.sdis.kademlia.Protocol.Message;
import org.ogreg.sdis.kademlia.Protocol.Message.Builder;
import org.ogreg.sdis.kademlia.Protocol.MessageType;
import org.ogreg.sdis.kademlia.Protocol.Node;
import org.ogreg.sdis.model.BinaryKey;

import com.google.protobuf.ByteString;

/**
 * Common Kademlia-related utilities.
 * 
 * @author gergo
 */
class Util {

	private static final Random Rnd = new Random();

	/**
	 * A thread local map caching instances of {@link MessageDigest}s for faster access.
	 */
	private static final ThreadLocal<MessageDigest> SHA1Digests = new ThreadLocal<MessageDigest>();

	public static final Comparator<ContactWithDistance> ContactDistanceComparator = new Comparator<ContactWithDistance>() {
		@Override
		public int compare(ContactWithDistance o1, ContactWithDistance o2) {
			return o1.distance.compareTo(o2.distance);
		}
	};

	/**
	 * Generates a random identifier of {@link BinaryKey#LENGTH_BITS} bits, used for RPC keys and Node IDs.
	 * 
	 * @return
	 */
	public static ByteString generateByteStringId() {
		byte[] bytes = new byte[20];
		Rnd.nextBytes(bytes);
		return ByteString.copyFrom(bytes);
	}

	/**
	 * Generates a random identifier of {@link BinaryKey#LENGTH_BITS} bits, used for RPC keys and Node IDs.
	 * 
	 * @return
	 */
	public static BinaryKey generateId() {
		int[] value = new int[BinaryKey.LENGTH_INTS];
		for (int i = 0; i < BinaryKey.LENGTH_INTS; i++) {
			value[i] = Rnd.nextInt();
		}
		return new BinaryKey(value);
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
	 * Generates the SHA-1 checksum from the given <code>data</code>.
	 * 
	 * @param data
	 * @return
	 */
	public static BinaryKey checksum(ByteString data) {
		return checksum(data.asReadOnlyByteBuffer());
	}

	/**
	 * Generates the SHA-1 checksum from the given <code>data</code>.
	 * 
	 * @param data
	 * @return
	 */
	public static BinaryKey checksum(ByteBuffer data) {
		MessageDigest digest = SHA1Digests.get();
		if (digest == null) {
			digest = createSHA1Digest();
			SHA1Digests.set(digest);
		}

		digest.reset();
		digest.update(data);
		byte[] key = digest.digest();

		return new BinaryKey(key);
	}

	/**
	 * Converts the specified {@link Contact} to a {@link Node}.
	 * 
	 * @param contact
	 * @return
	 */
	public static Node toNode(Contact contact) {
		ByteString nodeId = ByteString.copyFrom(contact.nodeId.toByteArray());
		ByteString address = ByteString.copyFrom(contact.address.getAddress().getAddress());
		int port = contact.address.getPort();
		return Node.newBuilder().setNodeId(nodeId).setAddress(address).setPort(port).build();
	}

	/**
	 * Determines the {@link Contact} from the specified node.
	 * 
	 * @param node
	 * @return
	 */
	public static Contact toContact(Node node) {
		try {
			byte[] addr = node.getAddress().toByteArray();
			InetSocketAddress address = new InetSocketAddress(InetAddress.getByAddress(addr), node.getPort());
			return new Contact(new BinaryKey(node.getNodeId().toByteArray()), address);
		} catch (UnknownHostException e) {
			throw new IllegalArgumentException("Invalid Node: " + node, e);
		}
	}

	/**
	 * Determines the {@link Contact} from the specified incoming message parameters.
	 * 
	 * @param nodeId
	 * @param address
	 * @return
	 */
	public static Contact toContact(ByteString nodeId, InetSocketAddress address) {
		return new Contact(new BinaryKey(nodeId.toByteArray()), address);
	}

	/**
	 * Calculates the distance of the two keys using the XOR metric.
	 * 
	 * @return A {@link BinaryKey} holding the distance between k1 and k2
	 */
	public static BinaryKey distanceOf(BinaryKey k1, BinaryKey k2) {
		return new BinaryKey(k1.xor(k2));
	}

	/**
	 * @param nodeId
	 * @param type
	 * @return An initialized message builder with the specified type and node id set.
	 */
	public static Builder message(MessageType type, ByteString nodeId) {
		ByteString rpcId = Util.generateByteStringId();
		return Message.newBuilder().setType(type).setNodeId(nodeId).setRpcId(rpcId);
	}

	private static MessageDigest createSHA1Digest() {
		try {
			return MessageDigest.getInstance("SHA-1");
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e);
		}
	}

	// An immutable Contact-distance pair
	public static class ContactWithDistance {
		public final Contact contact;

		public final BinaryKey distance;

		public ContactWithDistance(Contact contact, BinaryKey distance) {
			this.contact = contact;
			this.distance = distance;
		}
	}
}
