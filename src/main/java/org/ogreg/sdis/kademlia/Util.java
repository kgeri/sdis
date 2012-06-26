package org.ogreg.sdis.kademlia;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;
import java.util.Random;

import javax.xml.bind.DatatypeConverter;

import org.jboss.netty.buffer.ChannelBuffer;
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
	 * @param frame
	 *            the message
	 * 
	 * @return the data from the message.
	 * @throws IllegalArgumentException
	 *             if the data is not set
	 */
	public static ChannelBuffer ensureHasData(Frame frame) {
		if (frame.hasData()) {
			return frame.getData();
		} else {
			throw new IllegalArgumentException("Malformed " + frame.getMessage().getType() + " message: data not set");
		}
	}

	/**
	 * Generates the SHA-1 checksum from the given <code>data</code>.
	 * <p>
	 * This operation should have no effect on the input buffer at all.
	 * 
	 * @param data
	 * @return
	 */
	public static BinaryKey checksum(ByteBuffer data) {
		data = data.asReadOnlyBuffer();
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
	 * Determines the {@link Contact} from the specified parameters.
	 * 
	 * @param nodeId
	 *            The Kademlia ID of the node
	 * @param address
	 *            The IP address of the node (either IPv4 or IPv6)
	 * @param port
	 *            The port which the node uses
	 * @return
	 */
	public static Contact toContact(ByteString nodeId, ByteString address, int port) {
		try {
			InetSocketAddress addr = new InetSocketAddress(InetAddress.getByAddress(address.toByteArray()), port);
			return new Contact(new BinaryKey(nodeId.toByteArray()), addr);
		} catch (UnknownHostException e) {
			throw new IllegalArgumentException("Invalid Node address: " + address, e);
		}
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
	 * @param type
	 * @param nodeId
	 * @param address
	 * @param rpcId
	 * @return An initialized message builder with the specified type and node id set.
	 */
	public static Builder message(MessageType type, ByteString nodeId, InetSocketAddress address, ByteString rpcId) {
		ByteString addr = ByteString.copyFrom(address.getAddress().getAddress());
		int port = address.getPort();
		return Message.newBuilder().setType(type).setNodeId(nodeId).setAddress(addr).setPort(port).setRpcId(rpcId);
	}

	/**
	 * @param bytes
	 * @return The bytes encoded in Base64
	 */
	public static String toString(ByteString bytes) {
		return DatatypeConverter.printHexBinary(bytes.toByteArray());
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
