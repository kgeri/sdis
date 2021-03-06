package org.ogreg.sdis;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.Future;

import org.ogreg.sdis.model.BinaryKey;

/**
 * Contract for services which provide access to a Peer-to-Peer network.
 * 
 * @author gergo
 */
public interface P2PService {

	/**
	 * Makes contact with a supported P2P service on the specified port asynchronously.
	 * <p>
	 * Upon successful communication, the service might use this contact as a peer. This is basically for telling the
	 * P2P service about new, externally identified nodes (ie. bootstrapping).
	 * 
	 * @param address
	 * @return True if the node was contacted successfully
	 */
	Future<Boolean> contact(InetSocketAddress address);

	/**
	 * Stores the specified data chunk in the P2P network.
	 * <p>
	 * The data buffer must be pre-positioned, and its limit must be set properly. The specified instance will not be
	 * modified (limit, position).
	 * 
	 * @param data
	 *            The data to store
	 * @return The key on which this data chunk was stored, or null if store failed.
	 */
	Future<BinaryKey> store(ByteBuffer data);

	/**
	 * Loads data with the specified key from the P2P network.
	 * 
	 * @param key
	 *            The key to search for
	 * @return A buffer positioned to the requested data, or null if the data was not found.
	 */
	Future<ByteBuffer> load(BinaryKey key);
}
