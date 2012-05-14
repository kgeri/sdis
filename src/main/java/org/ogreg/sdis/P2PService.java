package org.ogreg.sdis;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

/**
 * Contract for services which provide access to a Peer-to-Peer network.
 * 
 * @author gergo
 */
public interface P2PService {

	/**
	 * Tries to make contact with a supported P2P service on the specified port.
	 * <p>
	 * Upon successful communication, the service might use this contact as a peer. This is basically for telling the
	 * P2P service about new, externally identified nodes (ie. bootstrapping).
	 * 
	 * @param address
	 */
	void contact(InetSocketAddress address);
	
	/**
	 * Stores the specified data chunk in the P2P network.
	 * <p>
	 * The data buffer must be pre-positioned, and its limit must be set properly. The specified instance will not be
	 * modified (limit, position).
	 * 
	 * @param data
	 */
	void store(ByteBuffer data);
}
