package org.ogreg.sdis;

import java.net.InetSocketAddress;

/**
 * Contract for services which provide access to a Peer-to-Peer network.
 * 
 * @author gergo
 */
public interface P2PService {

	/**
	 * Tells the service that a supported P2P service might be listening on the specified port.
	 * 
	 * @param address
	 */
	void add(InetSocketAddress address);
}
