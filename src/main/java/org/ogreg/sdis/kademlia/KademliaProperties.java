package org.ogreg.sdis.kademlia;

import org.ogreg.sdis.kademlia.Protocol.MessageType;
import org.ogreg.sdis.util.Properties;

/**
 * VO for holding Kademlia client and server properties.
 * 
 * @author gergo
 */
class KademliaProperties {

	/**
	 * The number of milliseconds to wait before a {@link MessageType#REQ_PING} times out.
	 */
	final int responseTimeOutMs;

	/**
	 * The maximum number of contacts stored in a k-bucket (k).
	 */
	final int maxContacts;

	/**
	 * The degree of parallelism in network calls (alpha).
	 */
	final int maxParallelism;

	/**
	 * The maximum number of server worker threads.
	 */
	final int serverThreads;

	/**
	 * The maximum number of client worker threads.
	 */
	final int clientThreads;

	public KademliaProperties(Properties properties) {
		this.responseTimeOutMs = properties.get("responseTimeoutMs", 10000, Integer.class);
		this.maxContacts = properties.get("maxContacts", 20, Integer.class);
		this.maxParallelism = properties.get("maxParallelism", 3, Integer.class);
		this.serverThreads = properties.get("serverThreads", 1, Integer.class);
		this.clientThreads = properties.get("clientThreads", 1, Integer.class);
	}
}
