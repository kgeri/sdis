package org.ogreg.sdis.model;

import java.net.InetSocketAddress;

/**
 * Represents a computing device running an instance of SDiS.
 * <p>
 * Nodes may change their listening address and port at any time.
 * 
 * @author gergo
 */
public interface Node {

	/**
	 * @return The address of the node.
	 */
	InetSocketAddress getAddress();

}
