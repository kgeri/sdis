package org.ogreg.sdis.model;

import java.net.InetSocketAddress;
import java.security.PublicKey;

/**
 * Entity representing a computing device running an instance of SDiS.
 * 
 * Nodes may change their listening address and port at any time.
 * 
 * @author gergo
 */
public class Node {

	/**
	 * The public key of the node.
	 * 
	 * Public keys identify nodes and make it possible to verify the
	 * authenticity of the messages received from them.
	 */
	private PublicKey publicKey;

	/**
	 * The last known address of the node (optional).
	 */
	private InetSocketAddress lastKnownAddress = null;

	/**
	 * The nick name of the node (optional).
	 */
	private String nickName = null;
}
