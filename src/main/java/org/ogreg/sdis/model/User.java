package org.ogreg.sdis.model;

import java.util.ArrayList;
import java.util.List;

/**
 * Entity representing a person of the network.
 * 
 * @author gergo
 */
public class User {

	/**
	 * The user's first (given) name.
	 */
	private String firstName;

	/**
	 * The user's last (family) name.
	 */
	private String lastName;

	/**
	 * The user's nickname (optional).
	 */
	private String nickName = null;

	/**
	 * The {@link Node}s this user owns (optional, but never null).
	 */
	private List<Node> nodes = new ArrayList<Node>();
}
