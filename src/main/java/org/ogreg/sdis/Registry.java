package org.ogreg.sdis;

import org.ogreg.sdis.model.Node;
import org.ogreg.sdis.model.User;

/**
 * Contract for a stateful service capabe of storing up-to-date information
 * about {@link User}s on the network.
 * 
 * @author gergo
 */
public interface Registry {

	/**
	 * Signals to the registry that <code>user</code> was discovered on the
	 * network, so one or more of its {@link Node}s should be online.
	 * 
	 * @param user
	 */
	void discovered(User user);
}
