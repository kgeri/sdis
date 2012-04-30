package org.ogreg.sdis.kademlia;

import static org.ogreg.sdis.kademlia.Protocol.MessageType.REQ_PING;
import static org.ogreg.sdis.kademlia.Protocol.MessageType.RSP_SUCCESS;
import static org.ogreg.sdis.kademlia.Util.message;
import static org.testng.Assert.assertEquals;

import org.ogreg.sdis.kademlia.Protocol.Message;
import org.ogreg.sdis.model.BinaryKey;
import org.ogreg.sdis.storage.InMemoryStorageServiceImpl;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

/**
 * Tests the behaviour of the Kademlia server.
 * 
 * @author gergo
 */
@Test(groups = "functional")
public class ServerTest {

	private Server alice;

	private Server bob;

	@BeforeTest
	public void setUp() {
		alice = new Server(new InMemoryStorageServiceImpl(), key(0, 0, 0, 0, 1));
		alice.setPortRange(5000, 5100);
		bob = new Server(new InMemoryStorageServiceImpl(), key(0, 0, 0, 0, 2));
		bob.setPortRange(5000, 5100);

		alice.start();
		bob.start();
	}

	@AfterTest
	public void tearDown() {
		alice.stop();
		bob.stop();
	}

	/**
	 * Ensures that PING messages work as expected.
	 */
	public void testPing() throws Exception {
		Message req = message(REQ_PING, alice.getNodeId()).build();
		Message rsp = alice.sendMessageSync(req, bob.getAddress());

		assertEquals(rsp.getNodeId(), bob.getNodeId());
		assertEquals(rsp.getType(), RSP_SUCCESS);
	}

	BinaryKey key(int... value) {
		return new BinaryKey(value);
	}
}
