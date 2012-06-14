package org.ogreg.sdis.kademlia;

import static org.ogreg.sdis.kademlia.Protocol.MessageType.REQ_FIND_NODE;
import static org.ogreg.sdis.kademlia.Protocol.MessageType.REQ_PING;
import static org.ogreg.sdis.kademlia.Protocol.MessageType.REQ_STORE;
import static org.ogreg.sdis.kademlia.Protocol.MessageType.RSP_SUCCESS;
import static org.ogreg.sdis.kademlia.Util.message;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.List;

import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelException;
import org.ogreg.sdis.CommonUtil;
import org.ogreg.sdis.P2PService;
import org.ogreg.sdis.model.BinaryKey;
import org.ogreg.sdis.storage.InMemoryStorageServiceImpl;
import org.ogreg.sdis.util.Properties;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.google.protobuf.ByteString;

/**
 * Tests the behaviour of the Kademlia server.
 * 
 * @author gergo
 */
@Test(groups = "functional")
public class ServerTest {

	private Properties properties;
	private Server alice;
	private Server bob;
	private Server charlie;

	private InMemoryStorageServiceImpl aliceStore;
	private InMemoryStorageServiceImpl bobStore;
	private InMemoryStorageServiceImpl charlieStore;

	@BeforeTest
	public void setUp() {
		properties = new Properties();
		properties.put("maxContacts", 2); // Note: k = 2!

		aliceStore = new InMemoryStorageServiceImpl();
		alice = new Server(aliceStore, key(0, 0, 0, 0, 1), properties);
		alice.setPortRange(6000, 6100);

		bobStore = new InMemoryStorageServiceImpl();
		bob = new Server(bobStore, key(0, 0, 0, 0, 2), properties);
		bob.setPortRange(6000, 6100);

		charlieStore = new InMemoryStorageServiceImpl();
		charlie = new Server(charlieStore, key(0, 0, 0, 0, 3), properties);
		charlie.setPortRange(6000, 6100);

		alice.start();
		bob.start();
		charlie.start();
	}

	@AfterTest
	public void tearDown() {
		alice.stop();
		bob.stop();
		charlie.stop();
	}

	/**
	 * Ensures that a PING message is processed and a RSP_SUCCESS is returned.
	 */
	public void testPingMessage() throws Exception {
		Frame req = new Frame(message(REQ_PING, alice.getNodeId(), alice.getAddress()).build());
		Frame rsp = alice.sendMessageSync(req, bob.getAddress());

		assertEquals(rsp.getMessage().getNodeId(), bob.getNodeId());
		assertEquals(rsp.getMessage().getType(), RSP_SUCCESS);
	}

	/**
	 * Ensures that a FIND_NODE message returns the known neighbours. The queried node should not be returned, as it's
	 * useless information, because the requestor already knows about it. Also tests that the
	 * {@link P2PService#contact(java.net.InetSocketAddress)} function tries to make contact with the specified address.
	 */
	public void testFindNodeMessage() throws Exception {
		// Only Bob knows Charlie, Alice will ask Bob
		bob.contact(charlie.getAddress());

		Frame req = new Frame(message(REQ_FIND_NODE, alice.getNodeId(), alice.getAddress())
				.setKey(keyBS(0, 0, 0, 0, 7)).build());
		Frame rsp = alice.sendMessageSync(req, bob.getAddress());

		assertEquals(rsp.getMessage().getNodeId(), bob.getNodeId());
		assertEquals(rsp.getMessage().getType(), RSP_SUCCESS);
		assertEquals(rsp.getMessage().getNodesCount(), 1);
		assertBSEquals(rsp.getMessage().getNodes(0).getNodeId(), charlie.getNodeId());
	}

	/**
	 * Ensures that a STORE message stores the specified data chunk at the appropriate node.
	 */
	public void testStoreMessage() throws Exception {
		ByteBuffer data = data(4096);
		BinaryKey dataKey = Util.checksum(data);
		ByteString dataKeyBS = ByteString.copyFrom(dataKey.toByteArray());

		Frame req = new Frame(message(REQ_STORE, alice.getNodeId(), alice.getAddress()).setKey(dataKeyBS).build(),
				ChannelBuffers.wrappedBuffer(data));
		Frame rsp = alice.sendMessageSync(req, bob.getAddress());

		assertEquals(rsp.getMessage().getNodeId(), bob.getNodeId());
		assertEquals(rsp.getMessage().getType(), RSP_SUCCESS);
		assertEquals(bobStore.load(dataKey), data);
	}

	/**
	 * Ensures that a full STORE operation - including the iterativeFindNode cycle - works and stores the content at the
	 * appropriate nodes.
	 */
	public void testStore() throws Exception {
		alice.contact(bob.getAddress());
		bob.contact(charlie.getAddress());

		ByteBuffer data = data(4096);
		BinaryKey dataKey = Util.checksum(data);

		alice.store(data);

		// Because of iterativeFindNode, Alice will eventually get know of Charlie
		// Since k is 2, it will be stored both on Bob and Charlie
		assertEquals(bobStore.load(dataKey), data);
		assertEquals(charlieStore.load(dataKey), data);

		// But not on Alice
		assertNull(aliceStore.load(dataKey));
	}

	/**
	 * Ensures that a STOREd content can be retrieved from the network - testing the iterativeFindValue cycle.
	 */
	public void testLoad() throws Exception {
		alice.contact(bob.getAddress());
		bob.contact(charlie.getAddress());

		ByteBuffer data = data(4096);
		BinaryKey dataKey = Util.checksum(data);

		charlieStore.store(dataKey, data);

		assertEquals(alice.load(dataKey), data);

		// The data should not be on Bob, he was just an intermediary
		assertNull(bobStore.load(dataKey));

		// And now Alice should know Charlie
		assertKnows(alice, charlie);
	}

	/**
	 * Tests what happens if the starting port is already bound. Tests what happens if no port is free.
	 */
	@Test(expectedExceptions = ChannelException.class)
	public void testBindError() throws Exception {
		Server dave = new Server(new InMemoryStorageServiceImpl(), key(0, 0, 0, 0, 1), properties);
		dave.setPortRange(alice.getAddress().getPort(), bob.getAddress().getPort());
		dave.start();
	}

	/**
	 * Ensures that calling start() twice results in an error.
	 */
	@Test(expectedExceptions = IllegalStateException.class)
	public void testRestartError() throws Exception {
		alice.start();
	}

	/**
	 * Ensures that calling stop() if the service is not running, has no effect.
	 */
	public void testReStopNoError() {
		Server dave = new Server(new InMemoryStorageServiceImpl(), key(0, 0, 0, 0, 1), properties);
		dave.stop();
	}

	BinaryKey key(int... value) {
		return new BinaryKey(value);
	}

	ByteString keyBS(int... value) {
		return ByteString.copyFrom(CommonUtil.toByteArray(value));
	}

	ByteBuffer data(int size) {
		byte[] bytes = new byte[size];
		for (int i = 0; i < size; i++) {
			bytes[i] = (byte) i;
		}
		return ByteBuffer.wrap(bytes);
	}

	void assertBSEquals(ByteString actual, ByteString expected) {
		assertEquals(CommonUtil.toHexString(actual.toByteArray()), CommonUtil.toHexString(expected.toByteArray()));
	}

	void assertKnows(Server server1, Server server2) {
		BinaryKey server2Key = new BinaryKey(server2.getNodeId().toByteArray());
		List<Contact> contacts = server1.getRoutingTable().getClosestTo(server2Key, 100);
		assertTrue(contacts.contains(new Contact(server2Key, server2.getAddress())));
	}
}
