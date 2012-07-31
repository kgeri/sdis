package org.ogreg.sdis.kademlia;

import static org.ogreg.sdis.kademlia.Protocol.MessageType.REQ_FIND_NODE;
import static org.ogreg.sdis.kademlia.Protocol.MessageType.REQ_PING;
import static org.ogreg.sdis.kademlia.Protocol.MessageType.REQ_STORE;
import static org.ogreg.sdis.kademlia.Protocol.MessageType.RSP_SUCCESS;
import static org.ogreg.sdis.kademlia.Util.generateByteStringId;
import static org.ogreg.sdis.kademlia.Util.message;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.jboss.netty.buffer.ChannelBuffers;
import org.ogreg.sdis.CommonUtil;
import org.ogreg.sdis.P2PService;
import org.ogreg.sdis.model.BinaryKey;
import org.ogreg.sdis.storage.InMemoryStorageServiceImpl;
import org.ogreg.sdis.util.Properties;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.google.common.util.concurrent.Service.State;
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
		Frame req = new Frame(message(REQ_PING, alice.getNodeId(), alice.getAddress(), generateByteStringId()).build());
		Frame rsp = sendMessageSync(alice, req, bob.getAddress());

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
		contact(bob, charlie);

		Frame req = new Frame(message(REQ_FIND_NODE, alice.getNodeId(), alice.getAddress(), generateByteStringId())
				.setKey(keyBS(0, 0, 0, 0, 7)).build());
		Frame rsp = sendMessageSync(alice, req, bob.getAddress());

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

		Frame req = new Frame(message(REQ_STORE, alice.getNodeId(), alice.getAddress(), generateByteStringId()).setKey(
				dataKeyBS).build(), ChannelBuffers.wrappedBuffer(data));
		Frame rsp = sendMessageSync(alice, req, bob.getAddress());

		assertEquals(rsp.getMessage().getNodeId(), bob.getNodeId());
		assertEquals(rsp.getMessage().getType(), RSP_SUCCESS);
		assertEquals(bobStore.load(dataKey), data);
	}

	/**
	 * Ensures that a full STORE operation - including the iterativeFindNode cycle - works and stores the content at the
	 * appropriate nodes.
	 */
	public void testStore() throws Exception {
		contact(alice, bob);
		contact(bob, charlie);

		ByteBuffer data = data(4096);
		BinaryKey dataKey = Util.checksum(data);

		alice.store(data);
		Thread.sleep(500); // TODO Get rid of this

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
		contact(alice, bob);
		contact(bob, charlie);

		ByteBuffer data = data(4096);
		BinaryKey dataKey = Util.checksum(data);

		charlieStore.store(dataKey, data);

		assertEquals(alice.load(dataKey).get(), data);

		// The data should not be on Bob, he was just an intermediary
		assertNull(bobStore.load(dataKey));

		// And now Alice should know Charlie
		assertKnows(alice, charlie);
	}

	/**
	 * Tests what happens if the starting port is already bound. Tests what happens if no port is free.
	 */
	@Test
	public void testBindError() throws Exception {
		Server dave = new Server(new InMemoryStorageServiceImpl(), key(0, 0, 0, 0, 1), properties);
		dave.setPortRange(alice.getAddress().getPort(), bob.getAddress().getPort());
		dave.start();
		assertEquals(dave.state(), State.FAILED);
	}

	/**
	 * Ensures that calling start() twice does not restart the service.
	 */
	@Test
	public void testReStartNoError() throws Exception {
		assertEquals(alice.state(), State.RUNNING);
		int port = alice.getAddress().getPort();
		alice.start();
		assertEquals(alice.state(), State.RUNNING);
		assertEquals(alice.getAddress().getPort(), port);
	}

	/**
	 * Ensures that calling stop() if the service is not running, has no effect.
	 */
	public void testReStopNoError() {
		Server dave = new Server(new InMemoryStorageServiceImpl(), key(0, 0, 0, 0, 1), properties);
		assertEquals(dave.state(), State.NEW);
		dave.stop();
		assertEquals(dave.state(), State.TERMINATED);
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

	void contact(Server src, Server dest) throws InterruptedException, ExecutionException, TimeoutException {
		src.contact(dest.getAddress()).get();
	}

	Frame sendMessageSync(Server src, Frame request, InetSocketAddress dest) throws InterruptedException,
			ExecutionException, TimeoutException {
		return src.sendMessageASync(request, dest).get();
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
