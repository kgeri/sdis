package org.ogreg.sdis.kademlia;

import static org.ogreg.sdis.kademlia.Protocol.MessageType.REQ_FIND_NODE;
import static org.ogreg.sdis.kademlia.Protocol.MessageType.REQ_FIND_VALUE;
import static org.ogreg.sdis.kademlia.Protocol.MessageType.REQ_PING;
import static org.ogreg.sdis.kademlia.Protocol.MessageType.REQ_STORE;
import static org.ogreg.sdis.kademlia.Protocol.MessageType.RSP_IO_ERROR;
import static org.ogreg.sdis.kademlia.Protocol.MessageType.RSP_SUCCESS;
import static org.ogreg.sdis.kademlia.Util.message;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelException;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.ogreg.sdis.P2PService;
import org.ogreg.sdis.StorageService;
import org.ogreg.sdis.kademlia.Protocol.Message;
import org.ogreg.sdis.kademlia.Protocol.Message.Builder;
import org.ogreg.sdis.kademlia.Protocol.MessageType;
import org.ogreg.sdis.kademlia.Protocol.Node;
import org.ogreg.sdis.kademlia.Util.ContactWithDistance;
import org.ogreg.sdis.model.BinaryKey;
import org.ogreg.sdis.util.Properties;
import org.ogreg.sdis.util.ProtobufFrameDecoder;
import org.ogreg.sdis.util.ProtobufFrameEncoder;
import org.ogreg.sdis.util.ShutdownException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

/**
 * The service endpoint listening for incoming requests from the P2P network.
 * 
 * @author gergo
 */
public class Server implements P2PService {

	private static final Logger log = LoggerFactory.getLogger(Server.class);

	/**
	 * The number of milliseconds to wait before a {@link MessageType#REQ_PING} times out.
	 */
	private final int responseTimeOutMs;

	/**
	 * The number of milliseconds to wait before a connection close attempt times out.
	 */
	private final int closeTimeOutMs;

	/**
	 * The maximum number of contacts stored in a k-bucket (k).
	 */
	private final int maxContacts;

	/**
	 * The degree of parallelism in network calls (alpha).
	 */
	private final int maxParallelism;

	/**
	 * The first port on which the server tries to listen (inclusive).
	 */
	private int portFrom;

	/**
	 * The last port on which the server listens (inclusive).
	 */
	private int portTo;

	/**
	 * The storage service used for loading and saving blocks of data.
	 */
	private StorageService store;

	/**
	 * The Kademlia routing table used by this node.
	 */
	private RoutingTable routingTable;

	/**
	 * The current Node's node ID.
	 */
	private final ByteString nodeId;

	private ExecutorService serverExecutor;

	private ServerBootstrap server;
	private ClientBootstrap client;

	private ChannelGroup channels = null;
	private InetSocketAddress address = null;

	private ReplaceAction updateAction;

	public Server(StorageService store, BinaryKey nodeId, Properties properties) {
		// Processing properties
		this.responseTimeOutMs = properties.get("responseTimeoutMs", 10000, Integer.class);
		this.closeTimeOutMs = properties.get("closeTimeOutMs", 3000, Integer.class);
		this.maxContacts = properties.get("maxContacts", 20, Integer.class);
		this.maxParallelism = properties.get("maxParallelism", 3, Integer.class);

		this.store = store;
		this.nodeId = ByteString.copyFrom(nodeId.toByteArray());
		this.routingTable = new RoutingTableFixedImpl(nodeId, maxContacts);
		this.serverExecutor = Executors.newCachedThreadPool();

		// Update action: if the appropriate k-bucket is full, then the recipient pings the k-bucket’s least-recently
		// seen node to decide what to do
		updateAction = new ReplaceAction() {
			@Override
			public Contact replace(Contact leastRecent, Contact newContact) {
				boolean response = false;
				try {
					response = sendPingSync(leastRecent.address);
				} catch (TimeoutException e) {
					log.debug("Ping timed out for: {}", leastRecent);
				}

				// If the least-recently seen node responds, the new contact is discarded
				// If the least-recently seen node fails to respond, the new contact is added
				return response ? leastRecent : newContact;
			}
		};
	}

	/**
	 * Starts listening on {@link #port}.
	 */
	public synchronized void start() {

		if (channels != null) {
			throw new IllegalStateException("Kademlia server already started. Channels: " + channels);
		}

		NioServerSocketChannelFactory serverChannelFactory = new NioServerSocketChannelFactory(serverExecutor,
				serverExecutor);
		server = new ServerBootstrap(serverChannelFactory);
		server.setPipelineFactory(new ChannelPipelineFactory() {
			@Override
			public ChannelPipeline getPipeline() throws Exception {
				ChannelPipeline p = Channels.pipeline();
				p.addLast("frameDecoder", new ProtobufFrameDecoder(Message.getDefaultInstance(), Frame.Factory));
				p.addLast("frameEncoder", new ProtobufFrameEncoder());
				p.addLast("handler", new ServerHandler());
				return p;
			}
		});

		NioClientSocketChannelFactory clientChannelFactory = new NioClientSocketChannelFactory(serverExecutor,
				serverExecutor);
		client = new ClientBootstrap(clientChannelFactory);
		client.setPipelineFactory(new ChannelPipelineFactory() {
			public ChannelPipeline getPipeline() throws Exception {
				ChannelPipeline p = Channels.pipeline();
				p.addLast("frameDecoder", new ProtobufFrameDecoder(Message.getDefaultInstance(), Frame.Factory));
				p.addLast("frameEncoder", new ProtobufFrameEncoder());
				p.addLast("handler", new ClientHandler());
				return p;
			}
		});

		channels = new DefaultChannelGroup("kademlia");

		for (int port = portFrom; port <= portTo; port++) {
			try {
				address = new InetSocketAddress(port);
				server.bind(address);
				log.info("Kademlia server started at {}", address);
				break;
			} catch (ChannelException e) {
				if (port < portTo) {
					log.warn("Failed to bind {}, trying next in range ({} - {})",
							new Object[] { port, portFrom, portTo });
				} else {
					log.error("Failed to bind port range ({} - {})", portFrom, portTo);
					channels = null;
					address = null;
					throw e;
				}
			}
		}
	}

	/**
	 * Stops the Kademlia server.
	 */
	public synchronized void stop() {
		if (channels != null) {
			channels.close().awaitUninterruptibly();
			client.releaseExternalResources();
			server.releaseExternalResources();
			log.info("Kademlia server stopped at {}", address);
			channels = null;
			address = null;
		}
	}

	@Override
	public void contact(InetSocketAddress address) {
		// Pinging peer
		try {
			// TODO Async pinging
			Frame req = new Frame(message(REQ_PING, nodeId, this.address).build());
			sendMessageSync(req, address);
			// TODO Direct errors to GUI
		} catch (TimeoutException e) {
			log.debug("Contact timed out for: {}", address);
		}
	}

	// Does an iterativeStore in the Kademlia network
	@Override
	public BinaryKey store(ByteBuffer data) {
		// Note: if you change this, make sure data is not modified, neither directly, nor by any side-effect
		BinaryKey key = Util.checksum(data);

		ByteString keyBS = ByteString.copyFrom(key.toByteArray());
		Message message = message(REQ_STORE, nodeId, this.address).setKey(keyBS).build();
		Frame frame = new Frame(message, ChannelBuffers.wrappedBuffer(data));

		List<Contact> contacts = iterativeFindNode(key);
		for (Contact contact : contacts) {
			try {
				Frame rsp = sendMessageSync(frame, contact.address);
				if (!rsp.getMessage().getType().equals(RSP_SUCCESS)) {
					log.error("Store failed for: {}", contact);
				}
			} catch (TimeoutException e) {
				log.debug("Store timed out for: {}", contact);
				continue;
			}
		}

		return key;
	}

	@Override
	public ByteBuffer load(BinaryKey key) {
		ChannelBuffer buffer = iterativeFindValue(key);
		// TODO Make sure that this does not copy the contents
		return buffer == null ? null : buffer.toByteBuffer();
	}

	/**
	 * Does an iterativeFindNode for the specified key and returns at most {@link #maxContacts} closest contacts.
	 * 
	 * @param key
	 * @param operation
	 *            the type of the operation to do iteratively - either REQ_FIND_NODE, or REQ_FIND_VALUE
	 */
	private List<Contact> iterativeFindNode(BinaryKey key) {
		final List<Contact> results = new ArrayList<Contact>(maxContacts);
		iterativeFind(key, new IterativeFindCallback() {
			@Override
			public Message getRequest(ByteString key) {
				return message(REQ_FIND_NODE, nodeId, Server.this.address).setKey(key).build();
			}

			@Override
			protected boolean onSuccess(Contact contact, Frame response) {
				results.add(contact);
				// We stop if we have k successfully probed contacts
				return results.size() >= maxContacts;
			}
		});
		return results;
	}

	/**
	 * Does an iterativeFindNode for the specified key and returns at most {@link #maxContacts} closest contacts.
	 * 
	 * @param key
	 * @return the data chunk for the searched key, or null if not found
	 */
	private ChannelBuffer iterativeFindValue(final BinaryKey key) {
		final ChannelBuffer[] result = new ChannelBuffer[1];
		iterativeFind(key, new IterativeFindCallback() {
			@Override
			public Message getRequest(ByteString key) {
				return message(REQ_FIND_VALUE, nodeId, Server.this.address).setKey(key).build();
			}

			@Override
			protected boolean onSuccess(Contact contact, Frame response) {
				if (!response.hasData()) {
					log.debug("Contact responded SUCCESS but no data to a FIND_VALUE: {}", contact);
					return false;
				}

				ChannelBuffer data = response.getData();

				// TODO Make sure that this does not copy the contents
				ByteBuffer buffer = data.toByteBuffer();
				BinaryKey computedKey = Util.checksum(buffer);

				if (!computedKey.equals(key)) {
					// Either someone was nasty, or there was a transfer error
					log.error("Data chunk cheksum mismatch (" + key + " != " + computedKey + ")");
					return false;
				}

				// We stop if we found the result
				result[0] = data;
				return true;
			}
		});
		return result[0];
	}

	/**
	 * Executes an iterativeFind operation.
	 * <p>
	 * This iteratively queries nodes of the network, and searches for a value or more nodes, depending on the callback
	 * implementation.
	 * 
	 * @param key
	 * @param callback
	 */
	private void iterativeFind(BinaryKey key, IterativeFindCallback callback) {
		Queue<ContactWithDistance> queue = new PriorityQueue<ContactWithDistance>(maxContacts,
				Util.ContactDistanceComparator);
		Set<Contact> seen = new HashSet<Contact>(maxContacts * 2);

		// Initially the queue is filled using the routing table
		for (Contact contact : routingTable.getClosestTo(key, maxParallelism)) {
			queue.add(new ContactWithDistance(contact, Util.distanceOf(key, contact.nodeId)));
		}

		ByteString keyBS = ByteString.copyFrom(key.toByteArray());

		// This will either be a REQ_FIND_NODE, or a REQ_FIND_VALUE
		Message message = callback.getRequest(keyBS);
		Frame frame = new Frame(message);

		// TODO Time-limited operation (?)
		ContactWithDistance cwd;
		while ((cwd = queue.poll()) != null) {
			Contact contact = cwd.contact;

			if (seen.contains(contact)) {
				continue;
			} else {
				seen.add(contact);
			}

			Frame rsp;

			try {
				// TODO Parallel execution
				rsp = sendMessageSync(frame, contact.address);
			} catch (TimeoutException e) {
				log.debug("FindNode timed out for: {}", contact);
				continue;
			}

			Message msg = rsp.getMessage();
			if (msg.getType() == RSP_SUCCESS) {

				if (callback.onSuccess(contact, rsp)) {
					break;
				}

				// Adding resulting nodes to queue
				for (Node node : msg.getNodesList()) {
					Contact newContact = Util.toContact(node.getNodeId(), node.getAddress(), node.getPort());
					queue.add(new ContactWithDistance(newContact, Util.distanceOf(key, newContact.nodeId)));
				}
			}
		}
	}

	// Callback interface for various iterativeFind implementations
	private abstract static class IterativeFindCallback {

		// The implementation returns the request used for the iterative find here
		public abstract Message getRequest(ByteString key);

		// The implementation returns true if all the results were found
		protected abstract boolean onSuccess(Contact contact, Frame response);
	}

	/**
	 * Sets the specified port range for this server (inclusive). Won't have effect until the server is restarted.
	 * 
	 * @param from
	 * @param to
	 */
	public void setPortRange(int from, int to) {
		this.portFrom = Math.min(from, to);
		this.portTo = Math.max(from, to);
	}

	// The bound server address, or null if the server is not bound, for testing
	InetSocketAddress getAddress() {
		return address;
	}

	// The nodeId of this server, for testing
	ByteString getNodeId() {
		return nodeId;
	}

	// The routing table, for testing
	RoutingTable getRoutingTable() {
		return routingTable;
	}

	/**
	 * Sends a {@link MessageType#REQ_PING} to <code>address</code> synchronously.
	 * 
	 * @param address
	 * 
	 * @return true if the remote node responded successfully
	 * @throws TimeoutException
	 *             if the request has timed out
	 */
	private boolean sendPingSync(InetSocketAddress address) throws TimeoutException {
		Message message = message(REQ_PING, nodeId, this.address).build();
		Frame frame = new Frame(message);
		Frame response = sendMessageSync(frame, address);
		return response.getMessage().getType().equals(RSP_SUCCESS);
	}

	private void onMessageReceived(Contact contact, Frame frame) {

		// When a Kademlia node receives any message from another node, it updates the appropriate k-bucket in the
		// routing table for the sender’s node ID
		routingTable.update(contact, updateAction);
	}

	/**
	 * Sends the <code>frame</code> to <code>address</code> asynchronously, and returns a future.
	 * 
	 * @param frame
	 * @param address
	 * 
	 * @return The future for the response received
	 */
	private Future<Frame> sendMessageASync(Frame frame, InetSocketAddress address) {
		ChannelFuture channelFuture = client.connect(address);
		return new MessageFuture(channelFuture, frame, address);
	}

	/**
	 * Sends the <code>message</code> to <code>address</code> synchronously, and returns the response.
	 * 
	 * @param frame
	 * @param address
	 * 
	 * @return The response received
	 * @throws TimeoutException
	 *             if the request has timed out
	 */
	// Package private for unit testing
	Frame sendMessageSync(Frame frame, InetSocketAddress address) throws TimeoutException {
		log.debug("Sending {} from {} to {}", new Object[] { frame.getType(), this.address, address });
		ChannelFuture channelFuture = client.connect(address);
		Frame rsp = new MessageFuture(channelFuture, frame, address).get(responseTimeOutMs, TimeUnit.MILLISECONDS);
		Message msg = rsp.getMessage();
		if (msg.getType().equals(RSP_SUCCESS)) {
			// TODO This should be done only in the server handler, as we should use async conversation states
			Contact contact = Util.toContact(msg.getNodeId(), msg.getAddress(), msg.getPort());
			onMessageReceived(contact, rsp);
		}
		return rsp;
	}

	/**
	 * Processes a {@link MessageType#REQ_PING} message.
	 * <p>
	 * Required:
	 * <ul>
	 * </ul>
	 * Optional:
	 * <ul>
	 * </ul>
	 * Results:<br>
	 * A {@link MessageType#RSP_SUCCESS}.
	 * 
	 * @param requestor
	 * @param request
	 * @return
	 */
	private Frame processPing(Contact requestor, Frame request) {
		return new Frame(message(RSP_SUCCESS, this.nodeId, this.address).build());
	}

	/**
	 * Processes a {@link MessageType#REQ_STORE} message.
	 * <p>
	 * Required:
	 * <ul>
	 * <li>the binary data chunk to store
	 * <li>the key of the data chunk
	 * </ul>
	 * Optional:
	 * <ul>
	 * </ul>
	 * Results:<br>
	 * A {@link MessageType#RSP_SUCCESS} a {@link MessageType#RSP_IO_ERROR}.
	 * 
	 * @param requestor
	 * @param request
	 * @return
	 */
	private Frame processStore(Contact requestor, Frame request) {
		BinaryKey key = Util.ensureHasKey(request.getMessage());
		// TODO 2-phase STORE (first signal if we already have the data, for performance)
		ChannelBuffer data = Util.ensureHasData(request);

		// TODO Make sure that this does not copy the contents
		ByteBuffer buffer = data.toByteBuffer();
		BinaryKey computedKey = Util.checksum(buffer);

		Message message;
		if (computedKey.equals(key)) {
			store.store(key, buffer);
			message = message(RSP_SUCCESS, nodeId, this.address).build();
		} else {
			log.error("Data chunk cheksum mismatch (" + key + " != " + computedKey + ")");
			message = message(RSP_IO_ERROR, nodeId, this.address).build();
		}
		return new Frame(message);
	}

	/**
	 * Processes a {@link MessageType#REQ_FIND_NODE} message.
	 * <p>
	 * Required:
	 * <ul>
	 * <li>the key of the searched data chunk
	 * </ul>
	 * Optional:
	 * <ul>
	 * </ul>
	 * Results:<br>
	 * A {@link MessageType#RSP_SUCCESS} along with the a list of {@link Node}s which are closer to the searched key.
	 * 
	 * @param requestor
	 * @param request
	 * @return
	 */
	private Frame processFindNode(Contact requestor, Frame request) {
		BinaryKey key = Util.ensureHasKey(request.getMessage());
		Builder builder = message(RSP_SUCCESS, this.nodeId, this.address);

		Collection<Contact> closestContacts = routingTable.getClosestTo(key, maxContacts);
		for (Contact c : closestContacts) {

			// The recipient of a FIND_NODE should never return a triple containing the nodeID of the requestor
			if (requestor.equals(c)) {
				continue;
			}

			Node node = Util.toNode(c);
			builder.addNodes(node);
		}
		return new Frame(builder.build());
	}

	/**
	 * Processes a {@link MessageType#REQ_FIND_VALUE} message.
	 * <p>
	 * Required:
	 * <ul>
	 * <li>the key of the requested data chunk
	 * </ul>
	 * Optional:
	 * <ul>
	 * </ul>
	 * Results:<br>
	 * A {@link MessageType#RSP_SUCCESS} along with the requested data chunk if it is stored locally, or calls
	 * {@link #processFindNode(Message, Builder)} to return the k closest nodes.
	 * 
	 * @param requestor
	 * @param request
	 * @return
	 */
	private Frame processFindValue(Contact requestor, Frame request) {
		BinaryKey key = Util.ensureHasKey(request.getMessage());
		ByteBuffer dataBuffer = store.load(key);
		if (dataBuffer != null) {
			Message message = message(RSP_SUCCESS, nodeId, this.address).build();
			return new Frame(message, ChannelBuffers.wrappedBuffer(dataBuffer));
		} else {
			return processFindNode(requestor, request);
		}
	}

	/**
	 * Processes a {@link MessageType#RSP_SUCCESS} message.
	 * <p>
	 * Required:
	 * <ul>
	 * </ul>
	 * Optional:
	 * <ul>
	 * <li>a list of {@link Node}s which are closer to the searched key, if the request was a
	 * {@link MessageType#REQ_FIND_NODE} or a {@link MessageType#REQ_FIND_VALUE}
	 * </ul>
	 * Results:<br>
	 * Internal processing, nothing is returned.
	 * 
	 * @param req
	 * @return
	 */
	private void processSuccess(Frame req) {
		// TODO process success result based on rpc id
	}

	/**
	 * Processes a {@link MessageType#RSP_IO_ERROR} message.
	 * <p>
	 * Required:
	 * <ul>
	 * </ul>
	 * Optional:
	 * <ul>
	 * </ul>
	 * Results:<br>
	 * Internal processing, nothing is returned.
	 * 
	 * @param req
	 */
	private void processIOError(Frame req) {
		// TODO process IO error result based on rpc id
	}

	private void unsupportedMessage(Contact contact, Frame request) {
		log.error("Unsupported message from: {} ({})", contact, request);
	}

	// The client handler for protobuf Kademlia messages
	private class ClientHandler extends SimpleChannelUpstreamHandler {

		private final BlockingQueue<Frame> responses = new LinkedBlockingQueue<Frame>();

		private volatile Channel channel;

		/**
		 * Sends a Kademlia message synchronously and returns the response received.
		 * 
		 * @return The response received from the server, or null if there was a timeout
		 * @throws InterruptedException
		 *             if the waiting for the response was interrupted
		 */
		public Frame sendMessage(Frame frame) throws InterruptedException {
			channel.write(frame);
			return responses.poll(responseTimeOutMs, TimeUnit.MILLISECONDS);
		}

		@Override
		public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
			channels.add(e.getChannel());
			channel = e.getChannel();
			super.channelOpen(ctx, e);
		}

		@Override
		public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
			responses.offer((Frame) e.getMessage());
		}

		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
			log.error("Unexpected communication error", e.getCause());
			e.getChannel().close();
		}
	}

	// The server handler for protobuf Kademlia messages
	private class ServerHandler extends SimpleChannelUpstreamHandler {

		@Override
		public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
			Frame frame = (Frame) e.getMessage();

			Message req = frame.getMessage();
			Contact src = Util.toContact(req.getNodeId(), req.getAddress(), req.getPort());

			onMessageReceived(src, frame);

			Frame rsp;

			// Processing messages
			switch (req.getType()) {
			case REQ_PING:
				rsp = processPing(src, frame);
				break;
			case REQ_STORE:
				rsp = processStore(src, frame);
				break;
			case REQ_FIND_NODE:
				rsp = processFindNode(src, frame);
				break;
			case REQ_FIND_VALUE:
				rsp = processFindValue(src, frame);
				break;
			case RSP_SUCCESS:
				processSuccess(frame);
				return;
			case RSP_IO_ERROR:
				processIOError(frame);
				return;
			default:
				unsupportedMessage(src, frame);
				return;
			}

			ctx.getChannel().write(rsp);
		}

		@Override
		public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
			channels.add(e.getChannel());
			super.channelOpen(ctx, e);
		}

		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
			log.error("Unexpected communication error: {}. Closing connection to: {}", e.getCause()
					.getLocalizedMessage(), e.getChannel().getRemoteAddress());
			log.debug("Failure trace", e.getCause());
			e.getChannel().close();
		}
	}

	// Future wrapper for sending messages with ChannelFutures
	private class MessageFuture implements Future<Frame> {

		private final ChannelFuture future;
		private final Frame frame;
		private final InetSocketAddress address;

		public MessageFuture(ChannelFuture future, Frame frame, InetSocketAddress address) {
			this.future = future;
			this.frame = frame;
			this.address = address;
		}

		@Override
		public boolean cancel(boolean mayInterruptIfRunning) {
			return future.cancel();
		}

		@Override
		public boolean isCancelled() {
			return future.isCancelled();
		}

		@Override
		public boolean isDone() {
			return future.isDone();
		}

		@Override
		public Frame get() throws InterruptedException, ExecutionException {
			try {
				return get(responseTimeOutMs, TimeUnit.MILLISECONDS);
			} catch (TimeoutException e) {
				throw new ExecutionException("Wait interrupted by default timeout: " + responseTimeOutMs + "ms", e);
			}
		}

		@Override
		public Frame get(long timeout, TimeUnit unit) throws TimeoutException {

			try {
				if (!future.await(timeout, unit)) {
					throw new TimeoutException(frame.getType() + " to " + address + " has timed out in "
							+ responseTimeOutMs + " milliseconds");
				}

				Channel channel = future.getChannel();

				try {
					ClientHandler handler = channel.getPipeline().get(ClientHandler.class);
					Frame rsp = handler.sendMessage(frame);
					if (rsp == null) {
						throw new TimeoutException(frame.getType() + " to " + address + " has timed out");
					}
					return rsp;
				} finally {
					channel.close().await(closeTimeOutMs, TimeUnit.MILLISECONDS);
				}
			} catch (InterruptedException e) {
				throw new ShutdownException(e);
			}
		}
	}
}
