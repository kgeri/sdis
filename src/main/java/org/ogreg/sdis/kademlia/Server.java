package org.ogreg.sdis.kademlia;

import static org.ogreg.sdis.kademlia.Protocol.MessageType.REQ_PING;
import static org.ogreg.sdis.kademlia.Protocol.MessageType.RSP_IO_ERROR;
import static org.ogreg.sdis.kademlia.Protocol.MessageType.RSP_SUCCESS;
import static org.ogreg.sdis.kademlia.Util.message;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ServerBootstrap;
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
import org.jboss.netty.handler.codec.protobuf.ProtobufDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufEncoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import org.ogreg.sdis.P2PService;
import org.ogreg.sdis.StorageService;
import org.ogreg.sdis.kademlia.Protocol.Message;
import org.ogreg.sdis.kademlia.Protocol.Message.Builder;
import org.ogreg.sdis.kademlia.Protocol.MessageType;
import org.ogreg.sdis.kademlia.Protocol.Node;
import org.ogreg.sdis.model.BinaryKey;
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
	private static final int RESPONSE_TIMEOUT_MS = 10000;

	/**
	 * The number of milliseconds to wait before a connection close attempt times out.
	 */
	private static final int CLOSE_TIMEOUT_MS = 3000;

	/**
	 * The maximum number of contacts stored in a k-bucket (k).
	 */
	private static final int MAX_CONTACTS = 20;

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

	private ServerBootstrap server;
	private ClientBootstrap client;

	private ChannelGroup channels = null;
	private InetSocketAddress address = null;

	private ReplaceAction updateAction;

	public Server(StorageService store, BinaryKey nodeId) {
		this.store = store;
		this.nodeId = ByteString.copyFrom(nodeId.toByteArray());
		this.routingTable = new RoutingTableFixedImpl(nodeId, MAX_CONTACTS);

		// Update action: if the appropriate k-bucket is full, then the recipient pings the k-bucket’s least-recently
		// seen node to decide what to do
		updateAction = new ReplaceAction() {
			@Override
			public Contact replace(Contact leastRecent, Contact newContact) {
				if (sendPingSync(leastRecent.address)) {
					// If the least-recently seen node responds, the new contact is discarded
					return leastRecent;
				} else {
					// If the least-recently seen node fails to respond, the new contact is added
					return newContact;
				}
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

		NioServerSocketChannelFactory serverChannelFactory = new NioServerSocketChannelFactory(
				Executors.newCachedThreadPool(), Executors.newCachedThreadPool());
		server = new ServerBootstrap(serverChannelFactory);
		server.setPipelineFactory(new ChannelPipelineFactory() {
			@Override
			public ChannelPipeline getPipeline() throws Exception {
				ChannelPipeline p = Channels.pipeline();
				p.addLast("frameDecoder", new ProtobufVarint32FrameDecoder());
				p.addLast("protobufDecoder", new ProtobufDecoder(Message.getDefaultInstance()));
				p.addLast("frameEncoder", new ProtobufVarint32LengthFieldPrepender());
				p.addLast("protobufEncoder", new ProtobufEncoder());
				p.addLast("handler", new ServerHandler());
				return p;
			}
		});

		NioClientSocketChannelFactory clientChannelFactory = new NioClientSocketChannelFactory(
				Executors.newCachedThreadPool(), Executors.newCachedThreadPool());
		client = new ClientBootstrap(clientChannelFactory);
		client.setPipelineFactory(new ChannelPipelineFactory() {
			public ChannelPipeline getPipeline() throws Exception {
				ChannelPipeline p = Channels.pipeline();
				p.addLast("frameDecoder", new ProtobufVarint32FrameDecoder());
				p.addLast("protobufDecoder", new ProtobufDecoder(Message.getDefaultInstance()));
				p.addLast("frameEncoder", new ProtobufVarint32LengthFieldPrepender());
				p.addLast("protobufEncoder", new ProtobufEncoder());
				p.addLast("handler", new ClientHandler());
				return p;
			}
		});

		channels = new DefaultChannelGroup("kademlia");

		for (int port = portFrom; port <= portTo; port++) {
			try {
				address = new InetSocketAddress(port);
				server.bind(address);
				break;
			} catch (ChannelException e) {
				if (port < portTo) {
					log.warn("Failed to bind {}, trying next in range ({} - {})",
							new Object[] { port, portFrom, portTo });
				} else {
					log.error("Failed to bind port range ({} - {})", portFrom, portTo);
					address = null;
					channels = null;
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
			channels = null;
		}
	}

	@Override
	public void add(InetSocketAddress address) {
		sendPingSync(address);
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

	// The bound server address, or null if the server is not bound.
	InetSocketAddress getAddress() {
		return address;
	}

	// The nodeId of this server.
	ByteString getNodeId() {
		return nodeId;
	}

	/**
	 * Sends a {@link MessageType#REQ_PING} to <code>address</code> synchronously.
	 * 
	 * @param address
	 * 
	 * @return true if the remote node responded successfully
	 */
	private boolean sendPingSync(InetSocketAddress address) {
		Message message = message(REQ_PING, nodeId).build();
		try {
			Message response = sendMessageSync(message, address);
			return response.getType().equals(RSP_SUCCESS);
		} catch (TimeoutException e) {
			log.debug("{}", e.getLocalizedMessage());
		} catch (InterruptedException e) {
			log.debug("{} was interrupted", REQ_PING);
		}
		return false;
	}

	private void onMessageReceived(Message req, InetSocketAddress fromAddress) {
		// When a Kademlia node receives any message from another node, it updates the appropriate k-bucket for the
		// sender’s node ID
		Contact contact = Util.toContact(req.getNodeId(), fromAddress);

		// Updating the routing table with the new contact
		routingTable.update(contact, updateAction);
	}

	/**
	 * Sends the <code>message</code> to <code>address</code> synchronously, and returns the response.
	 * 
	 * @param message
	 * @param address
	 * 
	 * @return The response received
	 * @throws TimeoutException
	 *             if the request has timed out
	 * @throws InterruptedException
	 *             if the request was interrupted
	 */
	// Package private for unit testing
	Message sendMessageSync(Message message, InetSocketAddress address) throws TimeoutException, InterruptedException {

		ChannelFuture future = client.connect(address);

		if (!future.await(RESPONSE_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
			throw new TimeoutException(message.getType() + " to " + address + " has timed out in "
					+ RESPONSE_TIMEOUT_MS + " milliseconds");
		}

		Channel channel = future.getChannel();

		try {
			ClientHandler handler = channel.getPipeline().get(ClientHandler.class);
			return handler.sendMessage(message);
		} finally {
			channel.close().await(CLOSE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
		}
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
	 * @param req
	 * @return
	 */
	private Message processPing(Message req) {
		return message(RSP_SUCCESS, nodeId).build();
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
	 * @param req
	 * @return
	 */
	private Message processStore(Message req) {
		BinaryKey key = Util.ensureHasKey(req);
		ByteString data = Util.ensureHasData(req);

		BinaryKey computedKey = Util.checksum(data);

		if (computedKey.equals(key)) {
			store.store(key, data.asReadOnlyByteBuffer());
			return message(RSP_SUCCESS, nodeId).build();
		} else {
			log.error("Data chunk cheksum mismatch (" + key + " != " + computedKey + ")");
			return message(RSP_IO_ERROR, nodeId).build();
		}
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
	 * @param req
	 * @return
	 */
	private Message processFindNode(Message req) {
		BinaryKey key = Util.ensureHasKey(req);
		Builder builder = message(RSP_SUCCESS, nodeId);
		Collection<Contact> closestContacts = routingTable.getClosestTo(key, MAX_CONTACTS);
		for (Contact contact : closestContacts) {
			Node node = Util.toNode(contact);
			builder.addNodes(node);
		}
		return builder.build();
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
	 * @param req
	 * @return
	 */
	private Message processFindValue(Message req) {
		BinaryKey key = Util.ensureHasKey(req);
		ByteBuffer dataBuffer = store.load(key);
		if (dataBuffer != null) {
			return message(RSP_SUCCESS, nodeId).setData(ByteString.copyFrom(dataBuffer)).build();
		} else {
			return processFindNode(req);
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
	private void processSuccess(Message req) {
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
	private void processIOError(Message req) {
		// TODO process IO error result based on rpc id
	}

	private void unsupportedMessage(MessageEvent event, Message request) {
		log.error("Unsupported message from: {} ({})", event.getRemoteAddress(), request);
	}

	// The client handler for protobuf Kademlia messages
	private class ClientHandler extends SimpleChannelUpstreamHandler {

		private final BlockingQueue<Message> responses = new LinkedBlockingQueue<Message>();

		private volatile Channel channel;

		/**
		 * Sends a Kademlia message synchronously and returns the response received.
		 * 
		 * @return The response received from the server, or null if there was a timeout
		 * @throws InterruptedException
		 *             if the waiting for the response was interrupted
		 */
		public Message sendMessage(Message message) throws InterruptedException {
			channel.write(message);
			return responses.poll(Server.RESPONSE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
		}

		@Override
		public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
			channels.add(e.getChannel());
			channel = e.getChannel();
			super.channelOpen(ctx, e);
		}

		@Override
		public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
			responses.offer((Message) e.getMessage());
		}

		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
			log.error("Unexpected communication error", e);
			e.getChannel().close();
		}
	}

	// The server handler for protobuf Kademlia messages
	private class ServerHandler extends SimpleChannelUpstreamHandler {

		@Override
		public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
			Message req = (Message) e.getMessage();

			onMessageReceived(req, (InetSocketAddress) e.getRemoteAddress());

			Message rsp;

			// Processing messages
			switch (req.getType()) {
			case REQ_PING:
				rsp = processPing(req);
				break;
			case REQ_STORE:
				rsp = processStore(req);
				break;
			case REQ_FIND_NODE:
				rsp = processFindNode(req);
				break;
			case REQ_FIND_VALUE:
				rsp = processFindValue(req);
				break;
			case RSP_SUCCESS:
				processSuccess(req);
				return;
			case RSP_IO_ERROR:
				processIOError(req);
				return;
			default:
				unsupportedMessage(e, req);
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
			log.error("Unexpected communication error", e);
			e.getChannel().close();
		}
	}
}
