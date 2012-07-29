package org.ogreg.sdis.kademlia;

import static org.ogreg.sdis.kademlia.Protocol.MessageType.REQ_PING;
import static org.ogreg.sdis.kademlia.Protocol.MessageType.RSP_IO_ERROR;
import static org.ogreg.sdis.kademlia.Protocol.MessageType.RSP_SUCCESS;
import static org.ogreg.sdis.kademlia.Util.message;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelException;
import org.jboss.netty.channel.ChannelFactory;
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
import org.ogreg.sdis.kademlia.Conversations.IterativeFindValueConversation;
import org.ogreg.sdis.kademlia.Conversations.IterativeStoreConversation;
import org.ogreg.sdis.kademlia.Conversations.SingleFrameConversation;
import org.ogreg.sdis.kademlia.Protocol.Message;
import org.ogreg.sdis.kademlia.Protocol.Message.Builder;
import org.ogreg.sdis.kademlia.Protocol.MessageType;
import org.ogreg.sdis.kademlia.Protocol.Node;
import org.ogreg.sdis.model.BinaryKey;
import org.ogreg.sdis.util.Conversations;
import org.ogreg.sdis.util.Properties;
import org.ogreg.sdis.util.ProtobufFrameDecoder;
import org.ogreg.sdis.util.ProtobufFrameEncoder;
import org.ogreg.sdis.util.ShutdownException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.JdkFutureAdapters;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;

/**
 * The service endpoint listening for incoming requests from the P2P network.
 * 
 * @author gergo
 */
public class Server extends AbstractService implements P2PService {

	private static final Logger log = LoggerFactory.getLogger(Server.class);

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
	private final StorageService store;

	/**
	 * The current Node's node ID.
	 */
	private final ByteString nodeId;

	/**
	 * The Kademlia routing table used by this node.
	 */
	private final RoutingTable routingTable;

	private final KademliaProperties props;

	private ServerBootstrap server;
	private ClientBootstrap client;

	private ChannelGroup channels = null;
	private InetSocketAddress address = null;
	private ExecutorService executor = null;
	private Conversations<Frame> conversations = null;

	private ReplaceAction updateAction;

	public Server(StorageService store, BinaryKey nodeId, Properties properties) {
		this.props = new KademliaProperties(properties);
		this.store = store;
		this.nodeId = ByteString.copyFrom(nodeId.toByteArray());
		this.routingTable = new RoutingTableFixedImpl(nodeId, props.maxContacts);

		// Update action: if the appropriate k-bucket is full, then the recipient pings the k-bucket’s least-recently
		// seen node to decide what to do
		updateAction = new ReplaceAction() {
			@Override
			public Contact replace(Contact leastRecent, Contact newContact) {
				// TODO Async replace
				boolean success = false;
				try {
					Message message = message(REQ_PING, Server.this.nodeId, leastRecent.address,
							Util.generateByteStringId()).build();
					Frame frame = new Frame(message);
					ListenableFuture<Frame> future = sendMessageASync(frame, address);
					Frame response = future.get(props.responseTimeOutMs, TimeUnit.MILLISECONDS);
					success = response.getMessage().getType().equals(RSP_SUCCESS);
				} catch (TimeoutException e) {
					log.debug("Ping timed out for: {}", leastRecent);
				} catch (InterruptedException e) {
					throw new ShutdownException(e);
				} catch (ExecutionException e) {
					log.error("Ping FAILED for: " + leastRecent, e.getCause());
				}

				// If the least-recently seen node responds, the new contact is discarded
				// If the least-recently seen node fails to respond, the new contact is added
				return success ? leastRecent : newContact;
			}
		};
	}

	@Override
	protected void doStart() {
		executor = Executors.newCachedThreadPool();

		ChannelFactory serverChannelFactory = new NioServerSocketChannelFactory(executor, executor, props.serverThreads);
		server = new ServerBootstrap(serverChannelFactory);
		server.setOption("connectTimeoutMillis", props.responseTimeOutMs);
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

		ChannelFactory clientChannelFactory = new NioClientSocketChannelFactory(executor, executor, props.clientThreads);
		client = new ClientBootstrap(clientChannelFactory);
		client.setOption("connectTimeoutMillis", props.responseTimeOutMs);
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
		conversations = new Conversations<Frame>(props.responseTimeOutMs);

		for (int port = portFrom; port <= portTo; port++) {
			try {
				address = new InetSocketAddress(port);
				server.bind(address);
				log.info("Kademlia server started at {}", address);
				notifyStarted();
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

	@Override
	protected void doStop() {
		channels.close().awaitUninterruptibly();
		conversations.cancelAll();
		client.releaseExternalResources();
		server.releaseExternalResources();

		log.info("Kademlia server stopped at {}", address);

		channels = null;
		conversations = null;
		address = null;
		executor.shutdownNow();
		executor = null;
		notifyStopped();
	}

	@Override
	public void contact(InetSocketAddress address) {
		contactASync(address);
	}

	// Package private for testing
	ListenableFuture<Frame> contactASync(InetSocketAddress address) {
		ByteString rpcId = Util.generateByteStringId();
		Frame req = new Frame(message(REQ_PING, nodeId, this.address, rpcId).build());
		// TODO Direct errors to GUI
		return sendMessageASync(req, address);
	}

	// Does an iterativeStore in the Kademlia network
	@Override
	public ListenableFuture<BinaryKey> store(ByteBuffer data) throws TimeoutException {
		// Note: if you change this, make sure data is not modified, neither directly, nor by any side-effect
		BinaryKey key = Util.checksum(data);

		IterativeStoreConversation conversation = new IterativeStoreConversation(client, routingTable, props, nodeId,
				address, key, data);
		conversations.add(conversation).proceed(null, null);

		return JdkFutureAdapters.listenInPoolThread(conversation, executor); // TODO get rid of this
	}

	@Override
	public ListenableFuture<ByteBuffer> load(BinaryKey key) {
		IterativeFindValueConversation conversation = new IterativeFindValueConversation(client, routingTable, props,
				nodeId, address, key);
		conversations.add(conversation).proceed(null, null);

		return JdkFutureAdapters.listenInPoolThread(conversation, executor); // TODO get rid of this
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
	 * @return A future holding the response frame
	 */
	// Package private for testing
	ListenableFuture<Frame> sendMessageASync(final Frame frame, final InetSocketAddress address) {
		SingleFrameConversation conversation = new SingleFrameConversation(client, frame, address);
		conversations.add(conversation).proceed(null, null);
		return JdkFutureAdapters.listenInPoolThread(conversation, executor); // TODO get rid of this
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
		return new Frame(message(RSP_SUCCESS, this.nodeId, this.address, request.getMessage().getRpcId()).build());
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
		Message req = request.getMessage();
		BinaryKey key = Util.ensureHasKey(req);
		// TODO 2-phase STORE (first signal if we already have the data, for performance)
		ChannelBuffer data = Util.ensureHasData(request);

		// TODO Make sure that this does not copy the contents
		ByteBuffer buffer = data.toByteBuffer();
		BinaryKey computedKey = Util.checksum(buffer);

		Message message;
		if (computedKey.equals(key)) {
			store.store(key, buffer);
			message = message(RSP_SUCCESS, nodeId, this.address, req.getRpcId()).build();
		} else {
			log.error("Data chunk cheksum mismatch (" + key + " != " + computedKey + ")");
			message = message(RSP_IO_ERROR, nodeId, this.address, req.getRpcId()).build();
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
		Message req = request.getMessage();
		BinaryKey key = Util.ensureHasKey(req);
		Builder builder = message(RSP_SUCCESS, this.nodeId, this.address, req.getRpcId());

		Collection<Contact> closestContacts = routingTable.getClosestTo(key, props.maxContacts);
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
		Message req = request.getMessage();
		BinaryKey key = Util.ensureHasKey(req);
		ByteBuffer dataBuffer = store.load(key);
		if (dataBuffer != null) {
			Message message = message(RSP_SUCCESS, nodeId, this.address, req.getRpcId()).build();
			return new Frame(message, ChannelBuffers.wrappedBuffer(dataBuffer));
		} else {
			return processFindNode(requestor, request);
		}
	}

	private void unsupportedMessage(Contact contact, Frame request) {
		log.error("Unsupported message from: {} ({})", contact, request);
	}

	// The client handler for protobuf Kademlia messages
	private class ClientHandler extends SimpleChannelUpstreamHandler {

		@Override
		public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
			channels.add(e.getChannel());
			super.channelOpen(ctx, e);
		}

		@Override
		public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
			Frame frame = (Frame) e.getMessage();
			Message rsp = frame.getMessage();

			Contact src = Util.toContact(rsp.getNodeId(), rsp.getAddress(), rsp.getPort());
			onMessageReceived(src, frame);

			conversations.get(rsp.getRpcId()).proceed(frame, src.address);
			e.getChannel().close();
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
}
