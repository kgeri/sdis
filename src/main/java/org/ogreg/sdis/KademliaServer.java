package org.ogreg.sdis;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.protobuf.ProtobufDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufEncoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import org.ogreg.sdis.messages.Kademlia;
import org.ogreg.sdis.messages.Kademlia.Message;
import org.ogreg.sdis.messages.Kademlia.Message.Builder;
import org.ogreg.sdis.messages.Kademlia.Node;
import org.ogreg.sdis.messages.Kademlia.Response;
import org.ogreg.sdis.storage.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

/**
 * The service endpoint listening for incoming requests from the P2P network.
 * 
 * @author gergo
 */
public class KademliaServer {

	private static final Logger log = LoggerFactory.getLogger(KademliaServer.class);

	/**
	 * The port on which the server listens.
	 */
	private int port;

	/**
	 * The storage service used for loading and saving blocks of data.
	 */
	private StorageService store;

	/**
	 * Starts listening on {@link #port}.
	 */
	public void start() {
		ServerBootstrap bootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(
				Executors.newCachedThreadPool(), Executors.newCachedThreadPool()));
		bootstrap.setPipelineFactory(new KademliaPipelineFactory());
		bootstrap.bind(new InetSocketAddress(port));
	}

	public void setPort(int port) {
		this.port = port;
	}

	// TODO util class?
	private static BinaryKey getKey(Message msg) {
		// TODO assert has key
		return new BinaryKey(msg.getKey().toByteArray());
	}

	private static ByteString getData(Message msg) {
		// TODO assert has data
		return msg.getData();
	}

	private class KademliaPipelineFactory implements ChannelPipelineFactory {

		@Override
		public ChannelPipeline getPipeline() throws Exception {
			ChannelPipeline p = Channels.pipeline();
			p.addLast("frameDecoder", new ProtobufVarint32FrameDecoder());
			p.addLast("protobufDecoder", new ProtobufDecoder(Kademlia.Message.getDefaultInstance()));
			p.addLast("frameEncoder", new ProtobufVarint32LengthFieldPrepender());
			p.addLast("protobufEncoder", new ProtobufEncoder());
			p.addLast("handler", new KademliaHandler());
			return p;
		}
	}

	private class KademliaHandler extends SimpleChannelUpstreamHandler {

		@Override
		public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
			Message req = (Message) e.getMessage();

			ByteString nodeId = null;
			ByteString rpcId = null;

			Builder builder = Message.newBuilder().setNodeId(nodeId).setRpcId(rpcId);

			if (req.hasRequestType()) {
				BinaryKey key;

				// Processing requests
				switch (req.getRequestType()) {
				case PING:
					builder.setResponseType(Response.SUCCESS);
					break;
				case STORE:
					key = getKey(req);
					ByteString data = getData(req);
					// TODO check key
					// TODO check data
					store.store(key, data.asReadOnlyByteBuffer());
					break;
				case FIND_NODE:
					key = getKey(req);
					// TODO get closest nodes
					builder.addNodes((Node) null);
					break;
				case FIND_VALUE:
					key = getKey(req);
					ByteBuffer dataBuffer = store.load(key);
					if (dataBuffer != null) {
						builder.setData(ByteString.copyFrom(dataBuffer));
					} else {
						// TODO get closest nodes
						builder.addNodes((Node) null);
					}
					break;
				default:
					unsupportedMessage(e, req);
					return;
				}
			} else if (req.hasResponseType()) {

				// Processing responses
				switch (req.getResponseType()) {
				case SUCCESS:
					// TODO process success result based on rpc id
					break;
				case IO_ERROR:
					// TODO process error result based on rpc id
					break;
				default:
					unsupportedMessage(e, req);
					return;
				}
			} else {
				unsupportedMessage(e, req);
				return;
			}

			ctx.getChannel().write(builder.build());
		}

		private void unsupportedMessage(MessageEvent event, Message request) {
			log.error("Unsupported message from: {} ({})", event.getRemoteAddress(), request);
		}
	}
}
