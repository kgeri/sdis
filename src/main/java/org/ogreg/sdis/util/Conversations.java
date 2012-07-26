package org.ogreg.sdis.util;

import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.ogreg.sdis.CommonUtil;

import com.google.protobuf.ByteString;

/**
 * Data structure for managing {@link Conversation}s - one stateful object per RPC id.
 * 
 * @author gergo
 * @param <M>
 *            The message type used by the conversations
 */
public class Conversations<M> {

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private static final Conversation EmptyConversation = new Conversation(null) {
		@Override
		protected Enum proceed(Enum state, Object message, InetSocketAddress address) {
			return state;
		}
	};

	private final ConcurrentMap<ByteString, Conversation<M, ?>> conversations = new ConcurrentHashMap<ByteString, Conversation<M, ?>>();

	/** The maximum number of milliseconds to wait for a conversation. */
	private final long timeoutMs;

	public Conversations(long timeoutMs) {
		this.timeoutMs = timeoutMs;
	}

	/**
	 * Adds the specified conversation.
	 * 
	 * @param id
	 *            The conversation's identifier (RPC id)
	 * @param conversation
	 *            The conversation to begin
	 */
	public Conversation<M, ?> add(ByteString id, Conversation<M, ?> conversation) {
		Conversation<M, ?> previous = conversations.putIfAbsent(id, conversation);

		if (previous != null && previous != conversation) {
			throw new IllegalStateException("Conversation id should not be reused: "
					+ CommonUtil.toHexString(id.toByteArray()));
		}

		conversation.init(id, this);
		return conversation;
	}

	/**
	 * Gets the specified conversation.
	 * 
	 * @param id
	 *            The conversation's identifier (RPC id)
	 * @return The conversation for the specified id, or the {@link #EmptyConversation}
	 */
	@SuppressWarnings("unchecked")
	public Conversation<M, ?> get(ByteString id) {
		Conversation<M, ?> conversation = conversations.get(id);
		return conversation == null ? EmptyConversation : conversation;
	}

	/**
	 * Cancels all conversations, notifying their listeners.
	 */
	public void cancelAll() {
		CancellationException ce = new CancellationException();
		for (Iterator<Conversation<M, ?>> it = conversations.values().iterator(); it.hasNext();) {
			Conversation<M, ?> conversation = it.next();
			conversation.finishImpl(null, ce);
			it.remove();
		}
	}

	/**
	 * Base class for asynchronous conversations.
	 * 
	 * @author gergo
	 * @param <M>
	 *            The message type used by the conversations
	 * @param <V>
	 *            The type of the end result of the conversation
	 */
	public static abstract class Conversation<M, V> implements Future<V> {

		private final Enum<?> initState;

		private Enum<?> state;

		private boolean done;

		private V value;

		private ByteString id;

		private Conversations<M> parent;

		private Throwable error;

		public Conversation(Enum<?> initState) {
			this.initState = initState;
		}

		protected synchronized void init(ByteString id, Conversations<M> parent) {
			this.id = id;
			this.parent = parent;
			this.error = null;
			this.state = initState;
			this.done = false;
		}

		public synchronized void proceed(M message, InetSocketAddress address) {
			state = proceed(state, message, address);
		}

		protected abstract Enum<?> proceed(Enum<?> state, M message, InetSocketAddress address);

		protected void succeed(V value) {
			finishImpl(value, null);
			parent.conversations.remove(id);
		}

		protected final void fail(Throwable error) {
			finishImpl(null, error);
			parent.conversations.remove(id);
		}

		private synchronized void finishImpl(V value, Throwable error) {
			this.done = true;
			this.value = value;
			this.error = error;
			notifyAll();
		}

		@Override
		public synchronized boolean cancel(boolean mayInterruptIfRunning) {
			if (done) {
				return false;
			}
			fail(new CancellationException());
			return true;
		}

		@Override
		public synchronized boolean isCancelled() {
			return error instanceof CancellationException;
		}

		@Override
		public synchronized boolean isDone() {
			return done;
		}

		@Override
		public synchronized V get() throws InterruptedException, ExecutionException {
			while (!done) {
				wait(10);
			}
			if (error != null)
				throw new ExecutionException(error);
			return value;
		}

		@Override
		public synchronized final V get(long duration, TimeUnit unit) throws InterruptedException, ExecutionException {
			if (done) {
				if (error != null)
					throw new ExecutionException(error);
				return value;
			}
			long startedMs = System.currentTimeMillis();
			long timeoutMs = unit.toMillis(duration);
			while (true) {
				wait(timeoutMs);
				if (done) {
					if (error != null)
						throw new ExecutionException(error);
					return value;
				}
				timeoutMs -= System.currentTimeMillis() - startedMs;
				if (timeoutMs <= 0) {
					throw new ExecutionException(new TimeoutException());
				}
			}
		}
	}

	public static abstract class NettyConversation<M, V> extends Conversation<M, V> {

		private final ClientBootstrap client;

		public NettyConversation(ClientBootstrap client, Enum<?> initState) {
			super(initState);
			this.client = client;
		}

		protected void send(final M message, final InetSocketAddress address) {

			ChannelFutureListener listener = new ChannelFutureListener() {
				private volatile boolean connected = false;

				@Override
				public void operationComplete(ChannelFuture future) throws Exception {
					if (!connected) {
						if (!future.isSuccess()) {
							onConnectionFailed(address, future.getCause());
							fail(future.getCause());
							return;
						}

						connected = true;
						Channel channel = future.getChannel();
						channel.write(message).addListener(this);
					} else {
						if (!future.isSuccess()) {
							onSendFailed(address, message, future.getCause());
							fail(future.getCause());
							return;
						}

						onSendSucceeded(address, message);
					}
				}
			};

			client.connect(address).addListener(listener);
		}

		protected void onConnectionFailed(InetSocketAddress address, Throwable cause) {
		}

		protected void onSendFailed(InetSocketAddress address, M message, Throwable cause) {
		}

		protected void onSendSucceeded(InetSocketAddress address, M message) {
		}
	}
}
