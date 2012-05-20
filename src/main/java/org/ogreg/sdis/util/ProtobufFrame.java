package org.ogreg.sdis.util;

import org.jboss.netty.buffer.ChannelBuffer;

import com.google.protobuf.MessageLite;

/**
 * A Protobuf message frame.
 * <p>
 * Immutable VO which will hold a Protobuf message, and optionally a large data buffer.
 * 
 * @author gergo
 */
public class ProtobufFrame<T extends MessageLite> {

	protected final T message;
	protected ChannelBuffer data = null;

	/**
	 * Creates a new frame with the specified message and without data.
	 * 
	 * @param message
	 */
	public ProtobufFrame(T message) {
		this.message = message;
	}

	/**
	 * Creates a new frame with the specified message and data buffer.
	 * <p>
	 * Important: the buffer must be Big-Endian!
	 * 
	 * @param message
	 * @param data
	 */
	public ProtobufFrame(T message, ChannelBuffer data) {
		this.message = message;
		this.data = data;
	}

	public boolean hasData() {
		return data != null;
	}

	public ChannelBuffer getData() {
		return data;
	}

	public T getMessage() {
		return message;
	}

	@Override
	public String toString() {
		return message.toString();
	}
}
