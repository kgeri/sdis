package org.ogreg.sdis.util;

import org.jboss.netty.buffer.ChannelBuffer;

import com.google.protobuf.MessageLite;

/**
 * Factory interface for {@link ProtobufFrame}s.
 * 
 * @author gergo
 */
public interface ProtobufFrameFactory<F extends ProtobufFrame<M>, M extends MessageLite> {

	/**
	 * @param message
	 * @return A new {@link ProtobufFrame}
	 */
	F newInstance(M message);

	/**
	 * @param message
	 * @param data
	 * @return A new {@link ProtobufFrame}
	 */
	F newInstance(M message, ChannelBuffer data);
}
