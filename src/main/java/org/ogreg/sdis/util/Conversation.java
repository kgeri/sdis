package org.ogreg.sdis.util;

import org.ogreg.sdis.kademlia.Frame;

import com.google.common.util.concurrent.SettableFuture;

/**
 * Value object for asynchronous conversations.
 * 
 * @author gergo
 * 
 * @param <T>
 *            The message type
 */
public class Conversation<T> {

	private final T request;

	private final SettableFuture<Frame> response;

	public Conversation(T request, SettableFuture<Frame> response) {
		super();
		this.request = request;
		this.response = response;
	}

	public T getRequest() {
		return request;
	}

	public SettableFuture<Frame> getResponse() {
		return response;
	}
}
