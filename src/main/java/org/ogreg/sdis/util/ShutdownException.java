package org.ogreg.sdis.util;

/**
 * Thrown when a thread is stopping because of an interrupt.
 * <p>
 * This usually happens when the application is forcefully shut down, and some threads are still waiting for IO. By
 * convention this is the only use-case, so error handling is not necessary for this exception.
 * 
 * @author gergo
 */
public class ShutdownException extends RuntimeException {
	private static final long serialVersionUID = 1782438929875474411L;

	public ShutdownException(InterruptedException cause) {
		super("Application is shutting down", cause);
	}
}
