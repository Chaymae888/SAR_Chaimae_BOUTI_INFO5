package edu.polytech.channels.local;

import edu.polytech.utils.CircularBuffer;

public class ChannelHelper {

		  static final int DEFAULT_CAPACITY = 8192;

		  private final CircularBuffer aToB;
		  private final CircularBuffer bToA;

		  private boolean aDisconnected = false;
		  private boolean bDisconnected = false;

		  ChannelHelper() {
		    this(DEFAULT_CAPACITY);
		  }

		  ChannelHelper(int capacity) {
		    aToB = new CircularBuffer(capacity);
		    bToA = new CircularBuffer(capacity);
		  }

		  private static void checkRange(byte[] bytes, int offset, int length) {
		    if (bytes == null)
		      throw new IllegalArgumentException("bytes array is null");
		    if (offset < 0 || length < 0 || offset + length > bytes.length)
		      throw new IllegalArgumentException(
		          "invalid range: offset=" + offset + ", length=" + length + ", array length=" + bytes.length);
		  }

		  private boolean localDisconnected(boolean isA) {
		    return isA ? aDisconnected : bDisconnected;
		  }

		  private boolean remoteDisconnected(boolean isA) {
		    return isA ? bDisconnected : aDisconnected;
		  }

		  private CircularBuffer incoming(boolean isA) {
		    return isA ? bToA : aToB;
		  }

		  /** The buffer this end point writes to. */
		  private CircularBuffer outgoing(boolean isA) {
		    return isA ? aToB : bToA;
		  }

		  synchronized boolean disconnected(boolean isA) {
		    return localDisconnected(isA) || (remoteDisconnected(isA) && incoming(isA).empty());
		  }
		  
		  synchronized void disconnect(boolean isA) {
		    if (localDisconnected(isA))
		      return;
		    if (isA)
		      aDisconnected = true;
		    else
		      bDisconnected = true;
		    notifyAll();
		  }

		  synchronized int read(boolean isA, byte[] bytes, int offset, int length) {
		    checkRange(bytes, offset, length);
		    if (length == 0)
		      return 0;
		    CircularBuffer in = incoming(isA);
		    while (!localDisconnected(isA) && in.empty() && !remoteDisconnected(isA)) {
		    	try {
				      wait();
				    } catch (InterruptedException e) {
				      Thread.currentThread().interrupt();
				    }
		    }
		    if (localDisconnected(isA)) {
		      return 0;
		    }
		    if (in.empty()) {
		      return 0;
		    }
		    int n = 0;
		    while (n < length && !in.empty()) {
		      bytes[offset + n] = in.pull();
		      n++;
		    }
		    notifyAll();
		    return n;
		  }

		  synchronized int write(boolean isA, byte[] bytes, int offset, int length) {
		    checkRange(bytes, offset, length);
		    if (length == 0)
		      return 0;
		    if (localDisconnected(isA) || remoteDisconnected(isA)) {
		      return length;
		    }
		    CircularBuffer out = outgoing(isA);
		    while (out.full() && !localDisconnected(isA) && !remoteDisconnected(isA)) {
		    	try {
				      wait();
				    } catch (InterruptedException e) {
				      Thread.currentThread().interrupt();
				    }
		    }
		    if (localDisconnected(isA) || remoteDisconnected(isA)) {
		      // Became disconnected while blocked: drop whatever remains.
		      return length;
		    }
		    int n = 0;
		    while (n < length && !out.full()) {
		      out.push(bytes[offset + n]);
		      n++;
		    }
		    notifyAll();
		    return n;
		  }

		  
	

}
