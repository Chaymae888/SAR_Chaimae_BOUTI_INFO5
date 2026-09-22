package edu.polytech.channels.local;

import edu.polytech.channels.Channel;
import edu.polytech.utils.CircularBuffer;

public class CChannel implements Channel {
	private final ChannelHelper helper;
	  private final boolean isA;

	  /**
	   * @param isA true for the end point created by {@code connect()},
	   *            false for the end point created by {@code accept()}.
	   */
	  CChannel(ChannelHelper helper, boolean isA) {
	    this.helper = helper;
	    this.isA = isA;
	  }

	  @Override
	  public int read(byte[] bytes, int offset, int length) {
	    return helper.read(isA, bytes, offset, length);
	  }

	  @Override
	  public int write(byte[] bytes, int offset, int length) {
	    return helper.write(isA, bytes, offset, length);
	  }

	  @Override
	  public boolean disconnected() {
	    return helper.disconnected(isA);
	  }

	  @Override
	  public void disconnect() {
	    helper.disconnect(isA);
	  }

}
