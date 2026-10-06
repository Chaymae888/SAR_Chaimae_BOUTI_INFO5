package edu.polytech.channels.local;

import java.util.concurrent.SynchronousQueue;

import edu.polytech.channels.Channel;

public class RendezVous {
	private final SynchronousQueue<ChannelHelper>queue = new SynchronousQueue<>(true);
	private volatile boolean accepting= false;
	
	Channel accept() {
		synchronized(this) {
			if (accepting) throw new IllegalStateException("Only one task can accept connection in this port in this broker at a time");
			accepting = true;
			
		}
		
		try {
			ChannelHelper channel=queue.take();
			return new CChannel(channel,false);
			
		} catch (InterruptedException e) {
		      Thread.currentThread().interrupt();
		      throw new RuntimeException("accept() was interrupted", e);
		    } finally {
		      accepting = false;
		    }
	}
	
	Channel connect() {
		ChannelHelper channel = new ChannelHelper();
		try {
			queue.put(channel);
		}catch(InterruptedException e){
			throw new RuntimeException("connect was interrupted", e);
			
		}
		return new CChannel(channel,true);
	}

}
