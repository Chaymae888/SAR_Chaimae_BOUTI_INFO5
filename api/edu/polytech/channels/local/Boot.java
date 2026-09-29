package edu.polytech.channels.local;

import edu.polytech.channels.Bootstrap;
import edu.polytech.channels.Broker;
import edu.polytech.channels.Task;

public class Boot implements Bootstrap {
	
	
	public Boot() {};

	@Override
	public Broker newBroker(String name) {
		return new CBroker(name);
	}

	@Override
	public Task newTask(Broker b, Runnable r, String name) {
		CTask t = new CTask(b, r, name);
	    t.start();
	    return t;
	}

}
