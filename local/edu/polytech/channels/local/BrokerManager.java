package edu.polytech.channels.local;

import java.util.HashMap;

import edu.polytech.channels.Broker;

public class BrokerManager {
	private static HashMap<String,Broker> brokers;
	private static final BrokerManager bm=new BrokerManager();
	
	private BrokerManager() {
		brokers=new HashMap<>();
	}
	
	static BrokerManager getInstance() {
		return bm;
	}
	
	void register(Broker b) {
		Broker new_broker=brokers.putIfAbsent(b.getName(), b);
		if(new_broker!=null)
			throw new IllegalArgumentException("A broker named"+ b.getName() + "already exist");
	}
	
	Broker find(String name) {
		return brokers.get(name);
		
		}
}
