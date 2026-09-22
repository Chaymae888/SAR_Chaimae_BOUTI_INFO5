package edu.polytech.channels.local;

import java.util.HashMap;

import edu.polytech.channels.Broker;
import edu.polytech.channels.Channel;

public class CBroker implements Broker {
	private final String name;
	private final BrokerManager bm=BrokerManager.getInstance();
	private final HashMap<Integer,RendezVous> rdvs=new HashMap<>();
	
	public CBroker(String name) {
		this.name=name;
		bm.register(this);
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public Channel connect(String name, int port) {
		Broker broker_distant = bm.find(name);
		if (broker_distant == null) return null; 
		RendezVous rdv =((CBroker) broker_distant).rdvs.computeIfAbsent((Integer) port, p -> new RendezVous());
		return rdv.connect();
	}

	@Override
	public Channel accept(int port) {
		RendezVous rdv =rdvs.computeIfAbsent((Integer) port, p -> new RendezVous());
		return rdv.accept();
	}

}
