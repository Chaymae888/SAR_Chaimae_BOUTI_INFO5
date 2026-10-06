package edu.polytech.channels.local;

import edu.polytech.channels.Broker;
import edu.polytech.channels.Task;

public class CTask extends Task {
	
	private final Broker broker;
	private final Runnable runnable;

	protected CTask(Broker b,Runnable r,String name) {
		super(name);
		this.broker=b;
		this.runnable=r;
	}

	@Override
	public Broker getBroker() {
		return broker;
	}

	@Override
	public boolean alive() {
		return isAlive();
	}

	@Override
	public boolean dead() {
		return getState()==State.TERMINATED;
	}

	@Override
	public Broker newBroker(String name) {
		return new CBroker(name);
	}

	@Override
	public Task newTask(Broker b, Runnable r, String name) {
		Task task= new CTask(b,r,name);
		task.start();
		return task;
	}
	

}
