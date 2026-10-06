package edu.polytech.queues.local;

import edu.polytech.queues.Bootstrap;
import edu.polytech.queues.Task;
import edu.polytech.utils.Executor;

/**
 * Bootstrap class for the local (single-JVM) implementation of the queue
 * specification. As required, it has a public, no-argument constructor
 * (loaded by reflection by the test harness, default name
 * {@code edu.polytech.queues.local.Boot}).
 */
public class Boot implements Bootstrap {

  public Boot() {
  }

  @Override
  public Task newTask(Runnable r, String name) {
    Task t = Executor.self().newTask(name);
    t.post(r);
    return t;
  }
}
