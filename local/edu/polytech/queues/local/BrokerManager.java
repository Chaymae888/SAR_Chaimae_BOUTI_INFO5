package edu.polytech.queues.local;

import java.util.HashMap;
import java.util.Map;

/**
 * Naming registry shared by every {@link CQueueBroker} created in this
 * runtime, so that {@code connect()} can resolve a broker name into the
 * actual instance, wherever it was created.
 * <p>
 * No synchronization is needed here: the whole runtime is driven by a
 * single {@code Executor} thread (see {@code edu.polytech.utils.Executor}),
 * so two brokers are never registered or looked up concurrently. A plain
 * {@link HashMap} is therefore enough, unlike the channel ("L1") runtime
 * which really did have one thread per task.
 */
final class BrokerManager {

  private BrokerManager() {
  }

  private static final Map<String, CQueueBroker> brokers = new HashMap<>();

  static void register(CQueueBroker broker) {
    CQueueBroker previous = brokers.putIfAbsent(broker.getName(), broker);
    if (previous != null && previous != broker)
      throw new IllegalArgumentException("A queue broker named '" + broker.getName() + "' already exists.");
  }

  static CQueueBroker find(String name) {
    return brokers.get(name);
  }
}
