package edu.polytech.queues.local;

import java.util.HashMap;
import java.util.Map;

import edu.polytech.queues.QueueBroker;
import edu.polytech.queues.Task;
import edu.polytech.utils.Executor;

/**
 * Local (single-JVM) {@link QueueBroker}. Owned by whichever task created
 * it (captured via {@link Task#task()} at construction time, exactly as
 * {@code CTask.newBroker} calls {@code new CQueueBroker(name)} from
 * within that task's own execution).
 * <p>
 * No synchronization is needed anywhere in this class: the whole runtime
 * is driven by a single {@code Executor} thread, so {@code bind}, {@code
 * unbind} and {@code connect} are never actually called concurrently.
 */
public final class CQueueBroker implements QueueBroker {

  private final String name;
  private final Task owner;
  private final Map<Integer, BindListener> bindings = new HashMap<>();

  public CQueueBroker(String name) {
    this.name = name;
    this.owner = Task.task();
    Executor.self().set(owner, this);
    BrokerManager.register(this);
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Task getTask() {
    return owner;
  }

  @Override
  public boolean bind(int port, BindListener listener) {
    if (bindings.containsKey(port))
      return false;
    bindings.put(port, listener);
    return true;
  }

  @Override
  public boolean unbind(int port) {
    BindListener l = bindings.remove(port);
    if (l == null)
      return false;
    // "the bind listener is always invoked on the task of the broker"
    owner.post(l::unbound);
    return true;
  }

  @Override
  public boolean connect(String name, int port, ConnectListener listener) {
    CQueueBroker remote = BrokerManager.find(name);
    if (remote == null)
      // "If that remote broker cannot be found, the method connect returns false"
      return false;

    Task callerTask = Task.task();
    // The lookup above is synchronous (it decides connect()'s own return
    // value), but matching against the current bind state, and every
    // listener callback, is always posted - never called back
    // synchronously from inside connect() itself.
    callerTask.post(() -> {
      BindListener bl = remote.bindings.get(port);
      if (bl == null) {
        listener.refused();
        return;
      }
      // "A" end point: owned by the connecting task.
      CMessageQueue a = new CMessageQueue(this);
      // "B" end point must be created while the REMOTE broker's owner
      // task is the current task, since that is its rightful owner.
      remote.owner.post(() -> {
        CMessageQueue b = new CMessageQueue(remote);
        a.setPeer(b);
        b.setPeer(a);
        bl.accepted(b);
      });
      callerTask.post(() -> listener.connected(a));
    });
    return true;
  }
}
