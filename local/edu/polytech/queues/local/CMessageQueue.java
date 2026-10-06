package edu.polytech.queues.local;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import edu.polytech.queues.MessageQueue;
import edu.polytech.queues.QueueBroker;
import edu.polytech.queues.Task;
import edu.polytech.utils.Executor;

/**
 * One end point of a connected message queue. The two end points created
 * together by {@link CQueueBroker#connect} reference each other via
 * {@link #peer}; there is no separate "pair" object because, with a
 * single-threaded {@code Executor}, nothing needs to be synchronized.
 * <p>
 * Mirrors, for messages, the same closed/closing distinction the channel
 * ("L1") specification used for bytes: {@link #outstanding} counts how
 * many messages this end point has sent that have not yet been delivered
 * to its peer, which is exactly what lets the "closing" end point learn
 * when it may also become fully {@link #closed()}.
 */
public final class CMessageQueue implements MessageQueue {

  private final CQueueBroker broker;
  private final Task owner;
  private CMessageQueue peer;

  private boolean closed = false;
  private boolean notifiedClosed = false;
  private int outstanding = 0; // messages I sent, not yet delivered to my peer

  private Listener listener;
  private Task listenerTask;

  // Messages that arrived before setListener() was ever called.
  private final List<byte[]> pendingMsgs = new LinkedList<>();
  private final List<CMessageQueue> pendingFrom = new LinkedList<>();

  public CMessageQueue(CQueueBroker broker) {
    this.broker = broker;
    this.owner = Task.task();
    // So that this queue is auto-closed if its owning task ever dies
    // (see CTask.done()).
    Executor.self().register(owner, this);
  }

  void setPeer(CMessageQueue peer) {
    this.peer = peer;
  }

  @Override
  public QueueBroker broker() {
    return broker;
  }

  @Override
  public void setListener(Listener l) {
    this.listener = l;
    this.listenerTask = Task.task();
    if (!pendingMsgs.isEmpty()) {
      List<byte[]> msgs = new LinkedList<>(pendingMsgs);
      List<CMessageQueue> froms = new LinkedList<>(pendingFrom);
      pendingMsgs.clear();
      pendingFrom.clear();
      for (int i = 0; i < msgs.size(); i++)
        deliver(msgs.get(i), froms.get(i));
    }
    // In case this end point was already closed before anyone was
    // listening, don't lose the closed() notification.
    recheckClosed();
  }

  private static void checkRange(byte[] bytes, int offset, int length) {
    if (bytes == null)
      throw new IllegalArgumentException("bytes array is null");
    if (offset < 0 || length < 0 || offset + length > bytes.length)
      throw new IllegalArgumentException(
          "invalid range: offset=" + offset + ", length=" + length + ", array length=" + bytes.length);
  }

  @Override
  public boolean send(byte[] bytes, int offset, int length, SendListener l) {
    checkRange(bytes, offset, length);
    Task caller = Task.task();
    if (closed || peer.closed) {
      // "no error case": the message is simply dropped, ownership of the
      // range is returned right away.
      if (l != null)
        caller.post(() -> l.sent(bytes, offset, length));
      return true;
    }
    byte[] copy = Arrays.copyOfRange(bytes, offset, offset + length);
    outstanding++;
    if (l != null)
      caller.post(() -> l.sent(bytes, offset, length));
    peer.deliver(copy, this);
    return true;
  }

  /** Called by the sender (`from`) to hand a copy of a message to me. */
  private void deliver(byte[] msg, CMessageQueue from) {
    if (listenerTask == null) {
      pendingMsgs.add(msg);
      pendingFrom.add(from);
      return;
    }
    listenerTask.post(() -> {
      from.outstanding--;
      Listener l = listener;
      if (!closed && l != null)
        l.received(msg);
      from.recheckClosed();
      this.recheckClosed();
    });
  }

  @Override
  public void close() {
    if (closed)
      return;
    closed = true;
    recheckClosed();
    if (peer != null)
      peer.recheckClosed();
  }

  @Override
  public boolean closed() {
    return closed || (peer != null && peer.closed && peer.outstanding == 0);
  }

  private void recheckClosed() {
    if (notifiedClosed || listenerTask == null || !closed())
      return;
    notifiedClosed = true;
    Listener l = listener;
    listenerTask.post(l::closed);
  }
}
