/*
 * Copyright 2013-2020 The OpenZipkin Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package brave.internal.recorder;

import brave.Clock;
import brave.Tracer;
import brave.handler.FinishedSpanHandler;
import brave.handler.MutableSpan;
import brave.internal.InternalPropagation;
import brave.internal.Nullable;
import brave.internal.Platform;
import brave.propagation.TraceContext;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Similar to Finagle's deadline span map, except this is GC pressure as opposed to timeout driven.
 * This means there's no bookkeeping thread required in order to flush orphaned spans. Work here is
 * stolen from callers, though. For example, a call to {@link Tracer#nextSpan()} implicitly performs
 * a check for orphans, invoking any handler that applies.
 *
 * <p>Spans are weakly referenced by their owning context. When the keys are collected, they are
 * transferred to a queue, waiting to be reported. A call to modify any span will implicitly flush
 * orphans to Zipkin. Spans in this state will have a "brave.flush" annotation added to them.
 *
 * <p>The internal implementation is derived from WeakConcurrentMap by Rafael Winterhalter. See
 * https://github.com/raphw/weak-lock-free/blob/master/src/main/java/com/blogspot/mydailyjava/weaklockfree/WeakConcurrentMap.java
 * Notably, this does not require reference equality for keys, rather stable {@link #hashCode()}.
 */
public final class PendingSpans extends ReferenceQueue<TraceContext> {
  // Even though we only put by RealKey, we allow get and remove by LookupKey
  final ConcurrentMap<Object, PendingSpan> delegate = new ConcurrentHashMap<>(64);
  final Clock clock;
  final FinishedSpanHandler orphanedSpanHandler;
  final boolean trackOrphans;
  final AtomicBoolean noop;

  public PendingSpans(Clock clock, FinishedSpanHandler orphanedSpanHandler, boolean trackOrphans,
    AtomicBoolean noop) {
    this.clock = clock;
    this.orphanedSpanHandler = orphanedSpanHandler;
    this.trackOrphans = trackOrphans;
    this.noop = noop;
  }

  /**
   * Gets a pending span, or returns {@code null} if there is none.
   *
   * <p>This saves processing time and ensures reference consistency by looking for an existing,
   * decorated context first. This ensures an externalized, but existing context is not mistaken for
   * a new local root.
   */
  @Nullable public PendingSpan get(TraceContext context) {
    if (context == null) throw new NullPointerException("context == null");
    reportOrphanedSpans();
    return delegate.get(context);
  }

  public PendingSpan getOrCreate(
    @Nullable TraceContext parent, TraceContext context, boolean start) {
    PendingSpan result = get(context);
    if (result != null) return result;

    MutableSpan data = new MutableSpan();
    if (context.shared()) data.setShared();

    PendingSpan parentSpan = parent != null ? get(parent) : null;

    // save overhead calculating time if the parent is in-progress (usually is)
    TickClock clock;
    if (parentSpan != null) {
      clock = parentSpan.clock;
      if (start) data.startTimestamp(clock.currentTimeMicroseconds());
    } else {
      long currentTimeMicroseconds = this.clock.currentTimeMicroseconds();
      clock = new TickClock(currentTimeMicroseconds, System.nanoTime());
      if (start) data.startTimestamp(currentTimeMicroseconds);
    }

    PendingSpan newSpan = new PendingSpan(context, data, clock);
    PendingSpan previousSpan = delegate.putIfAbsent(new RealKey(context, this), newSpan);
    if (previousSpan != null) return previousSpan; // lost race

    // We've now allocated a new trace context.
    // It is a bug to have neither a reference to your parent or local root set.
    assert parent != null || context.isLocalRoot();

    if (trackOrphans) {
      newSpan.caller =
        new Throwable("Thread " + Thread.currentThread().getName() + " allocated span here");
    }
    return newSpan;
  }

  /** @see brave.Span#abandon() */
  public void abandon(TraceContext context) {
    if (context == null) throw new NullPointerException("context == null");
    PendingSpan last = delegate.remove(context);
    reportOrphanedSpans(); // also clears the reference relating to the recent remove
  }

  /** @see brave.Span#finish() */
  public boolean remove(TraceContext context) {
    if (context == null) throw new NullPointerException("context == null");
    PendingSpan last = delegate.remove(context);
    reportOrphanedSpans(); // also clears the reference relating to the recent remove
    return last != null;
  }

  /** Reports spans orphaned by garbage collection. */
  void reportOrphanedSpans() {
    RealKey contextKey;
    // This is called on critical path of unrelated traced operations. If we have orphaned spans, be
    // careful to not penalize the performance of the caller. It is better to cache time when
    // flushing a span than hurt performance of unrelated operations by calling
    // currentTimeMicroseconds N times
    long flushTime = 0L;
    boolean noop = orphanedSpanHandler == FinishedSpanHandler.NOOP || this.noop.get();
    while ((contextKey = (RealKey) poll()) != null) {
      PendingSpan value = delegate.remove(contextKey);
      if (noop || value == null) continue;
      assert value.context() == null : "unexpected for the weak referent to be present after GC!";
      if (flushTime == 0L) flushTime = clock.currentTimeMicroseconds();

      boolean isEmpty = value.state.isEmpty();
      Throwable caller = value.caller;

      TraceContext context = InternalPropagation.instance.newTraceContext(
        contextKey.flags,
        contextKey.traceIdHigh, contextKey.traceId,
        contextKey.localRootId, 0L, contextKey.spanId,
        Collections.emptyList()
      );

      if (caller != null) {
        String message = isEmpty
          ? "Span " + context + " was allocated but never used"
          : "Span " + context + " neither finished nor flushed before GC";
        Platform.get().log(message, caller);
      }
      if (isEmpty) continue;

      value.state.annotate(flushTime, "brave.flush");
      orphanedSpanHandler.handle(context, value.state);
    }
  }

  /**
   * Real keys contain a reference to the real context associated with a span. This is a weak
   * reference, so that we get notified on GC pressure.
   *
   * <p>Since {@linkplain TraceContext}'s hash code is final, it is used directly both here and in
   * lookup keys.
   */
  static final class RealKey extends WeakReference<TraceContext> {
    final int hashCode;

    // Copy the identity fields from the trace context, so we can use them when the reference clears
    final long traceIdHigh, traceId, localRootId, spanId;
    final int flags;

    RealKey(TraceContext context, ReferenceQueue<TraceContext> queue) {
      super(context, queue);
      hashCode = context.hashCode();
      traceIdHigh = context.traceIdHigh();
      traceId = context.traceId();
      localRootId = context.localRootId();
      spanId = context.spanId();
      flags = InternalPropagation.instance.flags(context);
    }

    @Override public String toString() {
      TraceContext context = get();
      return context != null ? "WeakReference(" + context + ")" : "ClearedReference()";
    }

    @Override public int hashCode() {
      return this.hashCode;
    }

    /** Resolves hash code collisions */
    @Override public boolean equals(Object other) {
      TraceContext thisContext = get(), thatContext = ((RealKey) other).get();
      if (thisContext == null) {
        return thatContext == null;
      } else {
        return thisContext.equals(thatContext);
      }
    }
  }

  /**
   * Lookup keys are cheaper than real keys as reference tracking is not involved. We cannot use
   * {@linkplain TraceContext} directly as a lookup key, as eventhough it has the same hash code as
   * the real key, it would fail in equals comparison.
   */
  static final class LookupKey {
    long traceIdHigh, traceId, spanId;
    boolean shared;
    int hashCode;

    void set(TraceContext context) {
      set(context.traceIdHigh(), context.traceId(), context.spanId(), context.shared());
    }

    void set(long traceIdHigh, long traceId, long spanId, boolean shared) {
      this.traceIdHigh = traceIdHigh;
      this.traceId = traceId;
      this.spanId = spanId;
      this.shared = shared;
      hashCode = generateHashCode(traceIdHigh, traceId, spanId, shared);
    }

    @Override public int hashCode() {
      return hashCode;
    }

    static int generateHashCode(long traceIdHigh, long traceId, long spanId, boolean shared) {
      int h = 1;
      h *= 1000003;
      h ^= (int) ((traceIdHigh >>> 32) ^ traceIdHigh);
      h *= 1000003;
      h ^= (int) ((traceId >>> 32) ^ traceId);
      h *= 1000003;
      h ^= (int) ((spanId >>> 32) ^ spanId);
      h *= 1000003;
      h ^= shared ? InternalPropagation.FLAG_SHARED : 0; // to match TraceContext.hashCode
      return h;
    }

    /** Resolves hash code collisions */
    @Override public boolean equals(Object other) {
      RealKey that = (RealKey) other;
      TraceContext thatContext = that.get();
      if (thatContext == null) return false;
      return traceIdHigh == thatContext.traceIdHigh()
        && traceId == thatContext.traceId()
        && spanId == thatContext.spanId()
        && shared == thatContext.shared();
    }
  }

  @Override public String toString() {
    return "PendingSpans" + delegate.keySet();
  }
}
