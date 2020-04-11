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
package brave.handler;

import brave.Span;
import brave.internal.Nullable;
import brave.propagation.TraceContext;
import java.lang.ref.WeakReference;

/**
 * This is like {@link FinishedSpanHandler}, except it can cover all state conditions, including
 * when a span is created or abandoned. The purpose of this type is to allow tracking of children,
 * or partitioning of data for backend that needs to see an entire {@linkplain
 * TraceContext#localRootId() local root}.
 *
 * <p>As with {@link FinishedSpanHandler}, it is important to do work quickly as callbacks are run
 * on the same thread as application code. That said, there are some rules to keep in mind below.
 *
 * <p>The {@link TraceContext} parameter from {@link #onCreate} will be the same reference for
 * all callbacks, except {@link #onOrphan}, which has value, but not reference equality.
 *
 * <p>The {@link MutableSpan} parameter from {@link #onCreate} will be the same reference for
 * all callbacks. Do not mutate {@link MutableSpan} between callbacks as it is not thread safe.
 *
 * <p>If caching the {@link TraceContext} parameter, consider a {@link WeakReference} to avoid
 * holding up garbage collection.
 */
public class SpanListener {
  /** Use to avoid comparing against null references */
  public static final SpanListener NOOP = new SpanListener() {
    @Override public String toString() {
      return "NoopSpanListener{}";
    }
  };

  protected SpanListener() {
  }

  /**
   * This is called when a span is allocated, but before it is started. An allocation here will
   * result in one of:
   *
   * <ol>
   *   <li>{@link #onAbandon} if this was a speculative context</li>
   *   <li>{@link #onFlush} if this was intentionally reported incomplete</li>
   *   <li>{@link #onOrphan} if this was reported incomplete due to garbage collection</li>
   *   <li>{@link #onFinish} if this was reported complete</li>
   * </ol>
   *
   * <p>The {@code parent} can be {@code null} only when the new context is a {@linkplain
   * TraceContext#isLocalRoot() local root}.
   */
  public void onCreate(@Nullable TraceContext parent, TraceContext context, MutableSpan span) {
  }

  /**
   * Called on {@link Span#abandon()}.
   *
   * <p>This is useful when counting children. Decrement your counter when this occurs as the span
   * will not be reported.
   *
   * <p><em>Note:</em>Abandoned spans should be ignored as they aren't indicative of an error. Some
   * instrumentation speculatively create a span for possible outcomes such as retry.
   */
  public void onAbandon(TraceContext context, MutableSpan span) {
  }

  /**
   * Called on {@link Span#flush()}.
   *
   * <p>Even though the span here will is incomplete (missing {@link MutableSpan#finishTimestamp()},
   * it is reported to the tracing system unless a {@link FinishedSpanHandler} returns false.
   */
  public void onFlush(TraceContext context, MutableSpan span) {
  }

  /**
   * Called when the trace context was garbage collected prior to completion.
   *
   * <p>Unlike {@link FinishedSpanHandler#supportsOrphans()}, this is called even if {@linkplain
   * MutableSpan#isEmpty() empty}. Non-empty spans are reported to the tracing system unless a
   * {@link FinishedSpanHandler} returns false.
   *
   * @param context unlike the other methods, this context will not be the same reference as {@link
   * #onCreate} even though it will have the same trace IDs.
   * @param span possibly {@linkplain MutableSpan#isEmpty() empty} span.
   * @see FinishedSpanHandler#supportsOrphans()
   */
  public void onOrphan(TraceContext context, MutableSpan span) {
  }

  /** Called on {@link Span#finish()}. */
  public void onFinish(TraceContext context, MutableSpan span) {
  }
}
