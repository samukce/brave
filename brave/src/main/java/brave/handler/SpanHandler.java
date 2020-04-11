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
 * This is a special type of {@link FinishedSpanHandler}, signaled when a span is created or
 * abandoned. The purpose of this type is to allow tracking of children, or partitioning of data for
 * backend that needs to see an entire {@linkplain TraceContext#localRootId() local root}.
 *
 * <p>As with {@link FinishedSpanHandler}, it is important to do work quickly as callbacks are run
 * on the same thread as application code. That said, there are some rules to keep in mind below.
 *
 * <p>The {@link TraceContext} parameter will be the same reference for all callbacks, except if
 * {@link #supportsOrphans()} is set to true. Unless you handle orphans, it is safe to use identity
 * maps for tracking. When {@link #supportsOrphans()} is set to true, contexts in {@link #handle}
 * missing a {@link MutableSpan#finishTimestamp()} will have value, but not reference equality.
 *
 * <p>If caching the {@link TraceContext} parameter, use a {@link WeakReference} to avoid holding
 * up garbage collection. Do not cache the {@link MutableSpan} parameter.
 */
public abstract class SpanHandler extends FinishedSpanHandler {
  /** Use to avoid comparing against null references */
  public static final SpanHandler NOOP = new SpanHandler() {
    @Override public void handleCreate(
      @Nullable TraceContext parent, TraceContext context, MutableSpan span) {
    }

    @Override public void handleAbandon(TraceContext context, MutableSpan span) {
    }

    @Override public boolean handle(TraceContext context, MutableSpan span) {
      return true;
    }

    @Override public String toString() {
      return "NoopFinishedSpanHandler{}";
    }
  };

  /**
   * This is called when a span is allocated, but before it is started. An allocation here will
   * result in one of {@link #handle} or {@link #handleAbandon}.
   *
   * <p>The {@code parent} can be {@code null} only when the new context is a {@linkplain
   * TraceContext#isLocalRoot() local root}.
   */
  public abstract void handleCreate(
    @Nullable TraceContext parent, TraceContext context, MutableSpan span);

  /**
   * Called only when {@link Span#abandon()} was called before {@link Span#finish()} or {@link
   * Span#flush()}.
   *
   * <p>This is useful when counting children. Decrement your counter when this occurs as the span
   * will not be reported.
   *
   * <p><em>Note:</em>Abandoned spans should be ignored as they aren't indicative of an error. Some
   * instrumentation speculatively create a span for possible outcomes such as retry.
   */
  public abstract void handleAbandon(TraceContext context, MutableSpan span);
}
