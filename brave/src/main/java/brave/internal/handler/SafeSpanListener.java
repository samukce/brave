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
package brave.internal.handler;

import brave.handler.MutableSpan;
import brave.handler.SpanListener;
import brave.internal.Nullable;
import brave.internal.Platform;
import brave.propagation.TraceContext;
import java.util.Arrays;

import static brave.internal.Throwables.propagateIfFatal;

/** This logs exceptions instead of raising an error, as the supplied listener could have bugs. */
public abstract class SafeSpanListener extends SpanListener {
  // Array ensures no iterators are created at runtime
  public static SpanListener create(SpanListener[] handlers) {
    if (handlers.length == 0) return SpanListener.NOOP;
    if (handlers.length == 1) return new Single(handlers[0]);
    return new Multiple(handlers);
  }

  @Override
  public void onCreate(@Nullable TraceContext parent, TraceContext context, MutableSpan span) {
    try {
      doOnCreate(parent, context, span);
    } catch (Throwable t) {
      propagateIfFatal(t);
      Platform.get().log("error handling create {0}", context, t);
    }
  }

  @Override public void onAbandon(TraceContext context, MutableSpan span) {
    try {
      doOnAbandon(context, span);
    } catch (Throwable t) {
      propagateIfFatal(t);
      Platform.get().log("error handling abandon {0}", context, t);
    }
  }

  @Override public void onFlush(TraceContext context, MutableSpan span) {
    try {
      doOnFlush(context, span);
    } catch (Throwable t) {
      propagateIfFatal(t);
      Platform.get().log("error handling flush {0}", context, t);
    }
  }

  @Override public void onOrphan(TraceContext context, MutableSpan span) {
    try {
      doOnOrphan(context, span);
    } catch (Throwable t) {
      propagateIfFatal(t);
      Platform.get().log("error handling orphan {0}", context, t);
    }
  }

  @Override public void onFinish(TraceContext context, MutableSpan span) {
    try {
      doOnFinish(context, span);
    } catch (Throwable t) {
      propagateIfFatal(t);
      Platform.get().log("error handling finish {0}", context, t);
    }
  }

  abstract void doOnCreate(@Nullable TraceContext parent, TraceContext context, MutableSpan span);

  abstract void doOnAbandon(TraceContext context, MutableSpan span);

  abstract void doOnFlush(TraceContext context, MutableSpan span);

  abstract void doOnOrphan(TraceContext context, MutableSpan span);

  abstract void doOnFinish(TraceContext context, MutableSpan span);

  static final class Single extends SafeSpanListener {
    final SpanListener delegate;

    Single(SpanListener delegate) {
      this.delegate = delegate;
    }

    @Override
    void doOnCreate(@Nullable TraceContext parent, TraceContext context, MutableSpan span) {
      delegate.onCreate(parent, context, span);
    }

    @Override void doOnAbandon(TraceContext context, MutableSpan span) {
      delegate.onAbandon(context, span);
    }

    @Override void doOnFlush(TraceContext context, MutableSpan span) {
      delegate.onFlush(context, span);
    }

    @Override void doOnOrphan(TraceContext context, MutableSpan span) {
      delegate.onOrphan(context, span);
    }

    @Override void doOnFinish(TraceContext context, MutableSpan span) {
      delegate.onFinish(context, span);
    }

    @Override public String toString() {
      return delegate.toString();
    }
  }

  static final class Multiple extends SafeSpanListener {
    final SpanListener[] listeners;

    Multiple(SpanListener[] listeners) {
      this.listeners = listeners;
    }

    @Override
    void doOnCreate(@Nullable TraceContext parent, TraceContext context, MutableSpan span) {
      for (SpanListener listener : listeners) {
        listener.onCreate(parent, context, span);
      }
    }

    @Override void doOnAbandon(TraceContext context, MutableSpan span) {
      for (SpanListener listener : listeners) {
        listener.onAbandon(context, span);
      }
    }

    @Override void doOnFlush(TraceContext context, MutableSpan span) {
      for (SpanListener listener : listeners) {
        listener.onFlush(context, span);
      }
    }

    @Override void doOnOrphan(TraceContext context, MutableSpan span) {
      for (SpanListener listener : listeners) {
        listener.onOrphan(context, span);
      }
    }

    @Override void doOnFinish(TraceContext context, MutableSpan span) {
      for (SpanListener listener : listeners) {
        listener.onFinish(context, span);
      }
    }

    @Override public String toString() {
      return Arrays.toString(listeners);
    }
  }
}
