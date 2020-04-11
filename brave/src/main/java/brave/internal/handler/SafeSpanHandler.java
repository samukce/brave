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
import brave.handler.SpanHandler;
import brave.internal.Nullable;
import brave.internal.Platform;
import brave.propagation.TraceContext;
import java.util.Arrays;

import static brave.internal.Throwables.propagateIfFatal;

/** This logs exceptions instead of raising an error, as the supplied handler could have bugs. */
public abstract class SafeSpanHandler extends SpanHandler {
  // Array ensures no iterators are created at runtime
  public static SpanHandler create(SpanHandler[] handlers) {
    if (handlers.length == 0) return SpanHandler.NOOP;
    if (handlers.length == 1) return new Single(handlers[0]);
    return new Multiple(handlers);
  }

  @Override public final void handleCreate(
    @Nullable TraceContext parent, TraceContext context, MutableSpan span) {
    try {
      doHandleCreate(parent, context, span);
    } catch (Throwable t) {
      propagateIfFatal(t);
      Platform.get().log("error handling create {0}", context, t);
    }
  }

  @Override public final void handleAbandon(TraceContext context, MutableSpan span) {
    try {
      doHandleAbandon(context, span);
    } catch (Throwable t) {
      propagateIfFatal(t);
      Platform.get().log("error handling abandon {0}", context, t);
    }
  }

  @Override public final boolean handle(TraceContext context, MutableSpan span) {
    throw new AssertionError("SpanHandler is only used for create and abandon hooks");
  }

  abstract void doHandleCreate(
    @Nullable TraceContext parent, TraceContext context, MutableSpan span);

  abstract void doHandleAbandon(TraceContext context, MutableSpan span);

  static final class Single extends SafeSpanHandler {
    final SpanHandler delegate;

    Single(SpanHandler delegate) {
      this.delegate = delegate;
    }

    @Override void doHandleCreate(
      @Nullable TraceContext parent, TraceContext context, MutableSpan span) {
      delegate.handleCreate(parent, context, span);
    }

    @Override void doHandleAbandon(TraceContext context, MutableSpan span) {
      delegate.handleAbandon(context, span);
    }

    @Override public String toString() {
      return delegate.toString();
    }
  }

  static final class Multiple extends SafeSpanHandler {
    final SpanHandler[] handlers;

    Multiple(SpanHandler[] handlers) {
      this.handlers = handlers;
    }

    @Override void doHandleCreate(
      @Nullable TraceContext parent, TraceContext context, MutableSpan span) {
      for (SpanHandler handler : handlers) {
        handler.handleCreate(parent, context, span);
      }
    }

    @Override void doHandleAbandon(TraceContext context, MutableSpan span) {
      for (SpanHandler handler : handlers) {
        handler.handleAbandon(context, span);
      }
    }

    @Override public String toString() {
      return Arrays.toString(handlers);
    }
  }
}
