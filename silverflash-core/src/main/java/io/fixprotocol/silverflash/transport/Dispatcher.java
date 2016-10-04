/**
 *    Copyright 2015-2016 FIX Protocol Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package io.fixprotocol.silverflash.transport;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

import io.fixprotocol.silverflash.Service;

/**
 * Dispatcher thread passes received messages to consumers from one or more transports
 */
public class Dispatcher implements Runnable, Service {

  private final AtomicBoolean isRunning = new AtomicBoolean();
  private final AtomicBoolean started = new AtomicBoolean();
  private Thread thread = null;
  private final CopyOnWriteArrayList<Transport> transports = new CopyOnWriteArrayList<>();
  private final ThreadFactory threadFactory;

  /**
   * Constructor uses a default ThreadFactory
   */
  public Dispatcher() {
    this.threadFactory = Executors.defaultThreadFactory();
  }

  /**
   * Constructor with a ThreadFactory
   * 
   * @param threadFactory
   *          creates new threads on demand
   */
  public Dispatcher(ThreadFactory threadFactory) {
    this.threadFactory = threadFactory;
  }

  /**
   * Adds a Transport to dispatch its messages If the dispatcher thread has not yet been started,
   * this Dispatcher is started synchronously.
   * 
   * @param transport
   *          a Transport
   */
  public void addTransport(Transport transport) {
    Objects.requireNonNull(transport);
    transports.add(transport);
    if (isRunning.compareAndSet(false, true)) {
      if (thread == null) {
        thread = threadFactory.newThread(this);
      }
      thread.start();
      while (!started.compareAndSet(true, true))
        ;
    }
  }

  public void close() {
    isRunning.compareAndSet(true, false);
    if (thread != null) {
      try {
        thread.join(1000L);
        thread = null;
      } catch (InterruptedException e) {

      }
    }
  }
  
  public CompletableFuture<Dispatcher> open() {
    if (isRunning.compareAndSet(false, false)) {
      CompletableFuture<Dispatcher> future = new CompletableFuture<>();
      thread.start();
      return future;
    } else {
      return CompletableFuture.completedFuture(this);
    }
  }

  /**
   * Stop dispatching messages from a Transport. If no transports remain, then this Dispatcher is
   * closed.
   * 
   * @param transport
   *          a Transport to stop dispatching
   */
  public void removeTransport(Transport transport) {
    transports.remove(transport);
    if (transports.isEmpty()) {
      close();
    }
  }

  public void run() {
    started.set(true);
    while (isRunning.compareAndSet(true, true)) {
      for (int i = 0; i < transports.size(); i++) {
        try {
          final Transport transport = transports.get(i);
          if (transport.isReadyToRead()) {
            try {
              transport.read();
            } catch (IOException e) {
              removeTransport(transport);
            }
          }
        } catch (ArrayIndexOutOfBoundsException ex) {
          // item removed while iterating - retry iteration
        }
      }
    }
    started.set(false);
  }
}