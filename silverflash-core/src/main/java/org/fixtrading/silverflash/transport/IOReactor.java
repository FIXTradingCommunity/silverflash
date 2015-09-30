/**
 *    Copyright 2015 FIX Protocol Ltd
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

package org.fixtrading.silverflash.transport;

import java.io.IOException;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

import org.fixtrading.silverflash.Service;

/**
 * An IO event demultiplexor
 * 
 * @author Don Mendelson
 *
 */
public class IOReactor implements Runnable, Service {
  private Thread reactorThread;
  private final AtomicBoolean running = new AtomicBoolean();
  private Selector selector;
  private final ThreadFactory threadFactory;
  private CompletableFuture<IOReactor> future;


  public IOReactor() {
    this.threadFactory = Executors.defaultThreadFactory();
  }

  public IOReactor(ThreadFactory threadFactory) {
    this.threadFactory = threadFactory;
  }

  public void close() {
    running.compareAndSet(true, false);
    if (reactorThread != null) {
      try {
        reactorThread.join(1000L);
      } catch (InterruptedException e) {

      }
    }
  }

  public Selector getSelector() {
    return selector;
  }

  public CompletableFuture<IOReactor> open() {
    if (running.compareAndSet(false, false)) {
      future = new CompletableFuture<>();
      reactorThread = threadFactory.newThread(this);
      reactorThread.start();
      return future;
    } else {
      return CompletableFuture.completedFuture(this);
    }
  }

  public void run() {

    try {
      selector = Selector.open();
      running.set(true);
      future.complete(this);
    } catch (IOException ex) {
      future.completeExceptionally(ex);
    }

    try {
      System.out.println("IOReactor started");
      while (running.compareAndSet(true, true)) {
        int numberUpdated = selector.selectNow();
        Iterator<SelectionKey> iter = selector.selectedKeys().iterator();

        while (iter.hasNext()) {
          SelectionKey selectedKey = iter.next();
          iter.remove();

          Object attachment = selectedKey.attachment();
          if (selectedKey.isAcceptable()) {
            ((Acceptor) attachment).readyToAccept();
          } else if (selectedKey.isConnectable()) {
            ((Connector) attachment).readyToConnect();
          } else {
            if (selectedKey.isReadable()) {
              ((ReactiveTransport) attachment).readyToRead();
            }

            // Check if the key is still valid, since it might
            // have been invalidated in the read handler
            // (for instance, the socket might have been closed)
            if (selectedKey.isValid() && selectedKey.isWritable()) {
              ((ReactiveTransport) attachment).readyToWrite();
            }
          }
        }
      }

      selector.close();
    } catch (ClosedSelectorException e) {
      System.err.println("Selector closed unexpectedly; stopping IOReactor");
      running.set(false);
    } catch (CancelledKeyException | ClosedChannelException e) {
      System.out.println("Transport closed");
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      running.set(false);
      selector = null;
      System.out.println("IOReactor closed");
    }
  }

}
