/*
* Teragrep Reliable Event Logging Protocol (RELP) Library for Java
* Copyright (C) 2021-2026 Suomen Kanuuna Oy
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.teragrep.rlp_01.client;

import com.teragrep.rlp_01.RelpBatch;

import java.io.IOException;
import java.io.UncheckedIOException;

public class RetryableRelpConnection implements IManagedRelpConnection {

    private final IManagedRelpConnection managedRelpConnection;
    private final long maxRetries;
    private final int reconnectInterval;

    public RetryableRelpConnection(
            IManagedRelpConnection managedRelpConnection,
            long maxRetries,
            int reconnectInterval
    ) {
        this.managedRelpConnection = managedRelpConnection;
        this.maxRetries = maxRetries;
        this.reconnectInterval = reconnectInterval;
    }

    @Override
    public void forceReconnect() {
        long attempts = 0;
        boolean notConnected = true;
        while (notConnected) {
            attempts++;
            try {
                managedRelpConnection.forceReconnect();
                notConnected = false;
            }
            catch (UncheckedIOException e) {
                if (attempts >= maxRetries) {
                    throw new RuntimeException("forceReconnect() gave up after <[" + maxRetries + "]> tries.");
                }
                sleep();
            }
        }
    }

    @Override
    public void reconnect() {
        long attempts = 0;
        boolean notConnected = true;
        while (notConnected) {
            attempts++;
            try {
                managedRelpConnection.reconnect();
                notConnected = false;
            }
            catch (UncheckedIOException e) {
                if (attempts >= maxRetries) {
                    throw new RuntimeException("reconnect() gave up after <[" + maxRetries + "]> tries.");
                }
                sleep();
            }
        }
    }

    /**
     * Tries to establish a relp connection indefinitely, on failure awaits a configured interval before retry.
     *
     * @return number of attempts required to connect
     */
    @Override
    public long connect() throws IOException {
        long attempts = 0;
        boolean notConnected = true;
        while (notConnected) {
            attempts++;
            try {
                managedRelpConnection.connect();
                notConnected = false;
            }
            catch (UncheckedIOException e) {
                if (attempts >= maxRetries) {
                    throw new RuntimeException("connect() gave up after <[" + maxRetries + "]> tries.");
                }
                sleep();
            }
        }
        return attempts;
    }

    /**
     * Tries to commit a relp batch to a connection indefinitely until successful.
     *
     * @param relpBatch relp batch to be commited
     * @return number of attempts required to commit a batch
     */
    @Override
    public long ensureSent(RelpBatch relpBatch) {
        long attempts = 0;
        long retries = 0;
        boolean notSent = true;
        while (notSent) {
            try {
                attempts = managedRelpConnection.ensureSent(relpBatch);
                notSent = false;
            }
            catch (UncheckedIOException e) {
                if (retries >= maxRetries) {
                    throw e;
                }
                retries++;
                sleep();
            }
        }
        return attempts;
    }

    @Override
    public long ensureSent(byte[] bytes) {
        final RelpBatch relpBatch = new RelpBatch();
        relpBatch.insert(bytes);
        return ensureSent(relpBatch);
    }

    @Override
    public boolean isStub() {
        return managedRelpConnection.isStub();
    }

    @Override
    public void close() throws IOException {
        managedRelpConnection.close();
    }

    private void sleep() {
        try {
            Thread.sleep(reconnectInterval);
        }
        catch (InterruptedException exception) {
            System.err.println("Reconnection timer interrupted");
        }
    }
}
