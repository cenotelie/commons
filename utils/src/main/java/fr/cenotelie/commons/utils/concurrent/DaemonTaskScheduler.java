/*******************************************************************************
 * Copyright (c) 2017 Association Cénotélie (cenotelie.fr)
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation, either version 3
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General
 * Public License along with this program.
 * If not, see <http://www.gnu.org/licenses/>.
 ******************************************************************************/

package fr.cenotelie.commons.utils.concurrent;

import fr.cenotelie.commons.utils.logging.Logging;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Implements a simple scheduler for a recurrent task that needs to be run at some interval of time
 *
 * @author Laurent Wouters
 */
public class DaemonTaskScheduler implements AutoCloseable {
    /**
     * Commands the daemon to wait for the next execution opportunity
     */
    private static final int COMMAND_WAIT = 0;
    /**
     * Commands the daemon to execute the task
     */
    private static final int COMMAND_EXECUTE = 1;
    /**
     * Commands the daemon to exit
     */
    private static final int COMMAND_EXIT = 2;

    /**
     * The counter of threads
     */
    private static final AtomicInteger COUNTER = new AtomicInteger(0);
    /**
     * The recurrent task
     */
    private final Runnable task;
    /**
     * The thread that will run the task
     */
    private Thread thread;
    /**
     * The current command for the thread
     */
    private final AtomicInteger command;
    /**
     * The current wait period for the daemon
     */
    private final int waitPeriod;
    /**
     * In a manual trigger mode only, signal for the thread
     */
    private final CyclicBarrier signal;

    /**
     * Initializes this scheduler
     *
     * @param task   The recurrent task
     * @param period The wait period, 0 means that the task can only be manually triggered
     */
    public DaemonTaskScheduler(Runnable task, int period) {
        if (task == null || period < 0)
            throw new IllegalArgumentException();
        int id = COUNTER.getAndIncrement();
        this.task = task;
        this.thread = new Thread(new SafeRunnable() {
            @Override
            public void doRun() {
                daemonMain();
            }

            @Override
            protected void onRunFailed(Throwable throwable) {
                // the daemon thread failed
                daemonRestart();
            }
        }, DaemonTaskScheduler.class.getCanonicalName() + ".Thread." + id);
        this.command = new AtomicInteger(COMMAND_WAIT);
        this.waitPeriod = period;
        this.signal = period == 0 ? null : new CyclicBarrier(2);
        this.thread.start();
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                close();
            }
        }, DaemonTaskScheduler.class.getCanonicalName() + ".shutdown." + id));
    }

    /**
     * Triggers the task execution
     */
    public void trigger() {
        while (true) {
            int command = this.command.get();
            if (command == COMMAND_EXIT)
                return;
            if (command == COMMAND_WAIT && this.command.compareAndSet(COMMAND_WAIT, COMMAND_EXECUTE)) {
                daemonWake();
                return;
            }
        }
    }

    /**
     * Resets the wait time for the task execution
     */
    public void resetWait() {
        if (waitPeriod == 0)
            // no period, this does nothing
            return;
        while (true) {
            int command = this.command.get();
            if (command == COMMAND_EXIT)
                return;
            if (command == COMMAND_WAIT) {
                daemonWake();
                return;
            }
        }
    }

    /**
     * Main method for the daemon
     */
    private void daemonMain() {
        while (true) {
            int command = this.command.get();
            if (command == COMMAND_EXIT)
                return;
            if (command == COMMAND_EXECUTE && this.command.compareAndSet(COMMAND_EXECUTE, COMMAND_WAIT)) {
                // execute the task once
                try {
                    task.run();
                } catch (Throwable throwable) {
                    Logging.get().error(throwable);
                }
            } else if (command == COMMAND_WAIT) {
                daemonPark();
            }
        }
    }

    /**
     * Parks the daemon thread
     */
    private void daemonPark() {
        if (signal != null) {
            try {
                signal.await();
            } catch (Throwable throwable) {
                // do nothing
            } finally {
                // reset the signal and resume execution
                signal.reset();
            }
        } else {
            try {
                Thread.sleep(waitPeriod);
            } catch (InterruptedException exception) {
                // interrupted, clear and resume
                thread.isInterrupted();
            }
        }
    }

    /**
     * Wake the daemon
     */
    private void daemonWake() {
        if (waitPeriod == 0) {
            if (!signal.isBroken() && signal.getNumberWaiting() == 0)
                // daemon is running, do nothing
                return;
            try {
                signal.await();
            } catch (Throwable throwable) {
                // do nothing
            }
        } else {
            switch (thread.getState()) {
                case BLOCKED:
                case WAITING:
                case TIMED_WAITING:
                    thread.interrupt();
            }
        }
    }

    /**
     * When the daemon thread has failed for some reason, restart it
     */
    private void daemonRestart() {
        thread = new Thread(new SafeRunnable() {
            @Override
            public void doRun() {
                daemonMain();
            }

            @Override
            protected void onRunFailed(Throwable throwable) {
                // the daemon thread failed
                daemonRestart();
            }
        }, DaemonTaskScheduler.class.getCanonicalName() + ".Thread." + COUNTER.getAndIncrement());
        command.set(COMMAND_WAIT);
        if (signal != null)
            signal.reset();
        thread.start();
    }

    @Override
    public void close() {
        command.set(COMMAND_EXIT);
        daemonWake();
        try {
            thread.join();
        } catch (InterruptedException exception) {
            // do nothing
        }
    }
}
