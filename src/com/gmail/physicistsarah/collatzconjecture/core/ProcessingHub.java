/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.gmail.physicistsarah.collatzconjecture.core;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.jcip.annotations.NotThreadSafe;

/**
 *
 * @author Sarah Szabo <PhysicistSarah@Gmail.com>
 */
public final class ProcessingHub {

    private static final int AVAILABLE_PROCESSORS = Runtime.getRuntime().availableProcessors();
    private static final List<Future<CollatzSequencer.FinalSequencerReport<? extends Number>>> dataSet
            = new ArrayList<>(Math.toIntExact(ProcessingHubNumericalHelper.DEFAULT_INCREMENT_VALUE.intValueExact()));

    private static BigInteger getNextFinalNumber(BigInteger finishNumber) {
        return finishNumber.mod(BigInteger.valueOf(AVAILABLE_PROCESSORS)).equals(BigInteger.ZERO) ? finishNumber
                : new BigDecimal(finishNumber).divide(BigDecimal.valueOf(AVAILABLE_PROCESSORS)).setScale(0, RoundingMode.CEILING)
                .multiply(BigDecimal.valueOf(AVAILABLE_PROCESSORS)).toBigIntegerExact();
    }

    private static BigInteger getNextStartingNumber(BigInteger startNumber) {
        return startNumber.mod(BigInteger.valueOf(AVAILABLE_PROCESSORS)).equals(BigInteger.ZERO) ? startNumber
                : new BigDecimal(startNumber).divide(BigDecimal.valueOf(AVAILABLE_PROCESSORS)).setScale(0, RoundingMode.FLOOR)
                .multiply(BigDecimal.valueOf(AVAILABLE_PROCESSORS)).toBigIntegerExact();
    }
    private final CyclicBarrier barrier;
    private final ExecutorService service;
    private final ProcessingHubStorageManager storageManager;
    private final ProcessingHubControlCenter controlCenter;

    public ProcessingHub(BigInteger startingNumber, BigInteger endingNumber) {
        this();
        this.controlCenter.setStartingNumber(startingNumber);
        this.controlCenter.setTargetNumber(endingNumber);
        this.controlCenter.setState(HubNumericalState.NUMBER_RANGE);
    }

    public ProcessingHub(HubNumericalState state) {
        this();
        this.controlCenter.setState(state);
    }

    public ProcessingHub() {
        this.storageManager = new ProcessingHubStorageManager((t) -> {
            shutdownHub();
            return null;
        });
        this.controlCenter = new ProcessingHubControlCenter();
        this.barrier = new CyclicBarrier(1, () -> {
            synchronized (dataSet) {
                List<CollatzSequencer.FinalSequencerReport<? extends Number>> list
                        = new ArrayList<>(dataSet.size());
                for (Future<CollatzSequencer.FinalSequencerReport<? extends Number>> value : dataSet) {
                    try {
                        list.add(value.get());
                    } catch (InterruptedException | ExecutionException ex) {
                        Logger.getLogger(ProcessingHub.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
                dataSet.clear();
                Collections.sort(list, CollatzSequencer.FinalSequencerReport.compareByInitialValue());
                System.out.println("Final Iteration: \n" + list.get(list.size() - 1));
                System.out.println("Saving Values...\n-------------------------------------------------------------------");
                try {
                    storageManager.saveValues(list);
                } catch (InterruptedException ex) {
                    Logger.getLogger(ProcessingHub.class.getName()).log(Level.SEVERE, null, ex);
                    throw new RuntimeException(ex);
                }
            }
        });
        this.service = Executors.newFixedThreadPool(AVAILABLE_PROCESSORS + 1, new ThreadFactory() {
            private BigInteger count = BigInteger.ZERO;

            @Override
            public Thread newThread(Runnable r) {
                count = count.add(BigInteger.ONE);
                return new Thread(r, "Collatz Sequencer Thread: " + count);
            }
        });
    }

    //TODO: Add buffer to submit method
    private void createThreads(BigInteger startNumber, BigInteger finishNumber) {
        if (finishNumber.subtract(startNumber).compareTo(BigInteger.ZERO) < 0) {
            throw new IllegalArgumentException("The starting number is larger than the final number.");
        } else if (startNumber.compareTo(BigInteger.ZERO) < 0) {
            throw new IllegalArgumentException("The starting number is less than zero");
        }
        for (BigInteger i = startNumber; i.compareTo(finishNumber) < 0; i = i.add(BigInteger.ONE)) {
            BigInteger copy = i;
            dataSet.add(this.service.submit(() -> {
                return new CollatzSequencer(copy, true).init();
            }));
        }
        try {
            this.barrier.await();
        } catch (InterruptedException | BrokenBarrierException ex) {
            Logger.getLogger(ProcessingHub.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public void shutdownHub() {
        this.service.shutdown();
        this.storageManager.shutdown();
        System.exit(0);
    }

    public void init() {
        this.controlCenter.hubInit();
    }

    /**
     * A class to track the number on the interval.
     */
    @NotThreadSafe
    private static final class ProcessingHubNumericalHelper implements Iterable<BigInteger> {

        public static final BigInteger DEFAULT_INCREMENT_VALUE = new BigInteger("10000");

        /**
         * Gets the number of tasks per thread.
         *
         * @param startingNumber The starting number
         * @param finalNumber THe ending number
         * @return The number of tasks per thread
         */
        public static BigInteger getTasksPerThread(BigInteger startingNumber, BigInteger finalNumber) {
            return new BigDecimal(finalNumber.subtract(startingNumber))
                    .divide(new BigDecimal(Runtime.getRuntime().availableProcessors()))
                    .setScale(0, RoundingMode.UP).toBigIntegerExact();
        }

        /**
         * Calculates the number for the iteration of a loop assigning numbers
         * to be computed.
         *
         * @param startingNumber The starting number of the sequence
         * @param tasksPerThread The number of tasks per thread
         * @param index The current index of the loop
         * @return The computed final number to stop at for this loop
         */
        public static BigInteger calculateNumber(BigInteger startingNumber, BigInteger tasksPerThread, BigInteger index) {
            return startingNumber.add(tasksPerThread.multiply(index));
        }
        private final BigInteger startingNumber;
        private final BigInteger endingNumber;
        private final BigInteger incrementValue;
        private final BigInteger tasksPerThread;
        private BigInteger currentNumber;

        /**
         * Constructs a new number tracker with the current position set to the
         * starting position.
         *
         * @param startingNumber The number to start with
         * @param endingNumber The number to end on.
         */
        public ProcessingHubNumericalHelper(BigInteger startingNumber, BigInteger endingNumber) {
            this(startingNumber, endingNumber, DEFAULT_INCREMENT_VALUE);
        }

        /**
         * Constructs a new number tracker with the specified starting, ending,
         * and current position. The value that this tracker is incremented by
         * is also set.
         *
         * @param startingNumber The number to start with
         * @param endingNumber The number to end on.
         * @param incrementValue The increment value
         */
        public ProcessingHubNumericalHelper(BigInteger startingNumber, BigInteger endingNumber, BigInteger incrementValue) {
            this(startingNumber, endingNumber, startingNumber, incrementValue);
        }

        /**
         * Constructs a new number tracker with the specified starting, ending,
         * and current position. The value that this tracker is incremented by
         * is also set.
         *
         * @param startingNumber The number to start with
         * @param endingNumber The number to end on.
         * @param currentNumber The current position
         */
        public ProcessingHubNumericalHelper(BigInteger startingNumber, BigInteger endingNumber, BigInteger currentNumber, BigInteger incrementValue) {
            this.startingNumber = startingNumber;
            this.endingNumber = endingNumber.add(BigInteger.ONE);
            this.currentNumber = currentNumber;
            this.incrementValue = incrementValue;
            this.tasksPerThread = getTasksPerThread(this.startingNumber, this.endingNumber);
        }

        /**
         * Calculates the number for the iteration of a loop assigning numbers
         * to be computed.
         *
         * @param index The current index in the loop
         * @return The number desired
         */
        public BigInteger calculateNumber(BigInteger index) {
            return calculateNumber(this.startingNumber, this.tasksPerThread, endingNumber);
        }

        /**
         * A getter for the tasks per thread = (finalNumber -
         * initialNumber)/processors
         *
         * @return The number of tasks per thread
         */
        public BigInteger getTasksPerThread() {
            return this.tasksPerThread;
        }

        /**
         * Tests whether the current number is equal to the ending number.
         *
         * @return Whether the current number = the ending number
         */
        private boolean isFinished() {
            return this.currentNumber.equals(this.endingNumber);
        }

        /**
         * Gets the next interval stop location. Equal to starting + increment
         * value.
         *
         * @return The number
         */
        public BigInteger getNextEndPoint() {
            BigInteger number = this.currentNumber.add(this.incrementValue);
            if (number.compareTo(this.endingNumber) < 0) {
                return number;
            } else {
                return this.endingNumber;
            }
        }

        /**
         * Increments the internal interval by the set amount, or the default
         * amount.
         */
        public void increment() {
            if (currentNumber.add(this.incrementValue).compareTo(this.endingNumber) <= 0) {
                this.currentNumber = currentNumber.add(this.incrementValue);
            } else {
                this.currentNumber = this.endingNumber;
            }
        }

        /**
         * A getter for the starting number in this interval.
         *
         * @return The starting number
         */
        public BigInteger getStartingNumber() {
            return this.startingNumber;
        }

        /**
         * A getter for the value that the interval will be incremented by each
         * time increment() is called.
         *
         * @return The increment value
         */
        public BigInteger getIncrementValue() {
            return this.incrementValue;
        }

        /**
         * A getter for the ending number in this interval.
         *
         * @return The ending number
         */
        public BigInteger getEndingNumber() {
            return this.endingNumber;
        }

        /**
         * A getter for the current number in this interval.
         *
         * @return The current number
         */
        public BigInteger getCurrentNumber() {
            return this.currentNumber;
        }

        @Override
        public Iterator<BigInteger> iterator() {
            return new Iterator<BigInteger>() {

                @Override
                public boolean hasNext() {
                    return !isFinished();
                }

                @Override
                public BigInteger next() {
                    BigInteger i = getCurrentNumber();
                    increment();
                    return i;
                }
            };
        }

    }

    /**
     * The storage manager for any processing hub. Stores the numbers associated
     * with the hub.
     */
    @NotThreadSafe
    static final class ProcessingHubStorageManager {

        public static final Path CONJECTURE_FOLDER_PATH = Paths.get("M://", "Conjecture Program");
        public static final Path CONJECTURE_OUTPUT_FILE = Paths.get(CONJECTURE_FOLDER_PATH.toString(), "Conjecture Output.Dat");
        private final BlockingQueue<Collection<CollatzSequencer.FinalSequencerReport<? extends Number>>> queue;

        private final ExecutorService executor;
        private final Function<Void, Void> errorShutdown;
        private volatile boolean shutdown;

        public ProcessingHubStorageManager(Function<Void, Void> errorShutdown) {
            try {
                Files.createDirectories(CONJECTURE_FOLDER_PATH);
            } catch (IOException ex) {
                Logger.getLogger(Init.class.getName()).log(Level.SEVERE, null, ex);
                throw new RuntimeException(ex);
            }
            this.queue = new ArrayBlockingQueue<>(2);
            this.errorShutdown = errorShutdown;
            this.executor = Executors.newSingleThreadExecutor((Runnable r) -> new Thread(r, "Processing Hub Storage Manager Thread"));
            this.executor.submit(() -> {
                try (FileChannel channel = FileChannel.open(CONJECTURE_OUTPUT_FILE, StandardOpenOption.TRUNCATE_EXISTING,
                        StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
                    ByteBuffer buffer = ByteBuffer.allocateDirect(4 * 1024);
                    while (!this.shutdown || this.queue.size() != 0) {
                        Collection<CollatzSequencer.FinalSequencerReport<? extends Number>> values = Collections.emptyList();
                        try {
                            values = ProcessingHubStorageManager.this.queue.take();
                        } catch (InterruptedException ex) {
                            Logger.getLogger(Init.class.getName()).log(Level.SEVERE, null, ex);
                        }
                        for (CollatzSequencer.FinalSequencerReport<? extends Number> report : values) {
                            String formattedString = "Initial Value: " + report.getInitialValue() + ""
                                    + " Iterations: " + report.getIterations() + " Result: " + report.getResult() + "\n";
                            buffer.put(formattedString.getBytes());
                            buffer.flip();
                            channel.write(buffer);
                            buffer.clear();
                        }

                    }
                } catch (IOException ex) {
                    Logger.getLogger(ProcessingHub.class.getName()).log(Level.SEVERE, null, ex);
                    this.errorShutdown.apply(null);
                }
            });
        }

        public void shutdown() {
            this.shutdown = true;
            this.executor.shutdown();
        }

        public void saveValues(Collection<CollatzSequencer.FinalSequencerReport<? extends Number>> values) throws InterruptedException {
            this.queue.put(values);
        }

        public long fileSize() {
            try {
                return Files.size(CONJECTURE_OUTPUT_FILE);
            } catch (IOException ex) {
                Logger.getLogger(ProcessingHub.class.getName()).log(Level.SEVERE, null, ex);
                throw new RuntimeException(ex);
            }
        }

    }

    /**
     * An enum representing the state that this <code>TrackingHub</code> is
     * configured to run in. Can be run until a certain number range is
     * completed or run until the partition is full, or run until a certain
     * number of bytes is passed over.
     */
    public static enum HubNumericalState {

        WRITE_UNTIL_DISK_FULL, NUMBER_RANGE
    }

    @NotThreadSafe
    private final class ProcessingHubControlCenter {

        private HubNumericalState state;
        private BigInteger startingNumber, targetNumber;
        private ProcessingHubNumericalHelper numberTracker;

        public void hubInit() {
            if (this.state == null) {
                throw new IllegalStateException("State was never initialized");
            } else if (this.state == HubNumericalState.NUMBER_RANGE) {
                if (this.startingNumber == null || this.targetNumber == null) {
                    throw new IllegalStateException("Starting or ending number is null");
                } else if (this.startingNumber.compareTo(this.targetNumber) >= 0) {
                    throw new IllegalStateException("Starting number is less than ending number");
                }
                this.numberTracker = new ProcessingHubNumericalHelper(startingNumber, targetNumber);
                for (; !numberTracker.isFinished(); numberTracker.increment()) {
                    createThreads(numberTracker.getCurrentNumber(), numberTracker.getNextEndPoint());
                }
            } else if (this.state == HubNumericalState.WRITE_UNTIL_DISK_FULL) {
                this.startingNumber = BigInteger.ONE;
                this.targetNumber = new BigInteger("50000");
                this.numberTracker = new ProcessingHubNumericalHelper(this.startingNumber, this.targetNumber);
                for (long fileSize = storageManager.fileSize(), maxSize = 5L * 1024 * 1024 * 1024; fileSize <= maxSize;
                        fileSize = storageManager.fileSize(), this.numberTracker
                        = new ProcessingHubNumericalHelper(this.numberTracker.getEndingNumber(),
                                this.numberTracker.getStartingNumber().add(this.numberTracker.getEndingNumber()))) {
                    for (; !this.numberTracker.isFinished(); this.numberTracker.increment()) {
                        createThreads(this.numberTracker.getCurrentNumber(), this.numberTracker.getNextEndPoint());
                    }
                }
            }
        }

        public void setState(HubNumericalState state) {
            this.state = state;
        }

        public void setStartingNumber(BigInteger startingNumber) {
            this.startingNumber = startingNumber;
        }

        public void setTargetNumber(BigInteger targetNumber) {
            this.targetNumber = targetNumber;
        }
    }

}
