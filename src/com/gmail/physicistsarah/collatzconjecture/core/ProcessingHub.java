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
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.validation.constraints.NotNull;
import net.jcip.annotations.NotThreadSafe;

/**
 * A class that controls the environment in which the Collatz Algorithm is
 * executed.
 *
 * @author Sarah Szabo <PhysicistSarah@Gmail.com>
 */
public final class ProcessingHub {

    private static final int AVAILABLE_PROCESSORS = Runtime.getRuntime().availableProcessors();
    private static final Logger LOG = Logger.getLogger(ProcessingHub.class.getName());

    private final ExecutorService service = Executors.newFixedThreadPool(AVAILABLE_PROCESSORS, new ThreadFactory() {
        private BigInteger count = BigInteger.ZERO;

        @Override
        public Thread newThread(Runnable r) {
            count = count.add(BigInteger.ONE);
            return new Thread(r, "Collatz Sequencer Thread: " + count);
        }
    });
    private final HubControlState controlState;
    private final HubStorageManager storageManager;

    /**
     * Constructs a new {@link ProcessingHub} with the specified maximum disk
     * storage space.
     *
     * @param bytes The amount of storage space, in bytes
     * @throws IOException If an IOException occurred
     */
    public ProcessingHub(long bytes) throws IOException {
        this.controlState = new WriteUntilSize(bytes);
        this.storageManager = new HubStorageManager();
    }

    /**
     * Constructs a new {@link ProcessingHub} with the specified bounds.
     *
     * @param startingNumber The starting number
     * @param endingNumber THe ending number
     * @throws IOException If an IOExceptio occurred
     */
    public ProcessingHub(BigInteger startingNumber, BigInteger endingNumber) throws IOException {
        this.controlState = new NumberRangeState(startingNumber, endingNumber);
        this.storageManager = new HubStorageManager();
    }

    /**
     * Creates and launches tasks for each number between the number range
     * specified.
     *
     * @param startNumber The first number
     * @param finishNumber The final number
     */
    private void createTask(@NotNull BigInteger startNumber, @NotNull BigInteger finishNumber) {
        if (finishNumber.subtract(startNumber).compareTo(BigInteger.ZERO) < 0) {
            throw new IllegalArgumentException("The starting number is larger than the final number.");
        } else if (startNumber.compareTo(BigInteger.ZERO) < 0) {
            throw new IllegalArgumentException("The starting number is less than zero");
        }
        for (BigInteger i = startNumber; i.compareTo(finishNumber) < 0; i = i.add(BigInteger.ONE)) {
            final BigInteger copy = i;
            this.service.submit(() -> {
                try {
                    this.storageManager.saveValue(new CollatzSequencer(copy, true).init());
                } catch (IOException ex) {
                    Logger.getLogger(ProcessingHub.class.getName()).log(Level.SEVERE, null, ex);
                } catch (InterruptedException ex) {
                    Logger.getLogger(ProcessingHub.class.getName()).log(Level.SEVERE, null, ex);
                }
            });
        }
    }

    /**
     * Shuts down the {@link ProcessingHub} and waits for the termination of the
     * executor.
     *
     * @throws IOException
     */
    public void shutdownHub() throws IOException {
        try {
            this.service.shutdown();
            this.service.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
            this.storageManager.shutdown();
        } catch (InterruptedException ex) {
            Logger.getLogger(ProcessingHub.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /**
     * Initiates the {@code ProcessingHub} in whatever mode the
     * {@link ProcessingHub} is configured in.
     *
     * @throws java.io.IOException If there was an error with creating the file
     */
    public void hubInit() throws IOException {
        this.controlState.onHubInit();
        shutdownHub();
    }

    /**
     * A class to track the number on the interval.
     */
    @NotThreadSafe
    private static final class HubNumericalHelper implements Iterable<BigInteger> {

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
        public HubNumericalHelper(BigInteger startingNumber, BigInteger endingNumber) {
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
        public HubNumericalHelper(BigInteger startingNumber, BigInteger endingNumber, BigInteger incrementValue) {
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
        public HubNumericalHelper(BigInteger startingNumber, BigInteger endingNumber, BigInteger currentNumber, BigInteger incrementValue) {
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
                    if (hasNext()) {
                        BigInteger i = getCurrentNumber();
                        increment();
                        return i;
                    } else {
                        throw new NoSuchElementException("No more elements in iterator.");
                    }
                }
            };
        }

    }

    /**
     * The storage manager for any processing hub. Stores the numbers associated
     * with the hub.
     */
    @NotThreadSafe
    private static final class HubStorageManager {

        public static final Path CONJECTURE_FOLDER_PATH = Paths.get("M://", "Conjecture Program");
        public static final Path CONJECTURE_OUTPUT_FILE = Paths.get(CONJECTURE_FOLDER_PATH.toString(), "Conjecture Output.Dat");
        public static final String POISON = "Initiate Queue Shutdown";
        private final ExecutorService executor;
        private final FileChannel channel;
        private final ByteBuffer buffer;
        private final BlockingQueue<String> queue;

        /**
         * Constructs a new {@link HubStorageManager}, responsible for all IO
         * operations in the {@link ProcessingHub}.
         *
         * @throws IOException If an IOException occurred
         */
        public HubStorageManager() throws IOException {
            try {
                Files.createDirectories(CONJECTURE_FOLDER_PATH);
            } catch (IOException ex) {
                Logger.getLogger(Init.class.getName()).log(Level.SEVERE, null, ex);
                throw new RuntimeException(ex);
            }
            this.queue = new ArrayBlockingQueue<>(100);
            this.buffer = ByteBuffer.allocateDirect(1024 * 32);
            this.channel = FileChannel.open(CONJECTURE_OUTPUT_FILE, StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            this.executor = Executors.newSingleThreadExecutor((Runnable r) -> new Thread(r, "Processing Hub Storage Manager Thread"));
            this.executor.submit(() -> {
                String message = "";
                while (true) {
                    try {
                        String string = this.queue.take() + "\n\n";
                        if (string.equalsIgnoreCase(HubStorageManager.POISON + "\n\n")) {
                            writeString(message);
                            break;
                        } else if (message.length() >= 1000) {
                            writeString(message);
                            message = string;
                        } else {
                            message += string;
                        }
                    } catch (IOException ex) {
                        Logger.getLogger(Init.class.getName()).log(Level.SEVERE, null, ex);
                        break;
                    } catch (InterruptedException ex) {
                        Logger.getLogger(ProcessingHub.class.getName()).log(Level.SEVERE, null, ex);
                        break;
                    }
                }
            });
        }

        private void writeString(String message) throws IOException {
            this.buffer.put(message.getBytes(Charset.forName("UTF-16")));
            this.buffer.flip();
            this.channel.write(this.buffer);
            this.buffer.clear();
        }

        /**
         * Shuts down this processing hub.
         */
        public void shutdown() throws IOException, InterruptedException {
            this.queue.put(POISON);
            this.executor.shutdown();
            this.executor.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
            this.channel.close();
        }

        /**
         * Saves the current value to the disk.
         *
         * @param value The value to save
         * @throws InterruptedException If interrupted while waiting
         */
        public void saveValue(CollatzSequencer.FinalSequencerReport<? extends Number> value) throws IOException, InterruptedException {
            this.queue.put(value.toString());
        }

        /**
         * A getter for the current size of the output file.
         *
         * @return THe file size in bytes
         */
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
     * A state class representing the state that will write to the disk until a
     * certain amount of bytes has been written.
     */
    @NotThreadSafe
    private final class WriteUntilSize implements HubControlState {

        private HubNumericalHelper numberTracker;
        private final long MAX_SIZE;

        /**
         * Constructs a new {@link WriteUntilSize} with the specified amount of
         * bytes.
         *
         * @param bytes The amount of bytes; max
         * @throws IOException If an IOException occurred
         */
        public WriteUntilSize(long bytes) throws IOException {
            this.numberTracker = new HubNumericalHelper(BigInteger.ONE, new BigInteger("50000"));
            this.MAX_SIZE = bytes;
        }

        @Override
        public void onHubInit() throws IOException {
            for (long fileSize = storageManager.fileSize(); fileSize <= this.MAX_SIZE;
                    fileSize = storageManager.fileSize(), this.numberTracker
                    = new HubNumericalHelper(this.numberTracker.getEndingNumber(),
                            this.numberTracker.getStartingNumber().add(this.numberTracker.getEndingNumber()))) {
                for (; !this.numberTracker.isFinished(); this.numberTracker.increment()) {
                    createTask(this.numberTracker.getCurrentNumber(), this.numberTracker.getNextEndPoint());
                }
            }
        }
    }

    /**
     * A class representing the state for number ranges.
     */
    @NotThreadSafe
    private final class NumberRangeState implements HubControlState {

        private final BigInteger startingNumber, targetNumber;
        private final HubNumericalHelper numberTracker;

        /**
         * Constructs a new number range with specified starting number and
         * ending number.
         *
         * @param startingNumber The starting number
         * @param targetNumber The ending number
         */
        public NumberRangeState(@NotNull BigInteger startingNumber, @NotNull BigInteger targetNumber) throws IOException {
            if (startingNumber.compareTo(targetNumber) > 0) {
                throw new IllegalArgumentException("The starting number is larger than the ending number.");
            } else if (startingNumber.equals(targetNumber)) {
                throw new IllegalArgumentException("The starting number and ending numbers are equal.");
            }
            this.startingNumber = startingNumber;
            this.targetNumber = targetNumber;
            this.numberTracker = new HubNumericalHelper(startingNumber, targetNumber);
        }

        @Override
        public void onHubInit() throws IOException {
            for (; !this.numberTracker.isFinished(); this.numberTracker.increment()) {
                createTask(this.numberTracker.getCurrentNumber(), this.numberTracker.getNextEndPoint());
            }
        }

    }

    /**
     * The super interface of the state pattern for
     * {@link HubControlCenter#hubInit(com.gmail.physicistsarah.collatzconjecture.core.ProcessingHub)}
     */
    private static interface HubControlState {

        /**
         * Implementation specific method for control center initiation.
         */
        public void onHubInit() throws IOException;
    }
}
