/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.gmail.physicistsarah.collatzconjecture.core;

import static java.lang.Math.addExact;
import static java.lang.Math.multiplyExact;
import java.math.BigInteger;
import static java.math.BigInteger.ONE;
import static java.math.BigInteger.ZERO;
import java.util.Comparator;
import java.util.Objects;
import java.util.logging.Logger;
import net.jcip.annotations.Immutable;
import net.jcip.annotations.NotThreadSafe;

/**
 * A sequencer used for computing the Collatz sequence.
 *
 * @author Sarah Szabo <PhysicistSarah@Gmail.com>
 * @version 1.0
 */
@NotThreadSafe
public class CollatzSequencer {

    private static final Logger LOG = Logger.getLogger(CollatzSequencer.class.getName());

    private final boolean ultraLightweight;
    private final BigInteger initialValue;
    private long iterationsLong;
    private BigInteger iterationsBig;
    private String sequence;

    public CollatzSequencer(BigInteger currentValue, boolean ultraLightweight) {
        if (currentValue == null) {
            throw new NullPointerException("Value passed can't be null");
        } else if (currentValue.compareTo(new BigInteger("1")) < 0) {
            throw new NumberFormatException("The value passed to the constructor must be a natural number.");
        }
        this.iterationsLong = 0;
        this.iterationsBig = ZERO;
        this.initialValue = currentValue;
        this.sequence = "";
        this.ultraLightweight = ultraLightweight;
    }

    public FinalSequencerReport<? extends Number> init() {
        try {
            return new FinalSequencerReport<>(this.ultraLightweight ? performCalculationLightweight(this.initialValue.longValueExact())
                    : performCalculationLightweightWithSequence(this.initialValue.longValueExact()), this.iterationsLong,
                    this.sequence, this.initialValue.longValueExact());
        } catch (ArithmeticException e) {
            return new FinalSequencerReport<>(this.ultraLightweight ? performCalculationHeavyweight(this.initialValue)
                    : performCalculationHeavyweightWithSequence(this.initialValue), this.iterationsLong,
                    this.sequence, this.initialValue);
        }
    }

    private BigInteger performCalculationHeavyweight(BigInteger number) {
        if (number.equals(ONE)) {
            return number;
        } else if (number.and(ONE).equals(ZERO)) {
            this.iterationsBig = this.iterationsBig.add(ONE);
            return performCalculationHeavyweight(number.divide(new BigInteger("2")));
        } else {
            this.iterationsBig = this.iterationsBig.add(ONE);
            return performCalculationHeavyweight(number.multiply(new BigInteger("3")).add(ONE));
        }
    }

    private BigInteger performCalculationHeavyweightWithSequence(BigInteger number) {
        if (number.equals(ONE)) {
            this.sequence += this.ultraLightweight ? "" : "= 1";
            return number;
        } else if (number.and(ONE).equals(ZERO)) {
            this.sequence += this.ultraLightweight ? "" : number + " / 2 = " + number.divide(BigInteger.valueOf(2)) + " -> ";
            this.iterationsBig = this.iterationsBig.add(ONE);
            return performCalculationHeavyweightWithSequence(number.divide(new BigInteger("2")));
        } else {
            this.sequence += this.ultraLightweight ? "" : number + " * 3 + 1 = "
                    + number.multiply(new BigInteger("3")).add(ONE) + " -> ";
            this.iterationsBig = this.iterationsBig.add(ONE);
            return performCalculationHeavyweightWithSequence(number.multiply(new BigInteger("3")).add(ONE));
        }
    }

    private long performCalculationLightweight(long number) {
        if (number == 1) {
            return number;
        } else if ((number & 1) == 0) {
            this.iterationsLong++;
            return performCalculationLightweight(number / 2);
        } else {
            this.iterationsLong++;
            return performCalculationLightweight((number * 3) + 1);
        }
    }

    private long performCalculationLightweightWithSequence(long number) {
        if (number == 1) {
            this.sequence += this.ultraLightweight ? "" : "= 1";
            this.sequence = "Lightweight\n" + this.sequence;
            return number;
        } else if ((number & 1) == 0) {
            this.iterationsLong++;
            this.sequence += this.ultraLightweight ? "" : number + " / 2 = " + number / 2 + " -> ";
            return performCalculationLightweightWithSequence(number / 2);
        } else {
            this.iterationsLong++;
            this.sequence += this.ultraLightweight ? "" : number + " * 3 + 1 = " + ((number * 3) + 1) + " -> ";
            return performCalculationLightweightWithSequence((number * 3) + 1);
        }
    }

    private SequencerReport<Long> performCalculationLightweight(SequencerReport<Long> report) {
        if (report.getResult() == 1) {
            return new SequencerReport<>(report.getResult(), report.getIterations(), this.ultraLightweight ? "" : !report.getSequence().isEmpty()
                    ? report.getSequence().substring(0, report.getSequence().length() - 3) : "The sequence starts and ends at 1 <Nothing Done>");
        } else if ((report.getResult() & 1) == 0) {
            long value = report.getResult() / 2;
            return performCalculationLightweight(new SequencerReport<>(value, addExact(report.getIterations(), 1),
                    this.ultraLightweight ? "" : report.getSequence() + " " + report.getResult() + "/2 -> " + value + " ->"));
        } else {
            long value = addExact(multiplyExact(report.getResult(), 3), 1);
            return performCalculationLightweight(new SequencerReport<>(value, report.getIterations() + 1, this.ultraLightweight
                    ? "" : report.getSequence() + report.getResult() + " * 3 + 1 ->" + value + " ->"));
        }
    }

    private SequencerReport<BigInteger> performCalculationBig(SequencerReport<BigInteger> report) {
        if (report.getResult().equals(new BigInteger("1"))) {
            return new SequencerReport<>(report.getResult(), report.getIterations(), this.ultraLightweight ? ""
                    : !report.getSequence().isEmpty() ? report.getSequence().substring(0, report.getSequence().length() - 3)
                            : "The sequence starts and ends at 1 <Nothing Done>");
        } else if (report.getResult().and(new BigInteger("1")).equals(new BigInteger("0"))) {
            BigInteger value = report.getResult().divide(new BigInteger("2"));
            return performCalculationBig(new SequencerReport<>(value, report.getIterations().add(new BigInteger("1")),
                    this.ultraLightweight ? "" : report.getSequence() + " " + report.getResult() + "/2 -> " + value + " ->"));
        } else {
            BigInteger value = report.getResult().multiply(new BigInteger("3")).add(new BigInteger("1"));
            return performCalculationBig(new SequencerReport<>(value, report.getIterations()
                    .add(new BigInteger("1")), this.ultraLightweight ? ""
                            : report.getSequence() + report.getResult() + " * 3 + 1 ->" + value + " ->"));
        }
    }

    @Immutable
    public static final class FinalSequencerReport<T extends Number> extends SequencerReport<T> implements Comparable<FinalSequencerReport<T>> {

        public static Comparator<? super FinalSequencerReport<? extends Number>> compareByInitialValue() {
            return (FinalSequencerReport<? extends Number> o1, FinalSequencerReport<? extends Number> o2)
                    -> new BigInteger(o1.getInitialValue().toString()).compareTo(new BigInteger(o2.getInitialValue().toString()));
        }
        private final T initialValue;

        public FinalSequencerReport(SequencerReport<T> finalReport, T initialValue) {
            this(finalReport.getResult(), finalReport.getIterations(), finalReport.getSequence(), initialValue);
        }

        public FinalSequencerReport(T result, T iterations, String sequence, T initialValue) {
            super(result, iterations, sequence);
            this.initialValue = initialValue;
        }

        public Number getInitialValue() {
            return this.initialValue;
        }

        @Override
        public int compareTo(FinalSequencerReport<T> o) {
            return FinalSequencerReport.compareByInitialValue().compare(this, o);
        }

        @Override
        public int hashCode() {
            int hash = 3;
            hash = 41 * hash + Objects.hashCode(this.initialValue);
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final FinalSequencerReport<?> other = (FinalSequencerReport<?>) obj;
            return Objects.equals(this.initialValue, other.getInitialValue());
        }

        @Override
        public String toString() {
            return "Initial Value: "
                    + getInitialValue() + "\nFinal Value: " + getResult() + "\nIterations:  "
                    + getIterations() + (getSequence().isEmpty() ? "" : "\nAlgebraic Sequence:\n\n" + getSequence());
        }
    }

    @Immutable
    public static class SequencerReport<T extends Number> {

        private final T result, iterations;
        private final String sequence;

        public SequencerReport(T result, T iterations) {
            this(result, iterations, "");
        }

        public SequencerReport(T result, T iterations, String sequence) {
            this.result = result;
            this.iterations = iterations;
            this.sequence = sequence;
        }

        public T getResult() {
            return this.result;
        }

        public T getIterations() {
            return this.iterations;
        }

        public String getSequence() {
            return this.sequence;
        }
    }
}
