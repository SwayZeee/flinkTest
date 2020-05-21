package org.example.aggregator;

import org.apache.flink.table.functions.AggregateFunction;

public class LastDoubleValue extends AggregateFunction<Double, LastDoubleValueAccum> {
    @Override
    public Double getValue(LastDoubleValueAccum lastDoubleValueAccum) {
        return lastDoubleValueAccum.d;
    }

    @Override
    public LastDoubleValueAccum createAccumulator() {
        return new LastDoubleValueAccum();
    }

    public void accumulate(LastDoubleValueAccum lastDoubleValueAccum, double value) {
        lastDoubleValueAccum.d = value;
    }
}
