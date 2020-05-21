package org.example.aggregator;

import org.apache.flink.table.functions.AggregateFunction;

public class LastIntegerValue extends AggregateFunction<Integer, LastIntegerValueAccum> {

    @Override
    public Integer getValue(LastIntegerValueAccum lastIntegerValueAccum) {
        return lastIntegerValueAccum.i;
    }

    @Override
    public LastIntegerValueAccum createAccumulator() {
        return new LastIntegerValueAccum();
    }

    public void accumulate(LastIntegerValueAccum lastIntegerValueAccum, int value) {
        lastIntegerValueAccum.i = value;
    }
}
