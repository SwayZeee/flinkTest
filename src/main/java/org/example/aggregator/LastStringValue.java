package org.example.aggregator;

import org.apache.flink.table.functions.AggregateFunction;

public class LastStringValue extends AggregateFunction<String, LastStringValueAccum> {

    @Override
    public String getValue(LastStringValueAccum lastStringValueAccum) {
        return lastStringValueAccum.s.toString();
    }

    @Override
    public LastStringValueAccum createAccumulator() {
        return new LastStringValueAccum();
    }

    public void accumulate(LastStringValueAccum lastStringValueAccum, String value) {
        lastStringValueAccum.s.setLength(0);
        lastStringValueAccum.s.append(value);
    }
}
