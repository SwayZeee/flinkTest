package org.example.deserialization;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class EventMessageDeserializationSchema implements DeserializationSchema<EventMessage> {

    @Override
    public EventMessage deserialize(byte[] bytes) throws IOException {
        return EventMessage.fromString(new String(bytes));
    }

    @Override
    public boolean isEndOfStream(EventMessage eventMessage) {
        return false;
    }

    @Override
    public TypeInformation<EventMessage> getProducedType() {
        return TypeInformation.of(EventMessage.class);
    }
}
