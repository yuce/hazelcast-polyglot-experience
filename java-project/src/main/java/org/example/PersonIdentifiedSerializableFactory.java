package org.example;

import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

public class PersonIdentifiedSerializableFactory implements DataSerializableFactory {
    @Override
    public IdentifiedDataSerializable create(int typeId) {
        if (typeId == Person.CLASS_ID) {
            return new PersonIdentified();
        }
        return null;
    }
}
