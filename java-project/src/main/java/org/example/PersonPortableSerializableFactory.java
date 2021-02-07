package org.example;

import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;

public class PersonPortableSerializableFactory implements PortableFactory {
    @Override
    public Portable create(int classId) {
        if (classId == Person.CLASS_ID) {
            return new PersonPortable();
        }
        return null;
    }
}
