package org.example;

import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;

public class PersonPortable implements Portable {
    public static final int FACTORY_ID = 1;
    private Person person;

    @Override
    public int getFactoryId() {
        return FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return Person.CLASS_ID;
    }

    @Override
    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeUTF("name", person.getName());
        writer.writeInt("age", person.getAge());
    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        String name = reader.readUTF("name");
        int age = reader.readInt("age");
        person = new Person(name, age);
    }

    @Override
    public String toString() {
        return String.format("PersonPortable(%s, %d)",
                person.getName(), person.getAge());
    }
}
