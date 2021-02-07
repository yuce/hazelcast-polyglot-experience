package org.example;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

public class PersonIdentified implements IdentifiedDataSerializable {
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
    public void writeData(ObjectDataOutput output) throws IOException {
        output.writeUTF(person.getName());
        output.writeInt(person.getAge());
    }

    @Override
    public void readData(ObjectDataInput input) throws IOException {
        String name = input.readUTF();
        int age = input.readInt();
        person = new Person(name, age);
    }

    @Override
    public String toString() {
        return String.format("PersonIdentified(%s, %d)",
                person.getName(), person.getAge());
    }
}
