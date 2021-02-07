package org.example;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.util.Utf8;

import java.io.IOException;
import java.io.InputStream;

public class PersonAvroSerializer implements StreamSerializer<Person> {
    private static final Schema SCHEMA;
    private final DatumReader<GenericRecord> datumReader;
    private GenericRecord record;

    PersonAvroSerializer() {
        datumReader = new GenericDatumReader<>(SCHEMA);
        record = new GenericData.Record(SCHEMA);
    }

    @Override
    public void write(ObjectDataOutput out, Person object) throws IOException {

    }

    @Override
    public Person read(ObjectDataInput in) throws IOException {
        BinaryDecoder decoder =
                DecoderFactory.get().binaryDecoder(in.readByteArray(), null);
        record = datumReader.read(record, decoder);
        return new Person(((Utf8)record.get("name")).toString(), (int)record.get("age"));
    }

    @Override
    public int getTypeId() {
        return 1;
    }

    static {
        InputStream schemaStream = PersonAvroSerializer.class
                .getClassLoader()
                .getResourceAsStream("person.avsc");
        if (schemaStream == null) {
            throw new RuntimeException("Schema not found");
        }
        try {
            SCHEMA = new Schema.Parser().parse(schemaStream);
        } catch (IOException e) {
            throw new RuntimeException("Error reading schema", e);
        }
    }
}
