#! /usr/bin/env python3
from io import BytesIO

import hazelcast
from hazelcast import HazelcastClient
from hazelcast.serialization.api import IdentifiedDataSerializable, Portable, StreamSerializer
import avro.schema
import avro.io


class Person:
    CLASS_ID = 1

    def __init__(self, name, age):
        self.name = name
        self.age = age


class PersonIdentified(IdentifiedDataSerializable):
    def __init__(self, person=None):
        self.person = person

    @classmethod
    def get_factory_id(cls):
        return 1

    @classmethod
    def get_class_id(cls):
        return Person.CLASS_ID

    def write_data(self, output):
        output.write_utf(self.person.name)
        output.write_int(self.person.age)

    def read_data(self, input):
        name = input.read_utf()
        age = input.read_int()
        self.person = Person(name, age)


class PersonPortable(Portable):
    def __init__(self, person=None):
        self.person = person

    @classmethod
    def get_factory_id(cls):
        return 1

    @classmethod
    def get_class_id(self):
        return Person.CLASS_ID

    def write_portable(self, writer):
        writer.write_utf("name", self.person.name)
        writer.write_int("age", self.person.age)

    def read_portable(self, reader):
        name = reader.read_utf("name")
        age = reader.read_int("age")
        self.person = Person(name, age)


class PersonAvroSerializer(StreamSerializer):
    SCHEMA = avro.schema.parse("""
        {
            "namespace": "org.example",
            "type": "record",
            "name": "Person",
            "fields": [
                {"name": "name", "type": "string"},
                {"name": "age", "type": "int"}
            ]
        }
    """)

    @classmethod
    def get_type_id(cls):
        return 1

    def read(self, input):
        # type: (hazelcast.serialization.api.ObjectDataInput) -> Person
        bio = BytesIO(input.read_byte_array())
        decoder = avro.io.BinaryDecoder(bio)
        reader = avro.io.DatumReader(self.SCHEMA, self.SCHEMA)
        return Person(**reader.read(decoder))

    def write(self, out, person):
        bio = BytesIO()
        encoder = avro.io.BinaryEncoder(bio)
        datum_writer = avro.io.DatumWriter(self.SCHEMA)
        datum_writer.write(person.__dict__, encoder)
        out.write_byte_array(bytearray(bio.getvalue()))

    def destroy(self):
        pass


def main():
    data_serializable_factories = {
        PersonIdentified.get_factory_id(): {
            Person.CLASS_ID: PersonIdentified
        }
    }
    portable_factories = {
        PersonPortable.get_factory_id(): {
            Person.CLASS_ID: PersonPortable
        }
    }
    custom_serializers = {
        Person: PersonAvroSerializer
    }
    hz = HazelcastClient(
        data_serializable_factories=data_serializable_factories,
        portable_factories=portable_factories,
        custom_serializers=custom_serializers
    )
    polyglot_map = hz.get_map("polyglot").blocking()
    polyglot_map.put("python-integer", 1)
    polyglot_map.put("python-float", 3.14)
    polyglot_map.put("python-bool", True)
    polyglot_map.put("python-array", [1, 2, 3])
    polyglot_map.put("python-string", "Hazelcast Rocks!")
    polyglot_map.put("python-identified", PersonIdentified(Person("Ford Prefect", 42)))
    polyglot_map.put("python-portable", PersonPortable(Person("Jane Doe", 25)))
    polyglot_map.put("python-avro", Person("Marvin Minsky", 65))

    java_array = polyglot_map.get("java-array")
    print(f"javaArray: {java_array}")

    nodejs_array = polyglot_map.get("nodejs-array")
    print(f"nodejsArray: {nodejs_array}")

    hz.shutdown()


if __name__ == '__main__':
    main()