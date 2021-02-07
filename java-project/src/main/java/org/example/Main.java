package org.example;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

public class Main {
    public static void main(String[] args) {
        SerializerConfig personAvroSerializerConfig = new SerializerConfig()
                .setImplementation(new PersonAvroSerializer())
                .setTypeClass(Person.class);
        ClientConfig config = new ClientConfig();
        config.getSerializationConfig()
                .addDataSerializableFactoryClass(
                        PersonIdentified.FACTORY_ID,
                        PersonIdentifiedSerializableFactory.class
                )
                .addPortableFactoryClass(
                        PersonPortable.FACTORY_ID,
                        PersonPortableSerializableFactory.class
                )
                .addSerializerConfig(personAvroSerializerConfig);
        HazelcastInstance hz = null;
        try {
            hz = HazelcastClient.newHazelcastClient(config);
            IMap<String, Object> polyglotMap = hz.getMap("polyglot");

            Object pythonInteger = polyglotMap.get("python-integer");
            System.out.printf("pythonInteger: %s%n", pythonInteger);

            Object pythonBool = polyglotMap.get("python-bool");
            System.out.printf("pythonBool: %s%n", pythonBool);

            Object pythonList = polyglotMap.get("python-list");
            System.out.printf("pythonList: %s%n", pythonList);
            System.out.printf("pythonList type: %s%n", pythonList.getClass().toString());

            Object pythonString = polyglotMap.get("python-string");
            System.out.printf("pythonString: %s%n", pythonString);

            Object pythonIdentified = polyglotMap.get("python-identified");
            System.out.printf("pythonIdentified: %s%n", pythonIdentified);

            Object pythonPortable = polyglotMap.get("python-portable");
            System.out.printf("pythonPortable: %s%n", pythonPortable);

            Object pythonAvro = polyglotMap.get("python-avro");
            System.out.printf("pythonAvro: %s%n", pythonAvro);

            Object javaIntegerArray = new int[]{10, 20, 30};
            polyglotMap.put("java-array", javaIntegerArray);

            Object nodejsArray = polyglotMap.get("nodejs-array");
            System.out.printf("javaArray: %s%n", nodejsArray);
        } finally {
            if (hz != null) {
                hz.shutdown();
            }
        }
    }
}
