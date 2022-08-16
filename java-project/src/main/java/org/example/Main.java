package org.example;

import com.hazelcast.aggregation.impl.AggregatorDataSerializerHook;
import com.hazelcast.aggregation.impl.CountAggregator;
import com.hazelcast.aggregation.impl.MaxByAggregator;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.EntryView;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.map.IMap;
import com.hazelcast.topic.ITopic;
import com.hazelcast.topic.impl.TopicProxy;

import java.math.BigInteger;
import java.util.Collection;
import java.util.Collections;


public class Main {
    public static void main7(String[] args) {
        BigInteger bi = new BigInteger("1000");
        byte[] b = bi.toByteArray();
        System.out.println(b.length);
        System.out.println("-");
        for (byte value : b) {
            System.out.println(value);
        }
    }

    public static void main(String[] args) {
        ClientConfig config = new ClientConfig();
        config.getNetworkConfig().setAddresses(Collections.singletonList("localhost:15701"));
        HazelcastInstance hazelcastInstance = HazelcastClient.newHazelcastClient(config);

        Collection<DistributedObject> distributedObjects = hazelcastInstance.getDistributedObjects();
        for (DistributedObject object : distributedObjects) {
            if (object instanceof IMap) {
                IMap<Object, Object> map = hazelcastInstance.getMap(object.getName());
                System.out.println("Mapname=" + map.getName());
                map.entrySet().forEach(System.out::println);
            }
        }

//        hazelcastInstance.shutdown();
    }

    public static void main6(String[] args) {
        HazelcastInstance hz = HazelcastClient.newHazelcastClient();
        HazelcastJsonValue j1 = new HazelcastJsonValue("{}");
        IMap<String, Object> m = hz.getMap("m10");
        m.put("k1", j1);
        CountAggregator<Object> counter = new CountAggregator<>("Z");
        long count = m.aggregate(counter);
        System.out.println(count);
        MaxByAggregator<Object> maxBy = new MaxByAggregator<>("Z");
        hz.shutdown();
    }
    public static void main5(String[] args) throws InterruptedException {
        ClientConfig config = new ClientConfig();
        config.getNetworkConfig().getSSLConfig();
        config.getMetricsConfig().setEnabled(true);
        config.getSecurityConfig().setUsernamePasswordIdentityConfig("foo", "");
        config.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(1000);
//        config.getNetworkConfig().setConnectionTimeout(1000);
        HazelcastInstance hz = HazelcastClient.newHazelcastClient(config);
        IMap<Integer, String> m = hz.getMap("foo1");
        m.put(100, "foo");
        IMap<String, String> m2 = hz.getMap("foo1");
        EntryView<String, String> e2 = m2.getEntryView("100");
        System.out.println(e2);
        Thread.sleep(1000000);
        hz.shutdown();
    }
    public static void main4(String[] args) {
        HazelcastInstance hz = HazelcastClient.newHazelcastClient();
        byte[] b = new byte[1024 * 1024];
        for (int i = 0; i < b.length; i++) {
            b[i] = (byte)i;
        }
        IMap<String, byte[]> m = hz.getMap("foo");
        long tic = System.currentTimeMillis();
        m.put("k1", b);
        long toc = System.currentTimeMillis();
        long putTook = toc - tic;
        tic = System.currentTimeMillis();
        byte[] r = m.get("k1");
        toc = System.currentTimeMillis();
        long getTook = toc - tic;
        System.out.printf("put: %d ms%n", putTook);
        System.out.printf("get: %d ms%n", getTook);
        hz.shutdown();
    }

    public static void main3(String[] args) throws InterruptedException {
        ClientConfig config = new ClientConfig();
        config.getNetworkConfig().setSmartRouting(false);
        HazelcastInstance hz = HazelcastClient.newHazelcastClient(config);
        ITopic<String> topic = hz.getTopic("foo");
        topic.addMessageListener(listener -> {
            System.out.println("== " + listener.toString());
        });
        for (int i = 0; i < 10; i++) {
            topic.publish(String.format("msg:%d", i));
        }
        Thread.sleep(1000);
        hz.shutdown();
    }
    public static void main2(String[] args) {
        System.out.println(AggregatorDataSerializerHook.F_ID);
        /*
        HazelcastInstance hz = HazelcastClient.newHazelcastClient();
        IMap<Object, Object> m = hz.getMap("mymap");
        long tic = System.currentTimeMillis();
        int putCount = 100000;
        for (int i = 0; i < putCount; i++) {
            String key = "k" + i;
            String value = "v" + i;
            m.set(key, value);
        }
        long toc = System.currentTimeMillis();
        System.out.printf("\n\n== Took: %d millis\n", toc - tic);

        hz.shutdown();

         */
    }

    public static void main1(String[] args) {
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
        config.setClusterName("jet");
        HazelcastInstance hz = null;
        try {

            hz = HazelcastClient.newHazelcastClient(config);
            /*
            IMap<String, Object> polyglotMap = hz.getMap("polyglot");

            Object pythonInteger = polyglotMap.get("python-integer");
            System.out.printf("pythonInteger: %s%n", pythonInteger);

            Object pythonBool = polyglotMap.get("python-bool");
            System.out.printf("pythonBool: %s%n", pythonBool);

            Object pythonList = polyglotMap.get("python-list");
            System.out.printf("pythonList: %s%n", pythonList);

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

            IMap<Integer, Object> polyglotMap2 = hz.getMap("polyglot2");
            polyglotMap2.set(0, "foo");

            Object nodejsArray = polyglotMap.get("nodejs-array");
            System.out.printf("javaArray: %s%n", nodejsArray);
             */
            hz.getSql().execute("CREATE MAPPING csv_likes (id INT, name VARCHAR, likes INT)\n" +
                    "TYPE File\n" +
                    "OPTIONS ('format'='csv',\n" +
                    "    'path'='/csv-dir', 'glob'='likes.csv')\n" );
            hz.shutdown();
        } finally {
            if (hz != null) {
                hz.shutdown();
            }
        }
    }
}
