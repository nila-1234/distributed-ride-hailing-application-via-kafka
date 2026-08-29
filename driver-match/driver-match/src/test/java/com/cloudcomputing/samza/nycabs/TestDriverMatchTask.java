package com.cloudcomputing.samza.nycabs;

import java.time.Duration;
import java.util.Collections;
import java.util.ListIterator;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;

import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.test.framework.TestRunner;
import org.apache.samza.test.framework.system.descriptors.InMemoryInputDescriptor;
import org.apache.samza.test.framework.system.descriptors.InMemoryOutputDescriptor;
import org.apache.samza.test.framework.system.descriptors.InMemorySystemDescriptor;
import org.junit.Assert;
import org.junit.Test;
import com.cloudcomputing.samza.nycabs.application.DriverMatchTaskApplication;

public class TestDriverMatchTask {
    @Test
    public void testDriverMatchTask() throws Exception {
        Map<String, String> confMap = new HashMap<>();
        confMap.put("stores.driver-loc.factory", "org.apache.samza.storage.kv.RocksDbKeyValueStorageEngineFactory");
        confMap.put("stores.driver-loc.key.serde", "string");
        confMap.put("stores.driver-loc.msg.serde", "json");
        confMap.put("serializers.registry.json.class", "org.apache.samza.serializers.JsonSerdeFactory");
        confMap.put("serializers.registry.string.class", "org.apache.samza.serializers.StringSerdeFactory");

        InMemorySystemDescriptor isd = new InMemorySystemDescriptor("kafka");

        InMemoryInputDescriptor imdriverLocation = isd.getInputDescriptor("driver-locations", new NoOpSerde<>());

        InMemoryInputDescriptor imevents = isd.getInputDescriptor("events", new NoOpSerde<>());

        InMemoryOutputDescriptor outputMatchStream = isd.getOutputDescriptor("match-stream", new NoOpSerde<>());

        TestRunner
                .of(new DriverMatchTaskApplication())
                .addInputStream(imevents, TestUtils.genStreamData("events"))
                .addInputStream(imdriverLocation, TestUtils.genStreamData("driver-locations"))
                .addOutputStream(outputMatchStream, 1)
                .addConfig(confMap)
                .addConfig("deploy.test", "true")
                .run(Duration.ofSeconds(5));

        Assert.assertEquals(5, TestRunner.consumeStream(outputMatchStream, Duration.ofSeconds(10)).get(0).size());

        ListIterator<Object> resultIter = TestRunner.consumeStream(outputMatchStream, Duration.ofSeconds(10)).get(0).listIterator();
        Map<String, Object> genderTest = (Map<String, Object>) resultIter.next();

        Assert.assertTrue(genderTest.get("clientId").toString().equals("3")
                && genderTest.get("driverId").toString().equals("9001"));

        Map<String, Object> salaryTest = (Map<String, Object>) resultIter.next();
        Assert.assertTrue(salaryTest.get("clientId").toString().equals("4")
                && salaryTest.get("driverId").toString().equals("8000"));

        Map<String, Object> ratingTest = (Map<String, Object>) resultIter.next();
        Assert.assertTrue(ratingTest.get("clientId").toString().equals("5")
                && ratingTest.get("driverId").toString().equals("8000"));
        Map<String, Object> distanceTest = (Map<String, Object>) resultIter.next();
        Assert.assertTrue(distanceTest.get("clientId").toString().equals("6")
                && distanceTest.get("driverId").toString().equals("7001"));
        Map<String, Object> rightBlockTest = (Map<String, Object>) resultIter.next();
        System.out.println(rightBlockTest.get("clientId").toString());
        System.out.println(rightBlockTest.get("driverId").toString());
        Assert.assertTrue(rightBlockTest.get("clientId").toString().equals("7")
                        && rightBlockTest.get("driverId").toString().equals("3002"));
    }

    @Test
    public void testMultipleDriversSameBlock() throws Exception {
        Map<String, String> confMap = new HashMap<>();
        confMap.put("stores.driver-loc.factory", "org.apache.samza.storage.kv.RocksDbKeyValueStorageEngineFactory");
        confMap.put("stores.driver-loc.key.serde", "string");
        confMap.put("stores.driver-loc.msg.serde", "json");
        confMap.put("serializers.registry.json.class", "org.apache.samza.serializers.JsonSerdeFactory");
        confMap.put("serializers.registry.string.class", "org.apache.samza.serializers.StringSerdeFactory");

        InMemorySystemDescriptor isd = new InMemorySystemDescriptor("kafka");

        InMemoryInputDescriptor<Map<String, Object>> eventInput = isd.getInputDescriptor("events", new NoOpSerde<>());
        InMemoryOutputDescriptor<Map<String, Object>> output = isd.getOutputDescriptor("match-stream", new NoOpSerde<>());

        InMemoryInputDescriptor<Map<String, Object>> dummyDriverLocInput = isd.getInputDescriptor("driver-locations", new NoOpSerde<>());

        ArrayList<Map<String, Object>> events = new ArrayList<>();

        Map<String, Object> driver1 = new HashMap<>();
        driver1.put("blockId", 1234);
        driver1.put("driverId", 101);
        driver1.put("latitude", 10.0);
        driver1.put("longitude", 10.0);
        driver1.put("type", "ENTERING_BLOCK");
        driver1.put("status", "AVAILABLE");
        driver1.put("rating", 4.5);
        driver1.put("salary", 60);
        driver1.put("gender", "M");

        /* Closer to client */
        Map<String, Object> driver2 = new HashMap<>();
        driver2.put("blockId", 1234);
        driver2.put("driverId", 102);
        driver2.put("latitude", 10.1);
        driver2.put("longitude", 10.1);
        driver2.put("type", "ENTERING_BLOCK");
        driver2.put("status", "AVAILABLE");
        driver2.put("rating", 4.2);
        driver2.put("salary", 55);
        driver2.put("gender", "M");

        Map<String, Object> request = new HashMap<>();
        request.put("blockId", 1234);
        request.put("clientId", 301);
        request.put("latitude", 10.3);
        request.put("longitude", 10.3);
        request.put("type", "RIDE_REQUEST");
        request.put("gender_preference", "N");

        events.add(driver1);
        events.add(driver2);
        events.add(request);

        TestRunner
                .of(new DriverMatchTaskApplication())
                .addInputStream(eventInput, events)
                .addInputStream(dummyDriverLocInput, Collections.emptyList())
                .addOutputStream(output, 1)
                .addConfig(confMap)
                .addConfig("deploy.test", "true")
                .run(Duration.ofSeconds(5));

        ListIterator<Object> resultIter = TestRunner.consumeStream(output, Duration.ofSeconds(10)).get(0).listIterator();
        Assert.assertTrue(resultIter.hasNext());

        Map<String, Object> match = (Map<String, Object>) resultIter.next();
        Assert.assertEquals("301", match.get("clientId").toString());
        Assert.assertEquals("102", match.get("driverId").toString());
    }

    @Test
    public void testNoDriverAvailable() throws Exception {
        Map<String, String> confMap = new HashMap<>();
        confMap.put("stores.driver-loc.factory", "org.apache.samza.storage.kv.RocksDbKeyValueStorageEngineFactory");
        confMap.put("stores.driver-loc.key.serde", "string");
        confMap.put("stores.driver-loc.msg.serde", "json");
        confMap.put("serializers.registry.json.class", "org.apache.samza.serializers.JsonSerdeFactory");
        confMap.put("serializers.registry.string.class", "org.apache.samza.serializers.StringSerdeFactory");

        InMemorySystemDescriptor isd = new InMemorySystemDescriptor("kafka");

        InMemoryInputDescriptor<Map<String, Object>> eventInput = isd.getInputDescriptor("events", new NoOpSerde<>());
        InMemoryInputDescriptor<Map<String, Object>> driverLocationInput = isd.getInputDescriptor("driver-locations", new NoOpSerde<>());
        InMemoryOutputDescriptor<Map<String, Object>> output = isd.getOutputDescriptor("match-stream", new NoOpSerde<>());

        ArrayList<Map<String, Object>> events = new ArrayList<>();

        Map<String, Object> request = new HashMap<>();
        request.put("blockId", 4321);
        request.put("clientId", 999);
        request.put("latitude", 20.0);
        request.put("longitude", 20.0);
        request.put("type", "RIDE_REQUEST");
        request.put("gender_preference", "N");

        events.add(request);

        TestRunner
                .of(new DriverMatchTaskApplication())
                .addInputStream(eventInput, events)
                .addInputStream(driverLocationInput, Collections.emptyList()) // driver location is empty
                .addOutputStream(output, 1)
                .addConfig(confMap)
                .addConfig("deploy.test", "true")
                .run(Duration.ofSeconds(5));

        /* Output stream should be empty */
        ListIterator<Object> resultIterator = TestRunner.consumeStream(output, Duration.ofSeconds(10)).get(0).listIterator();
        Assert.assertFalse("No match should be emitted when no driver is available", resultIterator.hasNext());
    }


}