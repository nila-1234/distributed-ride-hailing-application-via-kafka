package com.cloudcomputing.samza.nycabs;


import com.cloudcomputing.samza.nycabs.application.AdPriceTaskApplication;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.test.framework.TestRunner;
import org.apache.samza.test.framework.system.descriptors.InMemoryInputDescriptor;
import org.apache.samza.test.framework.system.descriptors.InMemoryOutputDescriptor;
import org.apache.samza.test.framework.system.descriptors.InMemorySystemDescriptor;
import org.junit.Assert;
import org.junit.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.ListIterator;
import java.util.Map;


public class TestAdPriceTask {
    @Test
    public void testAdPriceTask() throws Exception {
        // TODO: write your own test cases (at least 2)
        Map<String, String> confMap = new HashMap<>();
        confMap.put("stores.ad-info.factory", "org.apache.samza.storage.kv.RocksDbKeyValueStorageEngineFactory");
        confMap.put("stores.ad-info.key.serde", "string");
        confMap.put("stores.ad-info.msg.serde", "json");
        confMap.put("serializers.registry.json.class", "org.apache.samza.serializers.JsonSerdeFactory");
        confMap.put("serializers.registry.string.class", "org.apache.samza.serializers.StringSerdeFactory");
        confMap.put("serializers.registry.integer.class", "org.apache.samza.serializers.IntegerSerdeFactory");

        InMemorySystemDescriptor isd = new InMemorySystemDescriptor("kafka");

        InMemoryInputDescriptor imevents = isd.getInputDescriptor("ad-click", new NoOpSerde<>());

        InMemoryOutputDescriptor outputAdStream = isd.getOutputDescriptor("ad-price", new NoOpSerde<>());

        TestRunner
                .of(new AdPriceTaskApplication())
                .addInputStream(imevents, TestUtils.genStreamData("adClick.txt"))
                .addOutputStream(outputAdStream, 1)
                .addConfig(confMap)
                .addConfig("deploy.test", "true")
                .run(Duration.ofSeconds(7));

        ListIterator<Object> resultIter = TestRunner.consumeStream(outputAdStream, Duration.ofSeconds(7)).get(0).listIterator();

        /* if clicked, ad is 80% of adPrice and cab is 20% */
        Map<String, Object> test1 = (Map<String, Object>) resultIter.next();
        Assert.assertEquals("store4", test1.get("storeId"));
        Assert.assertEquals(800, test1.get("ad"));
        Assert.assertEquals(200, test1.get("cab"));

        /* if not clicked, ad and cab are 50% of adPrice each */
        Map<String, Object> test2 = (Map<String, Object>) resultIter.next();
        Assert.assertEquals("store5", test2.get("storeId"));
        Assert.assertEquals(450, test2.get("ad"));
        Assert.assertEquals(450, test2.get("cab"));
    }
}
