package com.cloudcomputing.samza.nycabs;

import com.google.common.io.Resources;
import org.apache.samza.context.Context;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;

import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.HashMap;


/**
 * Consumes the stream of ad-click.
 * Outputs a stream which handles static file and one stream
 * and gives a stream of revenue distribution.
 */
public class AdPriceTask implements StreamTask, InitableTask {

    /*
       Define per task state here. (kv stores etc)
       READ Samza API part in Writeup to understand how to start
    */
    private KeyValueStore<String, Map<String, Object>> adInfo;


    @Override
    @SuppressWarnings("unchecked")
    public void init(Context context) throws Exception {
        // Initialize (maybe kv store and static data?)
        adInfo = (KeyValueStore<String, Map<String, Object>>) context.getTaskContext().getStore("ad-info");
        initialize("NYCstoreAds.json");
    }

    /**
     * Initialize adInfo store by reading from NYCstoreAds.json
     *
     * @param adInfoFile adInfo file name
     */
    public void initialize(String adInfoFile) {
        List<String> adInfoRawString = AdPriceConfig.readFile(adInfoFile);
        System.out.println("Reading ad info file from " + Resources.getResource(adInfoFile).toString());
        System.out.println("AdInfo raw string size: " + adInfoRawString.size());
        for (String rawString : adInfoRawString) {
            Map<String, Object> mapResult;
            ObjectMapper mapper = new ObjectMapper();
            try {
                mapResult = mapper.readValue(rawString, HashMap.class);
                String storeId = (String) mapResult.get("storeId");
                adInfo.put(storeId, mapResult);
            } catch (Exception e) {
                System.out.println("Failed at parse store info :" + rawString);
            }
        }
    }


    @Override
    @SuppressWarnings("unchecked")
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator)
            throws IOException {
        /*
        All the messsages are partitioned by userId, which means the messages
        sharing the same userId will arrive at the same task, similar to the
        approach that MapReduce sends all the key value pairs with the same key
        into the same reducer.
        */
        String incomingStream = envelope.getSystemStreamPartition().getStream();

        /* Get message */
        Map<String, Object> message = (Map<String, Object>) envelope.getMessage();

        if (incomingStream.equals(AdPriceConfig.AD_CLICK_STREAM.getStream())) {
            /* Handle Ad-click messages */

            /* get message details */
            int userId = (int) message.get("userId");
            String storeId = (String) message.get("storeId");
            String clicked = (String) message.get("clicked");

            /* find store from adInfo store */
            Map<String, Object> storeAds = adInfo.get(storeId);
            if (storeAds != null) {

                /* Get total ad price */
                int adPrice = (int) storeAds.get("adPrice");

                /* Calculate ad and cab split */
                int ad = clicked.equals("true") ? (int) (adPrice * 0.8) : (int) (adPrice * 0.5);
                int cab = adPrice - ad;

                /* Create map record of ad prices */
                Map<String, Object> output = new HashMap<>();
                output.put("userId", userId);
                output.put("storeId", storeId);
                output.put("ad", ad);
                output.put("cab", cab);

                /* Send ad prices to stream */
                collector.send(new OutgoingMessageEnvelope(AdPriceConfig.AD_PRICE_STREAM, output));
            }
            
        } else {
            throw new IllegalStateException("Unexpected input stream: " + envelope.getSystemStreamPartition());
        }
    }
}
