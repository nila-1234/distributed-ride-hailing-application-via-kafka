package com.cloudcomputing.samza.nycabs;

import org.apache.samza.context.Context;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.storage.kv.KeyValueIterator;


import java.util.HashMap;
import java.util.Map;

/**
 * Consumes the stream of driver location updates and rider cab requests.
 * Outputs a stream which joins these 2 streams and gives a stream of rider to
 * driver matches.
 */
public class DriverMatchTask implements StreamTask, InitableTask {

    /* Define per task state here. (kv stores etc)
       READ Samza API part in Primer to understand how to start
    */
    private double MAX_MONEY = 100.0;
    private double MAX_RATING = 5.0;

    private KeyValueStore<String, Map<String, Object>> driverStore;

    @Override
    @SuppressWarnings("unchecked")
    public void init(Context context) throws Exception {
        // Initialize (maybe the kv stores?)
        driverStore = (KeyValueStore<String, Map<String, Object>>) context.getTaskContext().getStore("driver-loc");
    }

    @Override
    @SuppressWarnings("unchecked")
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
        /*
        All the messages are partitioned by blockId, which means the messages
        sharing the same blockId will arrive at the same task, similar to the
        approach that MapReduce sends all the key value pairs with the same key
        into the same reducer.
        */
        String incomingStream = envelope.getSystemStreamPartition().getStream();

        /* get Message */
        Map<String, Object> message = (Map<String, Object>) envelope.getMessage();

        if (incomingStream.equals(DriverMatchConfig.DRIVER_LOC_STREAM.getStream())) {
            /* Handle Driver Location messages */
            handleDriverLocation(message);

        } else if (incomingStream.equals(DriverMatchConfig.EVENT_STREAM.getStream())) {
            /* Handle Event messages */
            handleEvent(message, collector);

        } else {
            throw new IllegalStateException("Unexpected input stream: " + envelope.getSystemStreamPartition());
        }
    }

    /**
     * Handles driver location stream (latitude and longitude) for existing driver-block entries.
     *
     * @param message Driver location message
     */
    public void handleDriverLocation(Map<String, Object> message) {
        int driverId = (int) message.get("driverId");
        int blockId = (int) message.get("blockId");
        /* concat blockId and driverId as key */
        String key = blockId + "-" + driverId;

        Map<String, Object> driver = driverStore.get(key);

        /* if driver-block pair already exists, update location */
        if (driver != null) {
            driver.put("latitude", message.get("latitude"));
            driver.put("longitude", message.get("longitude"));
            driverStore.put(key, driver);
        }
    }

    /**
     * Handles various types of event messages:
     * - ENTERING_BLOCK: Adds driver to store
     * - LEAVING_BLOCK: Removes driver from store
     * - RIDE_REQUEST: Finds and emits the best driver match
     * - RIDE_COMPLETE: Updates driver rating and status
     *
     * @param message   Event message
     * @param collector Message collector to emit matched driver
     */
    public void handleEvent(Map<String, Object> message, MessageCollector collector) {
        String type = message.get("type").toString();

        if (type.equals("ENTERING_BLOCK")) {
            /* if driver's entering the block, store a hashmap of the message in the driver store */
            int driverId = (int) message.get("driverId");
            int blockId = (int) message.get("blockId");
            String key = blockId + "-" + driverId;

            /* create map with drivers entering th block and add to driver store */
            Map<String, Object> driver = new HashMap<>();
            driver.put("driverId", driverId);
            driver.put("blockId", blockId);
            driver.put("latitude", (double) message.get("latitude"));
            driver.put("longitude", (double) message.get("longitude"));
            driver.put("rating", (double) message.get("rating"));
            driver.put("salary", (int) message.get("salary"));
            driver.put("gender", message.get("gender"));
            driver.put("status", message.get("status"));

            driverStore.put(key, driver);
        } else if (type.equals("LEAVING_BLOCK")) {
            /* if driver's leaving the block, delete the map entry */
            int driverId = (int) message.get("driverId");
            int blockId = (int) message.get("blockId");
            String key = blockId + "-" + driverId;
            driverStore.delete(key);
        } else if (type.equals("RIDE_REQUEST")) {
            /* if client requests a ride, check and compare drivers' metrics with client preferences */
            int clientId = (int) message.get("clientId");
            int blockId = (int) message.get("blockId");
            String genderPreference = (String) message.get("gender_preference");
            double clientLatitude = (double) message.get("latitude");
            double clientLongitude = (double) message.get("longitude");

            /* track best matched score and best matched driver key */
            double bestScore = -1.0;
            String bestDriverKey = null;

            /* Iterate through the driver store to see all applicable drivers in the block */
            KeyValueIterator<String, Map<String, Object>> iterator = driverStore.all();
            while (iterator.hasNext()) {
                Entry<String, Map<String, Object>> entry = iterator.next();
                String key = entry.getKey();

                /* check if the driver is in the same block */
                if (!key.startsWith(blockId + "-")) {
                    continue;
                }

                /* chcek if driver is available */
                Map<String, Object> driver = entry.getValue();
                if (!"AVAILABLE".equals(driver.get("status"))) {
                    continue;
                }

                /* get the driver's details */
                double driverLatitude = (double) driver.get("latitude");
                double driverLongitude = (double) driver.get("longitude");
                double driverRating = (double) driver.get("rating");
                int driverSalary = (int) driver.get("salary");
                String driverGender = (String) driver.get("gender");

                /* Score calculation */
                double dist = Math.sqrt(
                        Math.pow(driverLatitude - clientLatitude, 2)
                        + Math.pow(driverLongitude - clientLongitude, 2));
                double distanceScore = 1 * Math.pow(Math.E, -1 * dist);
                double ratingScore = driverRating / MAX_RATING;
                double salaryScore = 1 - driverSalary / MAX_MONEY;
                double genderScore = genderPreference.equals("N") || genderPreference.equals(driverGender) ? 1 : 0;

                double matchScore = distanceScore * 0.4
                        + genderScore * 0.1
                        + ratingScore * 0.3
                        + salaryScore * 0.2;

                /* if the current score is better, update score and key */
                if (matchScore > bestScore) {
                    bestScore = matchScore;
                    bestDriverKey = key;
                }
            }
            iterator.close();

            /* if a suitable driver exists */
            if (bestDriverKey != null) {
                Map<String, Object> driver = driverStore.get(bestDriverKey);
                /* make the driver unavailable in the store*/
                driver.put("status", "UNAVAILABLE");
                driverStore.put(bestDriverKey, driver);

                /* create map of client and driver to send to output stream */
                Map<String, Object> output = new HashMap<>();
                output.put("driverId", driver.get("driverId"));
                output.put("clientId", clientId);
                collector.send(new OutgoingMessageEnvelope(DriverMatchConfig.MATCH_STREAM, output));
            }
        } else if (type.equals("RIDE_COMPLETE")) {
            /* if the ride is complete, update driver details */
            int driverId = (int) message.get("driverId");
            int blockId = (int) message.get("blockId");
            String key = blockId + "-" + driverId;

            Map<String, Object> driver = driverStore.get(key);
            if (driver != null) {
                /* update rating based on user rating, location and store it in the driver store */
                double oldRating = (double) driver.getOrDefault("rating",5.0);
                double newRating = (double) message.getOrDefault("user_rating", 5.0);
                double rating = (oldRating + newRating) / 2.0;

                driver.put("rating", rating);
                driver.put("latitude", (double) message.get("latitude"));
                driver.put("longitude", (double) message.get("longitude"));
                driver.put("status", "AVAILABLE");

                driverStore.put(key, driver);
            }
        }
    }
}
