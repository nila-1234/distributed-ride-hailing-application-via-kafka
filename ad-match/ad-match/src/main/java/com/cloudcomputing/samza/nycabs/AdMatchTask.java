package com.cloudcomputing.samza.nycabs;

import com.google.common.io.Resources;
import org.apache.samza.context.Context;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;

import org.codehaus.jackson.map.ObjectMapper;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Arrays;


/**
 * Consumes the stream of events.
 * Outputs a stream which handles static file and one stream
 * and gives a stream of advertisement matches.
 */
public class AdMatchTask implements StreamTask, InitableTask {

    /*
       Define per task state here. (kv stores etc)
       READ Samza API part in Writeup to understand how to start
    */

    private KeyValueStore<Integer, Map<String, Object>> userInfo;

    private KeyValueStore<String, Map<String, Object>> yelpInfo;

    private Set<String> lowCalories;

    private Set<String> energyProviders;

    private Set<String> willingTour;

    private Set<String> stressRelease;

    private Set<String> happyChoice;

    private void initSets() {
        lowCalories = new HashSet<>(Arrays.asList("seafood", "vegetarian", "vegan", "sushi"));
        energyProviders = new HashSet<>(Arrays.asList("bakeries", "ramen", "donuts", "burgers",
                "bagels", "pizza", "sandwiches", "icecream",
                "desserts", "bbq", "dimsum", "steak"));
        willingTour = new HashSet<>(Arrays.asList("parks", "museums", "newamerican", "landmarks"));
        stressRelease = new HashSet<>(Arrays.asList("coffee", "bars", "wine_bars", "cocktailbars", "lounges"));
        happyChoice = new HashSet<>(Arrays.asList("italian", "thai", "cuban", "japanese", "mideastern",
                "cajun", "tapas", "breakfast_brunch", "korean", "mediterranean",
                "vietnamese", "indpak", "southern", "latin", "greek", "mexican",
                "asianfusion", "spanish", "chinese"));
    }

    // Get store tag
    private String getTag(String cate) {
        String tag = "";
        if (happyChoice.contains(cate)) {
            tag = "happyChoice";
        } else if (stressRelease.contains(cate)) {
            tag = "stressRelease";
        } else if (willingTour.contains(cate)) {
            tag = "willingTour";
        } else if (energyProviders.contains(cate)) {
            tag = "energyProviders";
        } else if (lowCalories.contains(cate)) {
            tag = "lowCalories";
        } else {
            tag = "others";
        }
        return tag;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(Context context) throws Exception {
        // Initialize kv store

        userInfo = (KeyValueStore<Integer, Map<String, Object>>) context.getTaskContext().getStore("user-info");
        yelpInfo = (KeyValueStore<String, Map<String, Object>>) context.getTaskContext().getStore("yelp-info");

        //Initialize store tags set
        initSets();

        //Initialize static data and save them in kv store
        initialize("UserInfoData.json", "NYCstore.json");
    }

    /**
     * This function will read the static data from resources folder
     * and save data in KV store.
     * <p>
     * This is just an example, feel free to change them.
     */
    public void initialize(String userInfoFile, String businessFile) {
        List<String> userInfoRawString = AdMatchConfig.readFile(userInfoFile);
        System.out.println("Reading user info file from " + Resources.getResource(userInfoFile).toString());
        System.out.println("UserInfo raw string size: " + userInfoRawString.size());
        for (String rawString : userInfoRawString) {
            Map<String, Object> mapResult;
            ObjectMapper mapper = new ObjectMapper();
            try {
                mapResult = mapper.readValue(rawString, HashMap.class);
                int userId = (Integer) mapResult.get("userId");
                userInfo.put(userId, mapResult);
            } catch (Exception e) {
                System.out.println("Failed at parse user info :" + rawString);
            }
        }

        List<String> businessRawString = AdMatchConfig.readFile(businessFile);

        System.out.println("Reading store info file from " + Resources.getResource(businessFile).toString());
        System.out.println("Store raw string size: " + businessRawString.size());

        for (String rawString : businessRawString) {
            Map<String, Object> mapResult;
            ObjectMapper mapper = new ObjectMapper();
            try {
                mapResult = mapper.readValue(rawString, HashMap.class);
                String storeId = (String) mapResult.get("storeId");
                String cate = (String) mapResult.get("categories");
                String tag = getTag(cate);
                mapResult.put("tag", tag);
                yelpInfo.put(storeId, mapResult);
            } catch (Exception e) {
                System.out.println("Failed at parse store info :" + rawString);
            }
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
        /*
        All the messsages are partitioned by blockId, which means the messages
        sharing the same blockId will arrive at the same task, similar to the
        approach that MapReduce sends all the key value pairs with the same key
        into the same reducer.
        */
        String incomingStream = envelope.getSystemStreamPartition().getStream();

        /* get Message */
        Map<String, Object> message = (Map<String, Object>) envelope.getMessage();

        if (incomingStream.equals(AdMatchConfig.EVENT_STREAM.getStream())) {
            /* Handle Event messages */
            String type = (String) message.get("type");

            switch (type) {
                case "RIDER_INTEREST":
                    handleRiderInterest(message);
                    break;
                case "RIDER_STATUS":
                    handleRiderStatus(message);
                    break;
                case "RIDE_REQUEST":
                    handleRideRequest(message, collector);
                    break;
                default:
                    break;
            }

        } else {
            throw new IllegalStateException("Unexpected input stream: " + envelope.getSystemStreamPartition());
        }
    }

    /**
     * Update user's interest based on their browsing duration
     *
     * @param message Event Message
     */
    public void handleRiderInterest(Map<String, Object> message) {

        int duration = (int) message.get("duration");
        /* if browsing duration less than 5 minutes, return */
        if (duration < 5 * 60 * 1000) {
            return;
        }
        /* get user info from message*/
        int userId = (int) message.get("userId");
        String interest = (String) message.get("interest");
        /* query user in the store */
        Map<String, Object> user = userInfo.get(userId);
        /* if user not found, return */
        if (user == null) {
            return;
        }
        /* Update user interests in the store */
        user.put("interest", interest);
        userInfo.put(userId, user);
    }

    /**
     * Update user's status in userInfo
     *
     * @param message Event Message
     */
    public void handleRiderStatus(Map<String, Object> message) {
        int userId = (int) message.get("userId");

        Map<String, Object> user = userInfo.get(userId);

        user.put("mood", (int) message.get("mood"));
        user.put("blood_sugar", (int) message.get("blood_sugar"));
        user.put("stress", (int) message.get("stress"));
        user.put("active", (int) message.get("active"));
        userInfo.put(userId, user);
    }

    /**
     * Handle ride request event and emit to ad-stream
     *
     * @param message Event Message
     * @param collector Message collector to emit matched store ad
     */
    public void handleRideRequest(Map<String, Object> message, MessageCollector collector) {
        int clientId = (int) message.get("clientId");

        Map<String, Object> user = userInfo.get(clientId);
        if (user == null) {
            return;
        }

        double latitude = (double) message.get("latitude");
        double longitude = (double) message.get("longitude");

        String interest = (String) user.get("interest");
        String device = (String) user.get("device");
        int age = (int) user.get("age");
        int travelCount = (int) user.get("travel_count");

        Set<String> tags = calculateUserTags(user);

        double bestScore = -1.0;
        Map<String, Object> bestStore = null;

        KeyValueIterator<String, Map<String, Object>> iterator = yelpInfo.all();
        while (iterator.hasNext()) {
            Entry<String, Map<String, Object>> entry = iterator.next();

            Map<String, Object> store = entry.getValue();

            String storeTag = getTag((String) store.get("categories"));
            if (!tags.contains(storeTag)) {
                continue;
            }

            int reviewCount = (int) store.get("review_count");
            double rating = (double) store.get("rating");
            String price = (String) store.get("price");
            double storeLatitude = (double) store.get("latitude");
            double storeLongitude = (double) store.get("longitude");
            String categories = (String) store.get("categories");

            double distance = distance(latitude, longitude, storeLatitude, storeLongitude, "M");

            /* Score calculation */
            double score = reviewCount * rating;

            /* interest score */
            if (categories.equals(interest)) score += 10;

            /* pricing score */
            score = score * (1 - Math.abs(calculateValue(price) - calculateValue(device)) * 0.1);

            /* age and distance score */
            if (travelCount > 50 || age == 20) {
                if (distance > 10.0) score = score * 0.1;
            } else {
                if (distance > 5.0) score = score * 0.1;
            }

            /* update best score */
            if (score > bestScore) {
                bestScore = score;
                bestStore = store;
            }
        }
        iterator.close();

        /* Send best matched store to ad-stream if it exists */
        if (bestStore != null) {
            Map<String, Object> output = new HashMap<>();
            output.put("userId", clientId);
            output.put("storeId", bestStore.get("storeId"));
            output.put("name", bestStore.get("name"));

            if (clientId == 7171) {
                System.out.println("Client ID: " + clientId);
                System.out.println(bestStore.get("storeId"));
                System.out.println(bestStore);
            }
            collector.send(new OutgoingMessageEnvelope(AdMatchConfig.AD_STREAM, output));
        }

    }

    /**
     * Calculates the tags to assign to a user based on their status
     *
     * @param user map of user object containing mood, blood sugar, stress and activity metrics
     * @return set of tags associated with the user
     */
    public Set<String> calculateUserTags(Map<String, Object> user) {
        int mood = (int) user.get("mood");
        int bloodSugar = (int) user.get("blood_sugar");
        int stress = (int) user.get("stress");
        int active = (int) user.get("active");

        Set<String> tags = new HashSet<>();

        if (bloodSugar > 4 && mood > 6 && active == 3) tags.add("lowCalories");
        if (bloodSugar < 2 || mood < 4) tags.add("energyProviders");
        if (active == 3) tags.add("willingTour");
        if (stress > 5 || active == 1 || mood < 4) tags.add("stressRelease");
        if (mood > 6) tags.add("happyChoice");
        if (tags.isEmpty()) tags.add("others");

        return tags;
    }

    /**
     * Calculates the value for either a user's device or a store's price rating
     *
     * @param item user's device or store's price rating
     * @return numeric value of device or rating
     */
    public int calculateValue(String item) {
        switch (item) {
            case "iPhone XS":
            case "$$$$":
            case "$$$":
                return 3;
            case "iPhone 7":
            case "$$":
                return 2;
            case "iPhone 5":
            case "$":
                return 1;
            default:
                return 0;
        }
    }

    /**
     *
     * @param lat1 first location's latitude
     * @param lon1 first location's longitude
     * @param lat2 second location's latitude
     * @param lon2 second location's longitude
     * @param unit distance unit - M (miles), K (kilometer), N (nautical miles)
     * @return distance between two locations
     */
    private static double distance(double lat1, double lon1, double lat2, double lon2, String unit) {
        if ((lat1 == lat2) && (lon1 == lon2)) {
            return 0;
        } else {
            double theta = lon1 - lon2;
            double dist = Math.sin(
                    Math.toRadians(lat1)) * Math.sin(Math.toRadians(lat2)) +
                    Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) * Math.cos(Math.toRadians(theta));
            dist = Math.acos(dist);
            dist = Math.toDegrees(dist);
            dist = dist * 60 * 1.1515;
            if (unit.equals("K")) {
                dist = dist * 1.609344;
            } else if (unit.equals("N")) {
                dist = dist * 0.8684;
            }
            return (dist);
        }
    }
}
