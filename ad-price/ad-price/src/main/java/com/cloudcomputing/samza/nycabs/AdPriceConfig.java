package com.cloudcomputing.samza.nycabs;

import com.google.common.io.Resources;
import org.apache.samza.system.SystemStream;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class AdPriceConfig {
    public static final SystemStream AD_CLICK_STREAM = new SystemStream("kafka", "ad-click");
    public static final SystemStream AD_PRICE_STREAM = new SystemStream("kafka", "ad-price");
    public static List<String> readFile(String path) {
        try {
            InputStream in = Resources.getResource(path).openStream();
            List<String> lines = new ArrayList<>();
            String line = null;
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            while ((line = reader.readLine()) != null) {
                lines.add(line);
            }
            reader.close();
            return lines;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
