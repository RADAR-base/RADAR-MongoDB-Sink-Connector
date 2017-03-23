package org.radarcns.pipeline;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import okhttp3.Response;
import org.junit.Test;
import org.radarcns.config.YamlConfigLoader;
import org.radarcns.mock.BasicMockConfig;
import org.radarcns.mock.MockDataConfig;
import org.radarcns.mock.data.CsvGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MongoDBSinkEndToEndTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(MongoDBSinkEndToEndTest.class);

    private static final String BASIC_MOCK_CONFIG_FILE = "basic_mock_config.yml";

    private static BasicMockConfig config = null;

    @Test
    public void testMongoDBSinkPipeline() throws InterruptedException, IOException {

        loadMockSetupConfig();

        waitForInfrastructureBootUp();

        generateCsvInputFiles();


    }

    private void generateCsvInputFiles() throws IOException {
        LOGGER.info("Generating CVS files ...");
        File parentFile = new File(
                MongoDBSinkEndToEndTest.class.getClassLoader()
                        .getResource(BASIC_MOCK_CONFIG_FILE).getFile()
        );
        for (MockDataConfig config : getBasicMockConfig().getData()) {
            CsvGenerator.generate(config, (long)30, parentFile);
        }
    }

    /**
     * Checks if the test bed is ready to accept data.
     */
    private void waitForInfrastructureBootUp() throws InterruptedException {
        LOGGER.info("Waiting infrastructure ... ");
        int retry = 60;
        long sleep = 1000;
        int count = 0;

        List<String> expectedTopics = new LinkedList<>();
        expectedTopics.add("android_empatica_e4_acceleration");
        expectedTopics.add("android_empatica_e4_acceleration_output");
        expectedTopics.add("android_empatica_e4_battery_level");
        expectedTopics.add("android_empatica_e4_battery_level_output");
        expectedTopics.add("android_empatica_e4_blood_volume_pulse");
        expectedTopics.add("android_empatica_e4_blood_volume_pulse_output");
        expectedTopics.add("android_empatica_e4_electrodermal_activity");
        expectedTopics.add("android_empatica_e4_electrodermal_activity_output");
        expectedTopics.add("android_empatica_e4_heartrate");
        expectedTopics.add("android_empatica_e4_inter_beat_interval");
        expectedTopics.add("android_empatica_e4_inter_beat_interval_output");
        expectedTopics.add("android_empatica_e4_sensor_status");
        expectedTopics.add("android_empatica_e4_sensor_status_output");
        expectedTopics.add("android_empatica_e4_temperature");
        expectedTopics.add("android_empatica_e4_temperature_output");
        expectedTopics.add("application_server_status");
        expectedTopics.add("application_record_counts");
        expectedTopics.add("application_uptime");

        for (int i = 0; i < retry; i++) {
            count = 0;

            Response response = null;
            try {
                response = EndToEndUtility.makeRequest(
                        config.getRestProxy().getUrlString()+ "/topics");
                if (response.code() == 200) {
                    String topics = response.body().string().toString();
                    String[] topicArray = topics.substring(1, topics.length() - 1).replace("\"", "")
                            .split(",");

                    for (String topic : topicArray) {
                        if (expectedTopics.contains(topic)) {
                            count++;
                        }
                    }

                    if (count == expectedTopics.size()) {
                        break;
                    }
                }
            } catch (IOException exec) {
                LOGGER.info("Error while waiting infrastructure", exec);
            }

            Thread.sleep(sleep * (i + 1));
        }

        assertEquals(expectedTopics.size(), count);
    }
    private void loadMockSetupConfig() {

        try {
            getBasicMockConfig();
            assertNotNull(config);


        } catch (IOException e) {
            e.printStackTrace();
            assertTrue(false);
        }

    }

    private static BasicMockConfig getBasicMockConfig() throws IOException {
        if(config==null) {

            config = new YamlConfigLoader().load(
                    new File(
                            MongoDBSinkEndToEndTest.class.getClassLoader()
                                    .getResource(BASIC_MOCK_CONFIG_FILE).getFile()
                    ), BasicMockConfig.class);
        }
        return config;
    }


}
