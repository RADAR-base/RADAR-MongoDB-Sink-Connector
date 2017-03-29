
/*
 *  Copyright 2016 Kings College London and The Hyve
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.radarcns.pipeline;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.bson.Document;
//import org.radarcns.avro.restapi.dataset.Dataset;
//import org.radarcns.avro.restapi.header.DescriptiveStatistic;
//import org.radarcns.avro.restapi.source.SourceType;
import org.radarcns.integration.aggregator.ExpectedArrayValue;
import org.radarcns.integration.aggregator.ExpectedDoubleValue;
import org.radarcns.integration.aggregator.ExpectedValue;
import org.radarcns.integration.aggregator.ExpectedValue.ExpectedType;
import org.radarcns.integration.commons.Parser;
import org.radarcns.integration.commons.Parser.Variable;
import org.radarcns.mock.BasicMockConfig;
import org.radarcns.mock.MockDataConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The MockAggregator simulates the behaviour of a Kafka Streams application based on time window.
 * It supported accumulators are <ul>
 *      <li>array of {@code Double}
 *      <li>singleton {@code Double}
 *  </ul>
 */
public class MockAggregator {

    private static final Logger LOGGER = LoggerFactory.getLogger(MockAggregator.class);

    /**
     * @param parser class that reads a CVS file line by line returning an {@code Map} .
     * @return {@code ExpectedArrayValue} the simulated results computed using the input parser.
     * @see {@link org.radarcns.integration.aggregator.ExpectedArrayValue}
     **/
    public static ExpectedArrayValue simulateArrayCollector(Parser parser)
        throws IOException, IllegalAccessException, InstantiationException {
        Map<Variable, Object> record = parser.next();

        String user = null;
        String source = null;
        if (record != null) {
            user = record.get(Variable.USER).toString();
            source = record.get(Variable.SOURCE).toString();
        }

        ExpectedArrayValue eav = new ExpectedArrayValue(user, source);

        while ( record != null ) {
            eav.add((Long) record.get(Variable.TIME_WINDOW),
                    (Long) record.get(Variable.TIMESTAMP),
                    (Double[]) record.get(Variable.VALUE));

            record = parser.next();
        }

        parser.close();

        return eav;
    }

    /**
     * @param parser class that reads a CVS file line by line returning an {@code HashMap}.
     * @return {@code ExpectedDoubleValue} the simulated results computed using the input parser.
     * @see {@link org.radarcns.integration.aggregator.ExpectedDoubleValue}
     **/
    public static ExpectedDoubleValue simulateSingletonCollector(Parser parser)
        throws IOException, IllegalAccessException, InstantiationException {
        Map<Variable, Object> record = parser.next();

        String user = null;
        String source = null;
        if (record != null) {
            user = record.get(Variable.USER).toString();
            source = record.get(Variable.SOURCE).toString();
        }

        ExpectedDoubleValue edv = new ExpectedDoubleValue(user, source);

        while (record != null) {
            edv.add((Long) record.get(Variable.TIME_WINDOW),
                    (Long) record.get(Variable.TIMESTAMP),
                    (Double) record.get(Variable.VALUE));

            record = parser.next();
        }

        parser.close();

        return edv;
    }

    /**
     * Given a list of configurations, it simulates all of them that has
     *      {@link ExpectedValue.ExpectedType#DOUBLE} as expected type.
     * @param configs list containing all configurations that have to be tested.
     * @return {@code Map} of key {@code MockDataConfig} and value {@code ExpectedValue}.
     * @see {@link org.radarcns.integration.aggregator.ExpectedDoubleValue}.
     **/
    public static Map<MockDataConfig, ExpectedValue> simulateSingleton(List<MockDataConfig> configs)
        throws ClassNotFoundException, NoSuchMethodException, IOException, IllegalAccessException,
        InvocationTargetException, InstantiationException {
        Map<MockDataConfig, ExpectedValue> exepctedValue = new HashMap<>();

        for (MockDataConfig config : configs) {
            Parser parser =  new Parser(config);

            if (parser.getExpecedType().equals(ExpectedType.DOUBLE)) {
                exepctedValue.put(config, MockAggregator.simulateSingletonCollector(parser));
            }
        }

        return exepctedValue;
    }

    /**
     * Given a list of configurations, it simulates all of them that has
     *      {@link ExpectedValue.ExpectedType#ARRAY} as expected type.
     * @param configs list containing all configurations that have to be tested.
     * @return {@code Map} of key {@code MockDataConfig} and value {@code ExpectedValue}.
     * @see {@link org.radarcns.integration.aggregator.ExpectedDoubleValue}.
     **/
    public static  Map<MockDataConfig, ExpectedValue> simulateArray(List<MockDataConfig> configs)
        throws ClassNotFoundException, NoSuchMethodException, IOException, IllegalAccessException,
        InvocationTargetException, InstantiationException {
        Map<MockDataConfig, ExpectedValue> exepctedValue = new HashMap<>();

        for (MockDataConfig config : configs) {
            Parser parser =  new Parser(config);
            if (parser.getExpecedType().equals(ExpectedType.ARRAY)) {
                exepctedValue.put(config, MockAggregator.simulateArrayCollector(parser));
            }
        }

        return exepctedValue;
    }

    /**
     * Simulates all possible test case scenarios configured in mock-configuration.
     * @return {@code Map} of key {@code MockDataConfig} and value {@code ExpectedValue}.
     * @see {@link org.radarcns.integration.aggregator.ExpectedDoubleValue}.
     **/
    public static Map<MockDataConfig, ExpectedValue> getSimulations(BasicMockConfig config)
        throws ClassNotFoundException, NoSuchMethodException, IOException, IllegalAccessException,
        InvocationTargetException, InstantiationException {
        Map<MockDataConfig, ExpectedValue> map = new HashMap<>();
        map.putAll(simulateSingleton(config.getData()));
        map.putAll(simulateArray(config.getData()));

        return map;
    }

//    /**
//     * Simulates all possible test case scenarios configured in mock-configuration. For each sensor,
//     *      it generates one dataset per statistical function. The measurement units are taken from
//     *      an Empatica device.
//     * @param expectedValue {@code Map} of key {@code MockDataConfig} and value
//     *      {@code ExpectedValue} containing all expected values
//     * @param stat statistical value that has be tested
//     * @return {@code Map} of key {@code MockDataConfig} and value {@code Dataset}.
//     * @see {@link org.radarcns.integration.aggregator.ExpectedValue}.
//     **/
//    public static Map<MockDataConfig, Dataset> getExpecetedDataset(
//            Map<MockDataConfig, ExpectedValue> expectedValue, DescriptiveStatistic stat)
//        throws ClassNotFoundException, NoSuchMethodException, IOException, IllegalAccessException,
//        InvocationTargetException, InstantiationException {
//        Map<MockDataConfig, Dataset> map = new HashMap<>();
//
//        for (MockDataConfig config : expectedValue.keySet()) {
//            map.put(config, expectedValue.get(config).getDataset(stat, SourceType.EMPATICA,
//                    config.getSensorType()));
//        }
//
//        return map;
//    }

    /**
     * Simulates all possible test case scenarios configured in mock-configuration. For each sensor,
     *      it generates the relative collection of Document that should be present in MongoDB.
     * @param expectedValue {@code Map} of key {@code MockDataConfig} and value
     *      {@code ExpectedValue} containing all expected values
     * @return {@code Map} of key {@code MockDataConfig} and value {@code Collection<Document>}.
     * @see {@link org.radarcns.integration.aggregator.ExpectedValue}.
     **/
    public static Map<MockDataConfig, Collection<Document>> getExpecetedDocument(
        Map<MockDataConfig, ExpectedValue> expectedValue) {
        Map<MockDataConfig, Collection<Document>> map = new HashMap<>();

        for (MockDataConfig config : expectedValue.keySet()) {
            map.put(config, expectedValue.get(config).getDocuments());
        }

        return map;
    }

}