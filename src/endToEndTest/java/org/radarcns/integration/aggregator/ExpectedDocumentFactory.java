/*
 * Copyright 2017 Kings College London and The Hyve
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

package org.radarcns.integration.aggregator;


import static org.radarcns.integration.model.ExpectedValue.DURATION;
import static org.radarcns.integration.model.ExpectedValue.STAT_TYPE.AVERAGE;
import static org.radarcns.integration.model.ExpectedValue.STAT_TYPE.COUNT;
import static org.radarcns.integration.model.ExpectedValue.STAT_TYPE.INTERQUARTILE_RANGE;
import static org.radarcns.integration.model.ExpectedValue.STAT_TYPE.MAXIMUM;
import static org.radarcns.integration.model.ExpectedValue.STAT_TYPE.MEDIAN;
import static org.radarcns.integration.model.ExpectedValue.STAT_TYPE.MINIMUM;
import static org.radarcns.integration.model.ExpectedValue.STAT_TYPE.QUARTILES;
import static org.radarcns.integration.model.ExpectedValue.STAT_TYPE.SUM;

import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.bson.Document;
import org.radarcns.integration.model.ExpectedValue;
import org.radarcns.integration.model.ExpectedValue.STAT_TYPE;
import org.radarcns.integration.model.ExpectedValueFactory;
import org.radarcns.mock.MockDataConfig;
import org.radarcns.stream.aggregator.DoubleArrayCollector;
import org.radarcns.stream.aggregator.DoubleValueCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * It computes the expected value for a test case.
 */
public class ExpectedDocumentFactory implements ExpectedValueFactory {

//    private static final Logger LOGGER = LoggerFactory.getLogger(ExpectedValue.class);
//
//    public enum STAT_TYPE {
//        MINIMUM,
//        MAXIMUM,
//        SUM,
//        QUARTILES,
//        MEDIAN,
//        INTERQUARTILE_RANGE,
//        COUNT,
//        AVERAGE
//
//    }
//
//    /**
//     * Enumerator containing all possible collector implementations. Useful to understand if
//     * the current isntance is managing single doubles or arrays of doubles.
//     *
//     * @see {@link DoubleArrayCollector}
//     * @see {@link DoubleValueCollector}
//     **/
//    public enum ExpectedType {
//        ARRAY("org.radarcns.integration.aggregator.DoubleArrayCollector"),
//        DOUBLE("org.radarcns.integration.aggregator.DoubleValueCollector");
//
//        private String value;
//
//        ExpectedType(String value) {
//            this.value = value;
//        }
//
//        public String getValue() {
//            return value;
//        }
//
//        @Override
//        public String toString() {
//            return this.getValue();
//        }
//
//        /**
//         * Return the {@code ExpectedType} associated to the input String.
//         *
//         * @param value representing an {@code ExpectedType} item
//         * @return the {@code ExpectedType} that matches the input
//         **/
//        public static ExpectedType getEnum(String value) {
//            for (ExpectedType v : values()) {
//                if (v.getValue().equalsIgnoreCase(value)) {
//                    return v;
//                }
//            }
//            throw new IllegalArgumentException();
//        }
//    }
//
//    //Timewindow length in milliseconds
//    @SuppressWarnings({"checkstyle:AbbreviationAsWordInName", "checkstyle:MemberName"})
//    protected long DURATION = TimeUnit.SECONDS.toMillis(10);
//
//    protected String user;
//    protected String source;
//
//    protected Long lastTimestamp;
//    protected V lastValue;
//    protected HashMap<Long, V> series;
//
//
//    /**
//     * Constructor.
//     **/
//    public ExpectedValue(String user, String source)
//            throws IllegalAccessException, InstantiationException {
//        series = new HashMap<>();
//
//        this.user = user;
//        this.source = source;
//        lastTimestamp = 0L;
//
//        Class<V> valueClass = (Class<V>) ((ParameterizedType) getClass()
//                .getGenericSuperclass()).getActualTypeArguments()[0];
//
//        lastValue = valueClass.newInstance();
//    }
//
//    public ExpectedType getExpectedType() {
//        for (ExpectedType expectedType : ExpectedType.values()) {
//            if (expectedType.getValue().equals(lastValue.getClass().getCanonicalName())) {
//                return expectedType;
//            }
//        }
//
//        return null;
//    }

    /**
     * It return the value of the given statistical function.
     *
     * @param statistic function that has to be returned
     * @param collectors array of aggregated data
     * @return the set of values that has to be stored within a {@code Dataset} {@code Item}
     * @see {@link DoubleValueCollector}
     **/
    private List<? extends Object> getStatValue(STAT_TYPE statistic,
            DoubleValueCollector[] collectors) {
        int len = collectors.length;

        List<Double> avgList = new ArrayList<>(len);
        List<Double> countList = new ArrayList<>(len);
        List<Double> iqrList = new ArrayList<>(len);
        List<Double> maxList = new ArrayList<>(len);
        List<Double> medList = new ArrayList<>(len);
        List<Double> minList = new ArrayList<>(len);
        List<Double> sumList = new ArrayList<>(len);
        List<List<Double>> quartileList = new ArrayList<>(len);

        for (DoubleValueCollector collector : collectors) {
            minList.add((Double) getStatValue(MINIMUM, collector));
            maxList.add((Double) getStatValue(MAXIMUM, collector));
            sumList.add((Double) getStatValue(SUM, collector));
            countList.add((Double) getStatValue(COUNT, collector));
            avgList.add((Double) getStatValue(AVERAGE, collector));
            iqrList.add((Double) getStatValue(INTERQUARTILE_RANGE, collector));
            quartileList.add((List<Double>) getStatValue(QUARTILES, collector));
            medList.add((Double) getStatValue(MEDIAN, collector));
        }

        switch (statistic) {
            case AVERAGE:
                return avgList;
            case COUNT:
                return countList;
            case INTERQUARTILE_RANGE:
                return iqrList;
            case MAXIMUM:
                return maxList;
            case MEDIAN:
                return medList;
            case MINIMUM:
                return minList;
            case QUARTILES:
                return quartileList;
            case SUM:
                return sumList;
            default:
                throw new IllegalArgumentException(
                        statistic.toString() + " is not supported");
        }
    }

    /**
     * It return the value of the given statistical function.
     *
     * @param statistic function that has to be returned
     * @param collector data aggregator
     * @return the value that has to be stored within a {@code Dataset} {@code Item}
     * @see {@link .DoubleValueCollector}
     **/
    private Object getStatValue(STAT_TYPE statistic,
            DoubleValueCollector collector) {

        switch (statistic) {
            case AVERAGE:
                return collector.getAvg();
            case COUNT:
                return collector.getCount();
            case INTERQUARTILE_RANGE:
                return collector.getIqr();
            case MAXIMUM:
                return collector.getMax();
            case MEDIAN:
                return collector.getQuartile().get(1);
            case MINIMUM:
                return collector.getMin();
            case QUARTILES:
                return collector.getQuartile();
            case SUM:
                return collector.getSum();
            default:
                throw new IllegalArgumentException(
                        statistic.toString() + " is not supported");
        }
    }



    private List<Document> getDocumentsBySingle(ExpectedValue expectedValue) {
        LinkedList<Document> list = new LinkedList<>();

        List<Long> windows = new ArrayList<>(expectedValue.getSeries().keySet());
        Collections.sort(windows);

        DoubleValueCollector doubleValueCollector;
        Long end;
        for (Long timestamp : windows) {
            doubleValueCollector = (DoubleValueCollector) expectedValue.getSeries().get(timestamp);

            end = timestamp + DURATION;

            list.add(new Document("_id", expectedValue.getUser() + "-" + expectedValue.getSource() + "-" + timestamp + "-" + end)
                    .append("user", expectedValue.getUser())
                    .append("source", expectedValue.getSource())
                    .append("min", getStatValue(MINIMUM, doubleValueCollector))
                    .append("max", getStatValue(MAXIMUM, doubleValueCollector))
                    .append("sum", getStatValue(SUM, doubleValueCollector))
                    .append("count", getStatValue(COUNT, doubleValueCollector))
                    .append("avg", getStatValue(AVERAGE, doubleValueCollector))
                    .append("quartile", extractQuartile((List<Double>) getStatValue(
                            QUARTILES, doubleValueCollector)))
                    .append("iqr", getStatValue(INTERQUARTILE_RANGE, doubleValueCollector))
                    .append("start", new Date(timestamp))
                    .append("end", new Date(end)));
        }

        return list;
    }

    private List<Document> getDocumentsByArray(ExpectedValue expectedValue) {
        LinkedList<Document> list = new LinkedList<>();

        List<Long> windows = new ArrayList<>(expectedValue.getSeries().keySet());
        Collections.sort(windows);

        DoubleArrayCollector doubleArrayCollector;
        Long end;
        for (Long timestamp : windows) {
            doubleArrayCollector = (DoubleArrayCollector) expectedValue.getSeries().get(timestamp);

            end = timestamp + DURATION;

            list.add(new Document("_id", expectedValue.getUser() + "-" + expectedValue.getSource() + "-" + timestamp + "-" + end)
                    .append("user", expectedValue.getUser())
                    .append("source", expectedValue.getSource())
                    .append("min", getStatValue(MINIMUM, doubleArrayCollector.getCollectors()))
                    .append("max", getStatValue(MAXIMUM, doubleArrayCollector.getCollectors()))
                    .append("sum", getStatValue(SUM, doubleArrayCollector.getCollectors()))
                    .append("count", getStatValue(COUNT, doubleArrayCollector.getCollectors()))
                    .append("avg", getStatValue(AVERAGE, doubleArrayCollector.getCollectors()))
                    .append("quartile",
                            extractAccelerationQuartile((List<List<Double>>) getStatValue(
                                    QUARTILES, doubleArrayCollector.getCollectors())))
                    .append("iqr", getStatValue(INTERQUARTILE_RANGE,
                            doubleArrayCollector.getCollectors()))
                    .append("start", new Date(timestamp))
                    .append("end", new Date(end)));
        }

        return list;
    }

    private Document extractAccelerationQuartile(List<List<Double>> statValue) {
        Document quartile = new Document();
        quartile.put("x", extractQuartile(statValue.get(0)));
        quartile.put("y", extractQuartile(statValue.get(1)));
        quartile.put("z", extractQuartile(statValue.get(2)));
        return quartile;
    }

    public static List<Document> extractQuartile(List<Double> component) {
        return Arrays.asList(new Document[]{
                new Document("25", component.get(0)),
                new Document("50", component.get(1)),
                new Document("75", component.get(2))
        });
    }

    @Override
    public List<Document> produceExpectedData(ExpectedValue expectedValue) {
            switch (expectedValue.getExpectedType()) {
                case ARRAY:
                    return getDocumentsByArray(expectedValue);
                default:
                    return getDocumentsBySingle(expectedValue);
            }
    }
}
