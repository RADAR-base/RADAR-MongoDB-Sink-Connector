package org.radarcns.integration;

/*
 * Copyright 2017 King's College London and The Hyve
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

import static org.radarcns.mock.model.ExpectedValue.DURATION;
import static org.radarcns.sink.mongodb.converter.AccelerationCollectorConverter.X_LABEL;
import static org.radarcns.sink.mongodb.converter.AccelerationCollectorConverter.Y_LABEL;
import static org.radarcns.sink.mongodb.converter.AccelerationCollectorConverter.Z_LABEL;
import static org.radarcns.sink.mongodb.util.MongoConstants.END;
import static org.radarcns.sink.mongodb.util.MongoConstants.FIRST_QUARTILE;
import static org.radarcns.sink.mongodb.util.MongoConstants.ID;
import static org.radarcns.sink.mongodb.util.MongoConstants.SECOND_QUARTILE;
import static org.radarcns.sink.mongodb.util.MongoConstants.SOURCE;
import static org.radarcns.sink.mongodb.util.MongoConstants.START;
import static org.radarcns.sink.mongodb.util.MongoConstants.Stat.AVERAGE;
import static org.radarcns.sink.mongodb.util.MongoConstants.Stat.COUNT;
import static org.radarcns.sink.mongodb.util.MongoConstants.Stat.INTERQUARTILE_RANGE;
import static org.radarcns.sink.mongodb.util.MongoConstants.Stat.MAXIMUM;
import static org.radarcns.sink.mongodb.util.MongoConstants.Stat.MINIMUM;
import static org.radarcns.sink.mongodb.util.MongoConstants.Stat.QUARTILES;
import static org.radarcns.sink.mongodb.util.MongoConstants.Stat.SUM;
import static org.radarcns.sink.mongodb.util.MongoConstants.THIRD_QUARTILE;
import static org.radarcns.sink.mongodb.util.MongoConstants.USER;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.bson.Document;
import org.radarcns.mock.model.ExpectedValue;
import org.radarcns.sink.mongodb.util.MongoConstants.Stat;
import org.radarcns.stream.collector.DoubleArrayCollector;
import org.radarcns.stream.collector.DoubleValueCollector;

/**
 * It computes the expected Documents for a test case i.e. {@link ExpectedValue}.
 */
public class ExpectedDocumentFactory {

    //private static final Logger LOGGER = LoggerFactory.getLogger(ExpectedDocumentFactory.class);

    /**
     * It return the value of the given statistical function.
     *
     * @param statistic function that has to be returned
     * @param collectors array of aggregated data
     * @return the set of values that has to be stored within a {@code Dataset} {@code Item}
     * @see DoubleValueCollector
     **/
    public List<?> getStatValue(Stat statistic, DoubleArrayCollector collectors) {

        List<DoubleValueCollector> subCollectors = collectors.getCollectors();
        List<Object> subList = new ArrayList<>(subCollectors.size());
        for (DoubleValueCollector collector : subCollectors) {
            subList.add(getStatValue(statistic, collector));
        }
        return subList;
    }

    /**
     * It return the value of the given statistical function.
     *
     * @param statistic function that has to be returned
     * @param collector data aggregator
     * @return the value that has to be stored within a {@code Dataset} {@code Item}
     * @see DoubleValueCollector
     **/
    public Object getStatValue(Stat statistic, DoubleValueCollector collector) {
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
                    statistic.toString() + " is not supported by DoubleValueCollector");
        }
    }


    private List<Document> getDocumentsBySingle(ExpectedValue<?> expectedValue) {

        List<Long> windows = new ArrayList<>(expectedValue.getSeries().keySet());
        Collections.sort(windows);

        List<Document> list = new ArrayList<>(windows.size());

        for (Long timestamp : windows) {
            DoubleValueCollector doubleValueCollector =
                    (DoubleValueCollector) expectedValue.getSeries().get(timestamp);

            long end = timestamp + DURATION;

            list.add(new Document(ID,
                        expectedValue.getLastKey().getUserId()
                        + "-" + expectedValue.getLastKey().getSourceId()
                        + "-" + timestamp + "-" + end).append(
                        USER, expectedValue.getLastKey().getUserId()).append(
                        SOURCE, expectedValue.getLastKey().getSourceId()).append(
                        MINIMUM.getParam(), getStatValue(MINIMUM, doubleValueCollector)).append(
                        MAXIMUM.getParam(), getStatValue(MAXIMUM, doubleValueCollector)).append(
                        SUM.getParam(), getStatValue(SUM, doubleValueCollector)).append(
                        COUNT.getParam(), getStatValue(COUNT, doubleValueCollector)).append(
                        AVERAGE.getParam(), getStatValue(AVERAGE, doubleValueCollector)).append(
                        QUARTILES.getParam(), extractQuartile((List<Double>) getStatValue(
                            QUARTILES, doubleValueCollector))).append(
                        INTERQUARTILE_RANGE.getParam(), getStatValue(INTERQUARTILE_RANGE,
                            doubleValueCollector)).append(
                        START, new Date(timestamp)).append(
                        END, new Date(end)));
        }

        return list;
    }

    private List<Document> getDocumentsByArray(ExpectedValue<?> expectedValue) {

        List<Long> windows = new ArrayList<>(expectedValue.getSeries().keySet());
        Collections.sort(windows);

        List<Document> list = new ArrayList<>(windows.size());

        for (Long timestamp : windows) {
            DoubleArrayCollector doubleArrayCollector = (DoubleArrayCollector) expectedValue
                    .getSeries().get(timestamp);

            long end = timestamp + DURATION;

            list.add(new Document(ID,
                        expectedValue.getLastKey().getUserId()
                            + "-" + expectedValue.getLastKey().getSourceId()
                            + "-" + timestamp + "-" + end).append(
                        USER, expectedValue.getLastKey().getUserId()).append(
                        SOURCE, expectedValue.getLastKey().getSourceId()).append(
                        MINIMUM.getParam(), extractAccelerationValue(
                            (List<Double>) getStatValue(MINIMUM, doubleArrayCollector))).append(
                        MAXIMUM.getParam(), extractAccelerationValue(
                            (List<Double>) getStatValue(MAXIMUM, doubleArrayCollector))).append(
                        SUM.getParam(), extractAccelerationValue(
                            (List<Double>) getStatValue(SUM, doubleArrayCollector))).append(
                        COUNT.getParam(), extractAccelerationValue(
                            (List<Double>) getStatValue(COUNT, doubleArrayCollector))).append(
                        AVERAGE.getParam(), extractAccelerationValue(
                            (List<Double>) getStatValue(AVERAGE, doubleArrayCollector))).append(
                        QUARTILES.getParam(),  extractAccelerationQuartile(
                            (List<List<Double>>) getStatValue(
                                QUARTILES, doubleArrayCollector))).append(
                        INTERQUARTILE_RANGE.getParam(), extractAccelerationValue(
                            (List<Double>) getStatValue(
                                INTERQUARTILE_RANGE, doubleArrayCollector))).append(
                        START, new Date(timestamp)).append(
                        END, new Date(end)));
        }

        return list;
    }

    private Document extractAccelerationValue(List<Double> statValue) {
        Document quartile = new Document();
        quartile.put(X_LABEL, statValue.get(0));
        quartile.put(Y_LABEL, statValue.get(1));
        quartile.put(Z_LABEL, statValue.get(2));
        return quartile;
    }

    private Document extractAccelerationQuartile(List<List<Double>> statValue) {
        Document quartile = new Document();
        quartile.put(X_LABEL, extractQuartile(statValue.get(0)));
        quartile.put(Y_LABEL, extractQuartile(statValue.get(1)));
        quartile.put(Z_LABEL, extractQuartile(statValue.get(2)));
        return quartile;
    }

    private static List<Document> extractQuartile(List<Double> component) {
        return Arrays.asList(
            new Document(FIRST_QUARTILE, component.get(0)),
            new Document(SECOND_QUARTILE, component.get(1)),
            new Document(THIRD_QUARTILE, component.get(2)));
    }

    /**
     * Produces {@link List} of {@link Document}s for given {@link ExpectedValue}.
     * @param expectedValue for test
     * @return {@link List} of {@link Document}s
     */
    public List<Document> produceExpectedData(ExpectedValue expectedValue) {
        Map series = expectedValue.getSeries();
        if (series.isEmpty()) {
            return Collections.emptyList();
        }
        Object firstCollector = series.values().iterator().next();
        if (firstCollector instanceof DoubleArrayCollector) {
            return getDocumentsByArray(expectedValue);
        } else {
            return getDocumentsBySingle(expectedValue);
        }
    }
}
