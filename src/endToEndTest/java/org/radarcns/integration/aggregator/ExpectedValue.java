package org.radarcns.integration.aggregator;

import static org.radarcns.integration.aggregator.ExpectedValue.STAT_TYPE.AVERAGE;
import static org.radarcns.integration.aggregator.ExpectedValue.STAT_TYPE.COUNT;
import static org.radarcns.integration.aggregator.ExpectedValue.STAT_TYPE.INTERQUARTILE_RANGE;
import static org.radarcns.integration.aggregator.ExpectedValue.STAT_TYPE.MAXIMUM;
import static org.radarcns.integration.aggregator.ExpectedValue.STAT_TYPE.MEDIAN;
import static org.radarcns.integration.aggregator.ExpectedValue.STAT_TYPE.MINIMUM;
import static org.radarcns.integration.aggregator.ExpectedValue.STAT_TYPE.QUARTILES;
import static org.radarcns.integration.aggregator.ExpectedValue.STAT_TYPE.SUM;

import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import org.radarcns.avro.restapi.dataset.Dataset;
//import org.radarcns.avro.restapi.dataset.Item;
//import org.radarcns.avro.restapi.dataset.Quartiles;
//import org.radarcns.avro.restapi.header.DescriptiveStatistic;
//import org.radarcns.avro.restapi.header.EffectiveTimeFrame;
//import org.radarcns.avro.restapi.header.Header;
//import org.radarcns.avro.restapi.sensor.Acceleration;
//import org.radarcns.avro.restapi.sensor.Battery;
//import org.radarcns.avro.restapi.sensor.BloodVolumePulse;
//import org.radarcns.avro.restapi.sensor.ElectroDermalActivity;
//import org.radarcns.avro.restapi.sensor.HeartRate;
//import org.radarcns.avro.restapi.sensor.InterBeatInterval;
//import org.radarcns.avro.restapi.sensor.SensorType;
//import org.radarcns.avro.restapi.sensor.Temperature;
//import org.radarcns.avro.restapi.sensor.Unit;
//import org.radarcns.avro.restapi.source.SourceType;
//import org.radarcns.source.SourceCatalog;
//import org.radarcns.util.RadarConverter;

/**
 * It computes the expected value for a test case.
 */
public abstract class ExpectedValue<V> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExpectedValue.class);

    public enum STAT_TYPE {
        MINIMUM,
        MAXIMUM,
        SUM,
        QUARTILES,
        MEDIAN,
        INTERQUARTILE_RANGE,
        COUNT,
        AVERAGE

    }
    /**
     * Enumerator containing all possible collector implementations. Useful to understand if
     *      the current isntance is managing single doubles or arrays of doubles.
     * @see {@link DoubleArrayCollector}
     * @see {@link DoubleValueCollector}
     **/
    public enum ExpectedType {
        ARRAY("org.radarcns.integration.aggregator.DoubleArrayCollector"),
        DOUBLE("org.radarcns.integration.aggregator.DoubleValueCollector");

        private String value;

        ExpectedType(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        @Override
        public String toString() {
            return this.getValue();
        }

        /**
         * Return the {@code ExpectedType} associated to the input String.
         * @param value representing an {@code ExpectedType} item
         * @return the {@code ExpectedType} that matches the input
         **/
        public static ExpectedType getEnum(String value) {
            for (ExpectedType v : values()) {
                if (v.getValue().equalsIgnoreCase(value)) {
                    return v;
                }
            }
            throw new IllegalArgumentException();
        }
    }

    //Timewindow length in milliseconds
    @SuppressWarnings({"checkstyle:AbbreviationAsWordInName","checkstyle:MemberName"})
    protected long DURATION = TimeUnit.SECONDS.toMillis(10);

    protected String user;
    protected String source;

    protected Long lastTimestamp;
    protected V lastValue;
    protected HashMap<Long, V> series;


    /** Constructor. **/
    public ExpectedValue(String user, String source)
            throws IllegalAccessException, InstantiationException {
        series = new HashMap<>();

        this.user = user;
        this.source = source;
        lastTimestamp = 0L;

        Class<V> valueClass = (Class<V>) ((ParameterizedType) getClass()
                .getGenericSuperclass()).getActualTypeArguments()[0];

        lastValue = valueClass.newInstance();
    }

//    /**
//     * @return {@code EffectiveTimeFrame} for the simulated inteval.
//     * @see {@link org.radarcns.avro.restapi.header.EffectiveTimeFrame}
//     */
//    public EffectiveTimeFrame getEffectiveTimeFrame() {
//        List<Long> windows = new ArrayList<>(series.keySet());
//        Collections.sort(windows);
//
//        EffectiveTimeFrame eft = new EffectiveTimeFrame(
//            RadarConverter.getISO8601(new Date(windows.get(0))),
//            RadarConverter.getISO8601(new Date(windows.get(windows.size() - 1)
//                + DURATION)));
//
//        return eft;
//    }
//
//    /**
//     * @param value timestamp.
//     * @return {@code EffectiveTimeFrame} starting on value and ending
//     *      {@link ExpectedValue#DURATION} milliseconds after.
//     * @see {@link org.radarcns.avro.restapi.header.EffectiveTimeFrame}
//     */
//    public EffectiveTimeFrame getEffectiveTimeFrame(Long value) {
//        return new EffectiveTimeFrame(RadarConverter.getISO8601(new Date(value)),
//            RadarConverter.getISO8601(new Date(value + DURATION)));
//    }
//
//    /**
//     * It generates the {@code Header} for the resulting {@code Dataset}.
//     * @param statistic function that has to be simulated
//     * @param unit values unit
//     * @return {@link org.radarcns.avro.restapi.header.Header} for a
//     *      {@link org.radarcns.avro.restapi.dataset.Dataset}
//     **/
//    public Header getHeader(DescriptiveStatistic statistic, Unit unit) {
//        return new Header(statistic, unit, getEffectiveTimeFrame());
//    }
//
//    /**
//     * It generates the {@code List<Item>} for the resulting {@code Dataset}
//     *      @see {@link org.radarcns.avro.restapi.dataset.Item}.
//     * @param statistic function that has to be simulated
//     * @return {@code List<Item>} for a {@link org.radarcns.avro.restapi.dataset.Dataset}
//     **/
//    public List<Item> getItem(DescriptiveStatistic statistic, SensorType sensorType)
//        throws IllegalAccessException, InstantiationException {
//
//        List<Long> keys = new LinkedList<>(series.keySet());
//        Collections.sort(keys);
//
//        switch (getExpectedType()) {
//            case ARRAY: return getArrayItems(keys, statistic, sensorType);
//            case DOUBLE: return getSingletonItems(keys, statistic, sensorType);
//            default:
//                throw new IllegalArgumentException(sensorType.name() + " not supported yet");
//        }
//    }
//
    public ExpectedType getExpectedType() {
        for (ExpectedType expectedType : ExpectedType.values()) {
            if (expectedType.getValue().equals(lastValue.getClass().getCanonicalName())) {
                return expectedType;
            }
        }

        return null;
    }

//    /**
//     * It generates the {@code List<Item>} for the resulting {@code Dataset}
//     *      @see {@link org.radarcns.avro.restapi.dataset.Item} containg sensor data that can be
//     *      represented as array of {@code Double}.
//     * @param keys {@code Collection} of timewindow initial time
//     * @param statistic function that has to be simulated
//     * @param sensor
//     * @return {@code List<Item>} for a {@link org.radarcns.avro.restapi.dataset.Dataset}
//     **/
//    private List<Item> getArrayItems(Collection<Long> keys, DescriptiveStatistic statistic,
//            SensorType sensor) {
//        List<Item> items = new LinkedList<>();
//
//        for (Long key : keys) {
//            DoubleArrayCollector dac = (DoubleArrayCollector) series.get(key);
//
//            switch (sensor) {
//                case ACCELEROMETER:
//                    Object content;
//
//                    if (statistic.name().equals(QUARTILES.name())) {
//                        List<List<Double>> statValues = (List<List<Double>>) getStatValue(statistic,
//                            dac.getCollectors());
//                        content = new Acceleration(getQuartile(statValues.get(0)),
//                            getQuartile(statValues.get(1)), getQuartile(statValues.get(2)));
//                    } else {
//                        List<Double> statValues = (List<Double>) getStatValue(statistic,
//                            dac.getCollectors());
//                        content = new Acceleration(statValues.get(0), statValues.get(1),
//                            statValues.get(2));
//                    }
//                    items.add(new Item(content, getEffectiveTimeFrame(key)));
//                    break;
//                default:
//                    throw new IllegalArgumentException(sensor.name()
//                        + " is not a supported test case");
//            }
//        }
//
//        return items;
//    }
//
//    /**
//     * It generates the {@code List<Item>} for the resulting {@code Dataset}
//     *      @see {@link org.radarcns.avro.restapi.dataset.Item} containg sensor data that can be
//     *      represented as {@code Double}.
//     * @param keys {@code Collection} of timewindow initial time
//     * @param statistic function that has to be simulated
//     * @param sensor
//     * @return {@code List<Item>} for a {@link org.radarcns.avro.restapi.dataset.Dataset}
//     **/
//    private List<Item> getSingletonItems(Collection<Long> keys, DescriptiveStatistic statistic,
//            SensorType sensor) throws InstantiationException, IllegalAccessException {
//        List<Item> items = new LinkedList<>();
//
//        for (Long key : keys) {
//            DoubleArrayCollector.DoubleValueCollector dac = (DoubleArrayCollector.DoubleValueCollector) series.get(key);
//
//            Object content = getContent(getStatValue(statistic, dac), statistic,
//                    getSensorClass(sensor));
//
//            items.add(new Item(content, getEffectiveTimeFrame(key)));
//        }
//
//        return items;
//    }
//
//
//    public <T extends SpecificRecord> T getContent(Object object, DescriptiveStatistic stat,
//            Class<T> tClass) throws IllegalAccessException, InstantiationException {
//        T content;
//
//        switch (stat) {
//            case QUARTILES: content = tClass.newInstance();
//                            content.put(content.getSchema().getField("value").pos(),
//                                getQuartile((List<Double>) object));
//                break;
//            default: content = tClass.newInstance();
//                content.put(content.getSchema().getField("value").pos(), object);
//        }
//
//        return content;
//    }
//
//    public Class getSensorClass(SensorType sensor) {
//        switch (sensor) {
//            case ACCELEROMETER: return Acceleration.class;
//            case BATTERY: return Battery.class;
//            case BLOOD_VOLUME_PULSE: return BloodVolumePulse.class;
//            case ELECTRODERMAL_ACTIVITY: return ElectroDermalActivity.class;
//            case INTER_BEAT_INTERVAL: return InterBeatInterval.class;
//            case HEART_RATE: return HeartRate.class;
//            case THERMOMETER: return Temperature.class;
//            default: throw new IllegalArgumentException(sensor.name()
//                        + " is not a supported test case");
//        }
//    }
//
    /**
     * It return the value of the given statistical function.
     * @param statistic function that has to be returned
     * @param collectors array of aggregated data
     *      @see {@link org.radarcns.integration.aggregator.DoubleValueCollector}
     * @return the set of values that has to be stored within a {@code Dataset} {@code Item}
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
            case AVERAGE: return avgList;
            case COUNT: return countList;
            case INTERQUARTILE_RANGE: return iqrList;
            case MAXIMUM: return maxList;
            case MEDIAN: return medList;
            case MINIMUM: return minList;
            case QUARTILES: return quartileList;
            case SUM: return sumList;
            default: throw new IllegalArgumentException(
                statistic.toString() + " is not supported");
        }
    }

    /**
     * It return the value of the given statistical function.
     * @param statistic function that has to be returned
     * @param collector data aggregator
     *      @see {@link org.radarcns.integration.aggregator.DoubleValueCollector}
     * @return the value that has to be stored within a {@code Dataset} {@code Item}
     **/
    private Object getStatValue(STAT_TYPE statistic,
        DoubleValueCollector collector) {

        switch (statistic) {
            case AVERAGE: return collector.getAvg();
            case COUNT: return collector.getCount();
            case INTERQUARTILE_RANGE: return collector.getIqr();
            case MAXIMUM: return collector.getMax();
            case MEDIAN: return collector.getQuartile().get(1);
            case MINIMUM: return collector.getMin();
            case QUARTILES: return collector.getQuartile();
            case SUM: return collector.getSum();
            default: throw new IllegalArgumentException(
                statistic.toString() + " is not supported");
        }
    }

//    /**
//     * @param list of {@code Double} values representing a quartile.
//     * @return the value that has to be stored within a {@code Dataset} {@code Item}
//     *      @see {@link org.radarcns.avro.restapi.dataset.Quartiles}.
//     **/
//    private Quartiles getQuartile(List<Double> list) {
//        return new Quartiles(list.get(0), list.get(1), list.get(2));
//    }
//
//    /**
//     * It computes the {@code Dataset} resulted from the mock data.
//     * @param statistic function that has to be simulated
//     * @param source the simulated source device
//     * @param sensor the simulated sensor of the source device
//     * @return {@code Dataset} resulted by the simulation
//     *      @see {@link org.radarcns.avro.restapi.dataset.Dataset}
//     **/
//    public Dataset getDataset(DescriptiveStatistic statistic, SourceType source,
//            SensorType sensor) throws InstantiationException, IllegalAccessException {
//        return new Dataset(getHeader(statistic,
//                SourceCatalog.getInstance(source).getMeasurementUnit(sensor)),
//                getItem(statistic, sensor));
//    }
//
//    @Override
//    public String toString() {
//        String result = "";
//        for (Long interval : series.keySet()) {
//            result += interval.toString() + "-" + (interval + DURATION) + '\t'
//                + series.get(interval).toString() + "\n";
//        }
//        return result;
//    }

    public List<Document> getDocuments() {
        switch (getExpectedType()) {
            case ARRAY: return getDocumentsByArray();
            default: return getDocumentsBySingle();
        }
    }
//
    private List<Document> getDocumentsBySingle() {
        LinkedList<Document> list = new LinkedList<>();

        List<Long> windows = new ArrayList<>(series.keySet());
        Collections.sort(windows);

        DoubleValueCollector doubleValueCollector;
        Long end;
        for (Long timestamp : windows) {
            doubleValueCollector = (DoubleValueCollector) series.get(timestamp);

            end = timestamp + DURATION;

            list.add( new Document("_id", user + "-" + source + "-" + timestamp + "-" + end)
                .append("user", user)
                .append("source", source)
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
//
    private List<Document> getDocumentsByArray() {
        LinkedList<Document> list = new LinkedList<>();

        List<Long> windows = new ArrayList<>(series.keySet());
        Collections.sort(windows);

        DoubleArrayCollector doubleArrayCollector;
        Long end;
        for (Long timestamp : windows) {
            doubleArrayCollector = (DoubleArrayCollector) series.get(timestamp);

            end = timestamp + DURATION;

            list.add( new Document("_id", user + "-" + source + "-" + timestamp + "-" + end)
                .append("user", user)
                .append("source", source)
                .append("min",  getStatValue(MINIMUM, doubleArrayCollector.getCollectors()))
                .append("max", getStatValue(MAXIMUM, doubleArrayCollector.getCollectors()))
                .append("sum", getStatValue(SUM, doubleArrayCollector.getCollectors()))
                .append("count", getStatValue(COUNT, doubleArrayCollector.getCollectors()))
                .append("avg", getStatValue(AVERAGE, doubleArrayCollector.getCollectors()))
                .append("quartile", extractQuartile((List<Double>) getStatValue(
                        QUARTILES, doubleArrayCollector.getCollectors())))
                .append("iqr", getStatValue(INTERQUARTILE_RANGE,
                        doubleArrayCollector.getCollectors()))
                .append("start", new Date(timestamp))
                .append("end", new Date(end)));
        }

        return list;
    }

    public static List<Document> extractQuartile(List<Double> component) {
        return Arrays.asList(new Document[]{
            new Document("25", component.get(0)),
            new Document("50", component.get(1)),
            new Document("75", component.get(2))
        });
    }
//
////    /**
////     * Compare two {@code EffectiveTimeFrame} values.
////     *      @see {@link org.radarcns.avro.restapi.header.EffectiveTimeFrame}
////     * @param window1 first component that to has to be compared
////     * @param window2 second component that to has to be compared
////     * @return {@code true} if they match, false otherwise
////     **/
////    public static boolean compareEffectiveTimeFrame(EffectiveTimeFrame window1,
////        EffectiveTimeFrame window2) {
////        return window1.getStartDateTime().equals(window2.getStartDateTime())
////            && window1.getEndDateTime().equals(window2.getEndDateTime());
////    }

}
