package org.radarcns.integration.aggregator;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code ExpectedValue} represented as {@code Double[]}.
 *      @see {@link ExpectedValue}
 */
public class ExpectedArrayValue extends ExpectedValue<DoubleArrayCollector> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExpectedArrayValue.class);

    /**
     * Constructor.
     **/
    public ExpectedArrayValue(String user, String source)
            throws InstantiationException, IllegalAccessException {
        super(user, source);
    }

    /**
     * It adds a new value the simulation taking into account if it belongs to an existing time
     *      window or not.
     * @param startTimeWindow timeZero for a time window that has this sample as initil value
     * @param timestamp time associated with the value
     * @param array sample value
     **/
    public void add(Long startTimeWindow, Long timestamp, Double[] array) {
        double[] temp = new double[array.length];

        for (int i = 0; i < array.length; i++) {
            temp[i] = array[i];
        }

        add(startTimeWindow, timestamp, temp);
    }

    /**
     * It adds a new value the simulation taking into account if it belongs to an existing time
     *      window or not.
     * @param startTimeWindow timeZero for a time window that has this sample as initil value
     * @param timestamp time associated with the value
     * @param array sample value
     **/
    public void add(Long startTimeWindow, Long timestamp, double[] array) {
        if (timestamp < lastTimestamp + DURATION) {
            lastValue.add(array);
        } else {
            lastTimestamp = startTimeWindow;
            lastValue = new DoubleArrayCollector();
            lastValue.add(array);
            series.put(startTimeWindow, lastValue);
        }
    }

}
