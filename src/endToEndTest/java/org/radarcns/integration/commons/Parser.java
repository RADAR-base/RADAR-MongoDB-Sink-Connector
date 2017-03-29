/*
 *  Copyright 2017 Kings College London and The Hyve
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

package org.radarcns.integration.commons;

import com.opencsv.CSVReader;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificData;
import org.radarcns.integration.aggregator.ExpectedValue.ExpectedType;
import org.radarcns.mock.MockDataConfig;

/**
 * Starting from a CVS file, this parser generates a map containing all available fields.
 *      The {@link Variable#VALUE} field can contain either a Double or an array of Doubles.
 */
public class Parser {

    /**
     * Enumerator containing all fields returned by the next function @see {@link Parser#next()}.
     **/
    public enum Variable {
        USER("userId"),
        SOURCE("sourceId"),
        TIME_WINDOW("timeWindow"),
        TIMESTAMP("timeReceived"),
        EXPECTED_TYPE("expectedType"),
        VALUE("value");

        private String value;

        Variable(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        @Override
        @SuppressWarnings("checkstyle:JavadocMethod")
        public String toString() {
            return this.getValue();
        }

        /**
         * @param value representing a {@code Variable} item
         * @return the {@code Variable} that matches the input.
         **/
        public static Variable getEnum(String value) {
            for (Variable v : values()) {
                if (v.getValue().equalsIgnoreCase(value)) {
                    return v;
                }
            }
            throw new IllegalArgumentException();
        }

        /**
         * @return a {@code List} of Variables.
         **/
        public static List<Variable> toList() {
            return new ArrayList<Variable>() {
                {
                    add(USER);
                    add(SOURCE);
                    add(TIME_WINDOW);
                    add(TIMESTAMP);
                    add(EXPECTED_TYPE);
                    add(VALUE);
                }
            };
        }
    }

    private final CSVReader csvReader;
    private final Map<String, Integer> headerMap;

    private final ExpectedType expecedType;

    private final MockDataConfig config;

    //private static final Logger logger = LoggerFactory.getLogger(Parser.class);

    /**
     * Constructor that initialises the {@code CSVReader} and computes the {@code ExpectedType}.
     * @param config containing the CSV file path that has to be parsed
     **/
    public Parser(MockDataConfig config)
        throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException,
        IllegalAccessException, IOException {

        this.config = config;

        Class<?> keyClass = Class.forName(config.getKeySchema());
        Schema keySchema = (Schema) keyClass.getMethod("getClassSchema").invoke(null);
        SpecificData.newInstance(keyClass, keySchema);

        Class<?> valueClass = Class.forName(config.getValueSchema());
        Schema valueSchema = (Schema) valueClass.getMethod("getClassSchema")
                .invoke(null);
        SpecificData.newInstance(valueClass, valueSchema);

        csvReader = new CSVReader(new FileReader(config.getAbsoluteDataFile()));

        headerMap = new HashMap<>();
        getHeader();

        expecedType = getExpectedType(config);
    }

    /**
     * @return {@code Map} of key {@link Variable} and value Object computed by the
     *      next available raw of CSV file.
     **/
    public Map<Variable, Object> next() throws IOException {
        HashMap<Variable, Object> map = null;

        String[] rawValues = csvReader.readNext();
        if (rawValues != null) {
            map = new HashMap<>();

            for (Variable var : Variable.toList()) {
                map.put(var, computeValue(rawValues, var, config));
            }
        }

        return map;
    }

    /**
     * Close the {@code Parser} closing the CSV reader.
     **/
    public void close() throws IOException {
        csvReader.close();
    }

    /**
     * @return {@code ExpectedType} type precomputed during the instantiation.
     **/
    public ExpectedType getExpecedType() {
        return expecedType;
    }

    /**
     * @param config parameters used to compute the returned value.
     * @return the {@code ExpectedType} type based on the input.
     **/
    public static ExpectedType getExpectedType(MockDataConfig config) {
        if ( config.getSensor().contains("ACCELEROMETER") ) {
            return ExpectedType.ARRAY;
        }

        return ExpectedType.DOUBLE;
    }

    /**
     * @param rawValues array of Strings containing data parsed from the CSV file.
     * @param var variable that has to be extracted from the raw data.
     * @return an {@code Object} representing the required variable.
     **/
    private Object computeValue(String[] rawValues, Variable var, MockDataConfig config) {
        switch (var) {
            case USER:
                existOrThrow(var.getValue());
                return rawValues[headerMap.get(var.getValue())];
            case SOURCE:
                existOrThrow(var.getValue());
                return rawValues[headerMap.get(var.getValue())];
            case TIMESTAMP:
                existOrThrow(var.getValue());
                return getTimestamp(rawValues[headerMap.get(var.getValue())]);
            case VALUE:
                return extractValue(rawValues, config);
            case TIME_WINDOW:
                return getStartTimeWindow(rawValues[headerMap.get(Variable.TIMESTAMP.getValue())]);
            case EXPECTED_TYPE:
                return expecedType;
            default:
                throw new IllegalArgumentException("Cannot handle variable of type "
                    + var.getValue());
        }
    }

    /**
     * @param rawValues array of Strings containing data parsed from the CSV file.
     * @param config states the variable name that has to be parsed.
     * @return either a {@code Double} or a {@code Double[]} according to the expected value type.
     **/
    //TODO use the Parser#parseValue function to verify if the associted Schema is representing the
    // required field has a Double or a List<Double>
    private Object extractValue(String[] rawValues, MockDataConfig config) {
        if (expecedType.equals(ExpectedType.DOUBLE)) {

            existOrThrow(config.getAssertHeader());

            return Double.parseDouble(rawValues[headerMap.get(config.getAssertHeader())]);
        } else if (expecedType.equals(ExpectedType.ARRAY)) {

            String[] testCase = config.getAssertHeader().split(", ");
            Double[] value = new Double[testCase.length];

            for (int i = 0; i < testCase.length; i++) {
                existOrThrow(testCase[i]);
                value[i] = Double.parseDouble(rawValues[headerMap.get(testCase[i])]);
            }

            return value;
        }

        throw new IllegalArgumentException("Illegale expected Type " + expecedType.getValue());
    }

    /**
     * Initialise the {@code HashMap} useful for converting a variable name to the relative index
     *  in the raw data array.
     **/
    public void getHeader() throws IOException {
        String[] header = csvReader.readNext();

        for (int i = 0; i < header.length; i++) {
            headerMap.put(header[i], i);
        }
    }

    /**
     * @param schema Avro schema @see {@link Schema}.
     * @param fieldString value that has to be deserialised.
     * @return the input value instantiated according to the type stated by the {@code Schema}.
     **/
    private Object parseValue(Schema schema, String fieldString) {
        switch (schema.getType()) {
            case INT:
                return Integer.parseInt(fieldString);
            case LONG:
                return Long.parseLong(fieldString);
            case FLOAT:
                return Float.parseFloat(fieldString);
            case DOUBLE:
                return Double.parseDouble(fieldString);
            case BOOLEAN:
                return Boolean.parseBoolean(fieldString);
            case STRING:
                return fieldString;
            case ARRAY:
                return parseArray(schema, fieldString);
            default:
                throw new IllegalArgumentException("Cannot handle schemas of type "
                    + schema.getType());
        }
    }

    /**
     * @param schema Avro schema. * @see {@link Schema}.
     * @param fieldString value that has to be deserialised.
     * @return the input value deserialised ad {@code List<Object>}.
     **/
    private List<Object> parseArray(Schema schema, String fieldString) {
        if (fieldString.charAt(0) != '['
                || fieldString.charAt(fieldString.length() - 1) != ']') {
            throw new IllegalArgumentException("Array must be enclosed by brackets.");
        }

        List<String> subStrings = new ArrayList<>();
        StringBuilder buffer = new StringBuilder(fieldString.length());
        int depth = 0;
        for (char c : fieldString.substring(1, fieldString.length() - 1).toCharArray()) {
            if (c == ';' && depth == 0) {
                subStrings.add(buffer.toString());
                buffer.setLength(0);
            } else {
                buffer.append(c);
                if (c == '[') {
                    depth++;
                } else if (c == ']') {
                    depth--;
                }
            }
        }
        if (buffer.length() > 0) {
            subStrings.add(buffer.toString());
        }

        List ret = new ArrayList(subStrings.size());
        for (String substring : subStrings) {
            ret.add(parseValue(schema.getElementType(), substring));
        }
        return ret;
    }

    /**
     * @param value String value that has to be deserialised.
     * @return the number of milliseconds since January 1, 1970, 00:00:00 GMT representing the
     *      initial time of a Kafka time window.
     **/
    private Long getStartTimeWindow(String value) {
        Double timeDouble = Double.parseDouble(value) / 10d;
        return timeDouble.longValue() * 10000;
    }

    /**
     * @param value String value that has to be deserialised.
     * @return the number of milliseconds since January 1, 1970, 00:00:00 GMT represented by this
     *      String input.
     **/
    private Long getTimestamp(String value) {
        Double timeDouble  = Double.valueOf(value) * 1000d;
        return timeDouble.longValue();
    }

    /**
     * @param key field name that has to be verified.
     * @throws IllegalArgumentException if the headers map does not contain the input key.
     **/
    private void existOrThrow(String key) {
        if (!headerMap.containsKey(key)) {
            throw new IllegalArgumentException("Headers does not contain " + key);
        }
    }
}