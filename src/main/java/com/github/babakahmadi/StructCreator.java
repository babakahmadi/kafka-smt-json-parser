package com.github.babakahmadi;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class StructCreator {

    private Schema convertSchema(String schemaString) {
        switch (schemaString) {
            case "string":
                return Schema.OPTIONAL_STRING_SCHEMA;
            case "int":
                return Schema.OPTIONAL_INT32_SCHEMA;
            case "long":
                return Schema.OPTIONAL_INT64_SCHEMA;
            case "float":
                return Schema.OPTIONAL_FLOAT32_SCHEMA;
            case "double":
                return Schema.OPTIONAL_FLOAT64_SCHEMA;
            case "boolean":
                return Schema.OPTIONAL_BOOLEAN_SCHEMA;
            default:
                throw new ConfigException("Unknown schema type: " + schemaString +
                        ". Should be one of: string, int, long, float, double, boolean");
        }
    }

    public Map<String, Schema> extractFieldSchema(String configFields) {
        return Arrays.stream(configFields.split(","))
                .map(field -> {
                    if (field.isEmpty()) {
                        throw new ConfigException("filtered fields may not be empty and separated with a comma");
                    }
                    String[] split = field.trim().split(":");
                    if (split.length != 2) {
                        throw new ConfigException("field should be name:type");
                    }
                    return split;
                }).collect(Collectors.toMap(split -> split[0], split -> convertSchema(split[1])));
    }

    private Optional<Object> extractMongoDollarField(JSONObject jsonObject) {
        Set<String> keySet = jsonObject.keySet();
        if (keySet.size() == 1) {
            String key = keySet.iterator().next();
            if (key.startsWith("$")) {
                return Optional.of(jsonObject.get(key));
            }
        }
        return Optional.empty();
    }

    private void updateMongoField(JSONObject currentObject) {
        Set<String> keySet = currentObject.keySet();
        for (String currentKey : keySet) {
            Object value = currentObject.get(currentKey);
            if (value instanceof JSONObject) {
                JSONObject jsonValue = (JSONObject) value;
                Optional<Object> mongoDollarField = extractMongoDollarField(jsonValue);
                if (mongoDollarField.isPresent()) {
                    currentObject.put(currentKey, mongoDollarField.get());
                } else {
                    updateMongoField(jsonValue);
                }
            } else if (value instanceof JSONArray) {
                JSONArray array = (JSONArray) value;
                for (int i = 0; i < array.length(); i++) {
                    Object object = array.get(i);
                    if (object instanceof JSONObject) {
                        JSONObject jsonObject = (JSONObject) object;
                        updateMongoField(jsonObject);
                    }
                }
            }
        }
    }

    Struct createStruct(String value, Map<String, Schema> filteredFields, Schema schema) {
        final Struct struct = new Struct(schema);
        JSONObject jsonObject = new JSONObject(value);
        updateMongoField(jsonObject);
        String[] fieldNames = jsonObject.keySet().toArray(new String[0]);
        for (String fieldName : fieldNames) {
            if (!filteredFields.containsKey(fieldName)) {
                continue;
            }
            Object o = jsonObject.get(fieldName);
            if (o instanceof JSONObject || o instanceof JSONArray) {
                o = o.toString();
            }
            System.out.println("fieldName:" + fieldName + ", fieldValue:" + o + ", fieldType: " + o.getClass().getName());
            struct.put(fieldName, o);
        }
        return struct;
    }

}
