package com.github.babakahmadi;

/*
 * Copyright © 2024 Babak Ahmadi (babak.ahmadi87@gmail.com)
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

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;


public abstract class JsonExtractor<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC =
            "parse json string to a new struct";
    private static final StructCreator creator = new StructCreator();
    private static final Logger log = LoggerFactory.getLogger(JsonExtractor.class);

    private interface ConfigName {
        String FIELD_NAME = "field.name";
        String FILTERED_FIELDS = "filtered.fields";
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.FIELD_NAME, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM,
                    "if field name is set, then we have to parse the value for that field")
            .define(ConfigName.FILTERED_FIELDS, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH,
                    "filtered fields we want, then we have to specify the field name");

    private static final String PURPOSE = "create a new struct from json string";

    private String fieldName;
    private Map<String, Schema> filteredFields;


    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        this.fieldName = config.getString(ConfigName.FIELD_NAME);
        this.filteredFields = creator.extractFieldSchema(config.getString(ConfigName.FILTERED_FIELDS));
    }

    @Override
    public R apply(R record) {
        if (this.fieldName == null) {
            final String value = operatingValue(record).toString();
            return applyValue(record, value);
        }

        final Struct value = requireStruct(operatingValue(record), PURPOSE);

        if (value.get(fieldName) == null) {
            log.warn("field name {} not found in record", fieldName);
            throw new DataException("field name " + fieldName + " not found in record");
        }
        return applyValue(record, value.getString(fieldName));
    }

    private R applyValue(R record, String value) {
        Schema newSchema = createSchema();
        Struct newValue = creator.createStruct(value, filteredFields, newSchema);
        return newRecord(record, newSchema, newValue);
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
    }

    private Schema createSchema() {
        final SchemaBuilder builder = new SchemaBuilder(Schema.Type.STRUCT);
        filteredFields.forEach(builder::field);
        return builder.build();
    }

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Struct updatedValue);

    public static class Key<R extends ConnectRecord<R>> extends JsonExtractor<R> {

        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, Schema schema, Struct struct) {
            return record.newRecord(
                    record.topic(), record.kafkaPartition(), schema, struct,
                    record.valueSchema(), record.value(), record.timestamp()
            );
        }

    }

    public static class Value<R extends ConnectRecord<R>> extends JsonExtractor<R> {

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema schema, Struct struct) {
            return record.newRecord(
                    record.topic(), record.kafkaPartition(), record.keySchema(),
                    record.key(), schema, struct, record.timestamp()
            );
        }

    }
}


