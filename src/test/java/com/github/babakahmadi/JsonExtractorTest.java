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

package com.github.babakahmadi;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class JsonExtractorTest {

    private final String sampleJsonString = "{\"_id\": {\"$oid\": \"66c5f3e7ab09aa8f6cc84e8b\"},\"unitName\": \"cashier\",\"unitValue\": \"1\",\"number\": 98}";
    private final String filteredFields = "unitName:string,unitValue:string, _id:string, storeId:string";

    private JsonExtractor<SourceRecord> xform = new JsonExtractor.Value<>();

    @After
    public void tearDown() throws Exception {
        xform.close();
    }

    private Map<String, String> createFieldProps() {
        Map<String, String> props = new HashMap<>();
        props.put("filtered.fields", filteredFields);
        props.put("field.name", "json");
        return props;
    }

    @Test(expected = DataException.class)
    public void fieldNameRequired() {
        xform.configure(createFieldProps());
        xform.apply(new SourceRecord(null, null, "", 0,
                Schema.STRING_SCHEMA, "{\"_id\": \"turnip\"}"));
    }

    @Test
    public void fieldJsonExtractor() {
        xform.configure(createFieldProps());

        final Schema simpleStructSchema = SchemaBuilder.struct()
                .name("name").version(1).doc("doc").field("json", Schema.STRING_SCHEMA)
                .build();
        final Struct simpleStruct = new Struct(simpleStructSchema).put("json", sampleJsonString);

        final SourceRecord record = new SourceRecord(null, null, "test", 0, simpleStructSchema, simpleStruct);
        final SourceRecord transformedRecord = xform.apply(record);

        assertEquals(Schema.OPTIONAL_STRING_SCHEMA, transformedRecord.valueSchema().field("_id").schema());
        assertEquals("66c5f3e7ab09aa8f6cc84e8b", ((Struct) transformedRecord.value()).getString("_id"));
        assertEquals(Schema.OPTIONAL_STRING_SCHEMA, transformedRecord.valueSchema().field("unitName").schema());
        assertNotNull(((Struct) transformedRecord.value()).getString("unitName"));
    }

    @Test
    public void inlineJsonExtractor() {
        final Map<String, Object> props = new HashMap<>();
        props.put("filtered.fields", filteredFields);

        xform.configure(props);

        final SourceRecord record = new SourceRecord(null, null, "test", 0,
                null, sampleJsonString);

        final SourceRecord transformedRecord = xform.apply(record);

        assertEquals(Schema.OPTIONAL_STRING_SCHEMA, transformedRecord.valueSchema().field("_id").schema());
        assertEquals("66c5f3e7ab09aa8f6cc84e8b", ((Struct) transformedRecord.value()).getString("_id"));
        assertEquals(Schema.OPTIONAL_STRING_SCHEMA, transformedRecord.valueSchema().field("unitName").schema());
        assertNotNull(((Struct) transformedRecord.value()).getString("unitName"));
    }
}