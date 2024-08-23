# kafka-smt-json-parser
Kafka SMT for parsing string json to a new struct,

This SMT supports parsing a json string to a new struct. it replaces the whole record with new schema from the message.
 * it supports just primitive types : `int`, `long`, `string`, `float`, `double`, `boolean`
 * it just extracts the first level of json string
 * it extracts all mongo values that start with `$` in the inner json object

Here, because my requirement was extracting info from a string value without any schema, I had more focus on this part, 
but if I think it's important to complete other parts, I will do it ASAP.

Properties:

|Name| Description                                              |Type| Default | Importance |
|---|----------------------------------------------------------|---|---------|------------|
|`field.name`| field name if we want extract json string from the field | String | `null`  | Medium     |
|`filtered.fields`| fields with their types(`field1:type1,field2:type2`)     | String | ``  | High       |

Build:
```
mvn clean package
```
then `target/kafka-smt-json-parser-1.0-SNAPSHOT.jar` will be the file you need to add under one of the directories
listed in the `plugin.path` property in the Connect worker configuration file as shown below:
```
plugin.path=/usr/local/share/kafka/plugins
```

Example on how to add to your connector:
```
transforms=jsonparser
transforms.jsonparser.type=com.github.babakahmadi.JsonExtractor$Value
transforms.jsonparser.filtered.fields=field1:type1,field2:type2
```
if we have value in one of fields we need to add this one:
```
transforms.jsonparser.field.name=fieldName
```


ToDO:
 * Not replace the whole record with new one
 * support map, array and struct in a recursive for complex objects
 * upgrade junit4 to junit5

Reference:
    * https://docs.confluent.io/platform/current/connect/transforms/custom.html

Lots borrowed from the Apache Kafka® `InsertField` SMT

**IMPORTANT**: Any pull request is appreciated.