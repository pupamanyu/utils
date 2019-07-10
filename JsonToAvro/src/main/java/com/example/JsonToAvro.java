/*
#
# Copyright (C) 2019 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
 */

package com.example;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.kitesdk.data.spi.JsonUtil;
import tech.allegro.schema.json2avro.converter.JsonAvroConverter;

/**
 * Utility Class to generate Avro Record given a JSON String.
 * Avro Schema is inferred from JSON field values
 */
public class JsonToAvro {

  private final JsonAvroConverter jsonAvroConverter;

  JsonToAvro() {
    this.jsonAvroConverter = new JsonAvroConverter();
  }

  String getJson(byte[] avroRecord, String avroSchema) {
    return new String(jsonAvroConverter.convertToJson(avroRecord, avroSchema));
  }

  String getJson(GenericData.Record genericRecord) {
    return new String(jsonAvroConverter.convertToJson(genericRecord));
  }

  public Schema getAvroSchema(String jsonString, String schemaName) {
    return JsonUtil.inferSchema(JsonUtil.parse(jsonString), schemaName);
  }

  public byte[] getAvroRecord(String jsonString, String schemaName) {
    return jsonAvroConverter.convertToAvro(jsonString.getBytes(), getAvroSchema(jsonString, schemaName));
  }

  public GenericData.Record getGenericRecord(String jsonString, String schemaName) {
    return jsonAvroConverter.convertToGenericDataRecord(jsonString.getBytes(), getAvroSchema(jsonString, schemaName));
  }

}
