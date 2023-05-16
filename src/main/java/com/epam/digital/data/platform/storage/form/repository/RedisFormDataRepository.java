/*
 * Copyright 2023 EPAM Systems.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.epam.digital.data.platform.storage.form.repository;


import com.epam.digital.data.platform.storage.form.dto.FormDataDto;
import com.epam.digital.data.platform.storage.form.model.FormDataRedis;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Builder;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ScanOptions;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

@Builder
public class RedisFormDataRepository extends BaseRedisRepository implements FormDataRepository {

  public static final String KEY_PREFIX = "bpm-form-submissions";

  private FormDataKeyValueRepository repository;
  private RedisTemplate<String, Object> template;
  private final ObjectMapper objectMapper;
  @Builder.Default
  private long count = 100;

  @Override
  public Set<String> getKeys(String pattern) {
    return execute(() -> getKeysWithPrefix(pattern));
  }

  private Set<String> getKeysWithPrefix(String prefix) {
    var keys = new HashSet<String>();
    var options = ScanOptions.scanOptions()
        .match(String.format("%s:%s*", KEY_PREFIX, prefix))
        .count(count)
        .build();
    try (
        var con = template.getConnectionFactory().getConnection();
        var cursor = con.scan(options)
    ) {
      while (cursor.hasNext()) {
        byte[] key = cursor.next();
        keys.add(new String(key, StandardCharsets.UTF_8));
      }
    }
    return keys;
  }

  @Override
  public void delete(Set<String> keys) {
    var keysWithPrefix = keys.stream()
        .map(k -> k.startsWith(KEY_PREFIX) ? k : String.format("%s:%s", KEY_PREFIX, k))
        .collect(Collectors.toSet());
    execute(() -> template.delete(keysWithPrefix));
  }

  @Override
  public Set<String> keys() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putFormData(String key, FormDataDto content) {
    execute(() -> repository.save(toFormDataRedis(key, content)));
  }

  @Override
  public Optional<FormDataDto> getFormData(String key) {
    if (Objects.isNull(key)) {
      return Optional.empty();
    }
    var data = repository.findById(key);
    return execute(() -> data.map(this::toFormDataDto));
  }

  private FormDataDto toFormDataDto(FormDataRedis formDataRedis) {
    var data = formDataRedis.getData();
    return FormDataDto.builder()
        .data(Optional.ofNullable(data).map(this::deserializeData).orElse(null))
        .accessToken(formDataRedis.getAccessToken())
        .signature(formDataRedis.getSignature())
        .build();
  }

  private FormDataRedis toFormDataRedis(String key, FormDataDto formDataDto) {
    return FormDataRedis.builder()
        .id(key)
        .accessToken(formDataDto.getAccessToken())
        .data(serializeData(formDataDto.getData()))
        .signature(formDataDto.getSignature())
        .build();
  }

  private LinkedHashMap<String, Object> deserializeData(String formData) {
    try {
      return objectMapper.readValue(formData, LinkedHashMap.class);
    } catch (IOException e) {
      throw new IllegalArgumentException("Couldn't deserialize data", e);
    }
  }

  private String serializeData(LinkedHashMap<String, Object> formData) {
    try {
      return objectMapper.writeValueAsString(formData);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Couldn't serialize data", e);
    }
  }
}
