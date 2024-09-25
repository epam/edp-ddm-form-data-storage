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
import com.epam.digital.data.platform.storage.form.dto.FormDataInputWrapperDto;
import com.epam.digital.data.platform.storage.form.model.FormDataRedis;
import com.epam.digital.data.platform.storage.form.model.RedisKeysSearchParams;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Builder;
import org.springframework.data.redis.core.RedisTemplate;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

@Builder
public class RedisFormDataRepository extends BaseRedisRepository implements FormDataRepository<RedisKeysSearchParams> {

  public static final String KEY_PREFIX = "bpm-form-submissions";
  public static final String PROCESS_INSTANCE_ID_PREFIX = "process-instance-id";

  private FormDataKeyValueRepository repository;
  private RedisTemplate<String, Object> template;
  private final ObjectMapper objectMapper;

  @Override
  public Set<String> getKeysBySearchParams(RedisKeysSearchParams redisKeysSearchParams) {
    return execute(
        () -> getKeysToDeleteByProcessInstanceId(redisKeysSearchParams.getProcessInstanceId()));
  }

  private Set<String> getKeysToDeleteByProcessInstanceId(String processInstanceId) {
    var parentKey = String.format("%s:%s:%s", KEY_PREFIX, PROCESS_INSTANCE_ID_PREFIX, processInstanceId);
    var keysToDeleteByProcessInstanceId = Optional.ofNullable(template.opsForSet().members(parentKey))
            .stream().flatMap(Collection::stream)
            .map(Object::toString)
            .collect(Collectors.toSet());
    if (keysToDeleteByProcessInstanceId.isEmpty()) {
      return Collections.emptySet();
    }
    keysToDeleteByProcessInstanceId.add(parentKey);
    return keysToDeleteByProcessInstanceId;
  }

  @Override
  public void delete(Set<String> keys) {
    var keysWithPrefix = keys.stream()
        .map(k -> k.startsWith(KEY_PREFIX) ? k : String.format("%s:%s", KEY_PREFIX, k))
        .collect(Collectors.toSet());
    execute(() -> template.delete(keysWithPrefix));
  }

  @Override
  public void putFormData(FormDataInputWrapperDto formDataInputWrapperDto) {
    execute(
        () -> {
          repository.save(
              toFormDataRedis(
                  formDataInputWrapperDto.getKey(), formDataInputWrapperDto.getFormData()));
          addToProcessInstanceRelatedKeySet(
              formDataInputWrapperDto.getProcessInstanceId(), formDataInputWrapperDto.getKey());
        });
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

  private void addToProcessInstanceRelatedKeySet(String processInstanceId, String key) {
    Optional.ofNullable(processInstanceId)
        .map(
            procInstId ->
                String.format("%s:%s:%s", KEY_PREFIX, PROCESS_INSTANCE_ID_PREFIX, procInstId))
        .ifPresent(
            processInstanceIdRelatedKeysSet ->
                template
                    .opsForSet()
                    .add(processInstanceIdRelatedKeysSet, String.format("%s:%s", KEY_PREFIX, key)));
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
