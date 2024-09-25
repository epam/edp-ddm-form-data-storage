/*
 * Copyright 2024 EPAM Systems.
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

package com.epam.digital.data.platform.storage.form.service;

import com.epam.digital.data.platform.storage.form.dto.FormDataDto;
import com.epam.digital.data.platform.storage.form.model.FormDataRedis;
import com.epam.digital.data.platform.storage.form.repository.FormDataKeyValueRepository;
import com.epam.digital.data.platform.storage.form.repository.RedisFormDataRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SetOperations;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RedisFormDataStorageServiceTest {

  private static final String REDIS_KEY_PREFIX = "bpm-form-submissions:";

  @Mock
  private FormDataKeyValueRepository redisKeyValueRepository;
  @Mock
  private RedisTemplate<String, Object> redisTemplate;
  @Mock
  private SetOperations<String, Object> redisSetOperationsMock;
  private FormDataStorageService<?> storageService;
  private FormDataKeyProvider formDataKeyProvider;

  @BeforeEach
  void init() {
    var repository = RedisFormDataRepository.builder()
        .objectMapper(new ObjectMapper())
            .repository(redisKeyValueRepository)
            .template(redisTemplate)
            .build();
    formDataKeyProvider = new FormDataKeyProviderImpl();
    storageService = RedisFormDataStorageService.builder()
        .keyProvider(formDataKeyProvider)
        .repository(repository)
        .build();
  }

  @Test
  @SneakyThrows
  void testGetFromData() {
    var taskDefKey = "taskDefKey";
    var processInstanceId = "piid";
    var key = formDataKeyProvider.generateKey(taskDefKey, processInstanceId);

    when(redisKeyValueRepository.findById(key))
        .thenReturn(
            Optional.of(
                FormDataRedis.builder()
                    .signature("signature")
                    .accessToken("token")
                    .data("{\"testField\": \"testValue\"}")
                    .build()));

    var result = storageService.getFormData(taskDefKey, processInstanceId);
    assertThat(result).isPresent();
    assertThat(result.get().getSignature()).isEqualTo("signature");
    assertThat(result.get().getAccessToken()).isEqualTo("token");
    assertThat(result.get().getData().get("testField")).isEqualTo("testValue");
  }

  @Test
  void testPutFormData() {
    var taskDefKey = "taskDefKey";
    var processInstanceId = "piid";
    var formData = FormDataDto.builder()
        .data(new LinkedHashMap<>(Map.of("testField", "testValue")))
        .build();
    var formDataStr = "{\"testField\":\"testValue\"}";
    var key = formDataKeyProvider.generateKey(taskDefKey, processInstanceId);

    when(redisTemplate.opsForSet()).thenReturn(redisSetOperationsMock);
    when(redisSetOperationsMock.add(
            "bpm-form-submissions:process-instance-id:piid", REDIS_KEY_PREFIX + key))
        .thenReturn(1L);

    when(redisKeyValueRepository.save(FormDataRedis.builder().id(key).data(formDataStr).build()))
        .thenReturn(FormDataRedis.builder().id(key).data(formDataStr).build());

    storageService.putFormData(taskDefKey, processInstanceId, formData);
    verify(redisKeyValueRepository).save(FormDataRedis.builder().id(key).data(formDataStr).build());
    verify(redisSetOperationsMock).add("bpm-form-submissions:process-instance-id:piid", REDIS_KEY_PREFIX + key);
  }

  @Test
  void testPutStartFormData() {
    var procDefKey = "procDefKey";
    var uuid = UUID.randomUUID().toString();
    var formData = FormDataDto.builder()
        .data(new LinkedHashMap<>(Map.of("testField", "testValue")))
        .build();
    var formDataStr = "{\"testField\":\"testValue\"}";
    var key = formDataKeyProvider.generateStartFormKey(procDefKey, uuid);

    storageService.putStartFormData(procDefKey, uuid, formData);
    verify(redisKeyValueRepository).save(FormDataRedis.builder().id(key).data(formDataStr).build());
    verify(redisSetOperationsMock, never()).add(anyString(), any());
  }

  @Test
  void testPutExternalSystemStartFormData() {
    var procDefKey = "procDefKey";
    var uuid = UUID.randomUUID().toString();
    var formData = FormDataDto.builder()
        .data(new LinkedHashMap<>(Map.of("testField", "testValue")))
        .build();
    var formDataStr = "{\"testField\":\"testValue\"}";
    var key = formDataKeyProvider.generateKeyForExternalSystem(procDefKey, uuid);

    storageService.putExternalSystemFormData(procDefKey, uuid, formData);
    verify(redisKeyValueRepository).save(FormDataRedis.builder().id(key).data(formDataStr).build());
    verify(redisSetOperationsMock, never()).add(anyString(), any());
  }

  @Test
  void testDeleteByProcInstId() {
    var procInstId = "id";
    var uuid = UUID.randomUUID().toString();
    var procDefKey = "procDefKey";
    var startFormKey = formDataKeyProvider.generateStartFormKey(procDefKey, uuid);
    var formDataKey = formDataKeyProvider.generateKey(procInstId, "taskDefId");
    var systemSignKey = formDataKeyProvider.generateSystemSignatureKey(procInstId, procInstId);

    var redisExistingKeys =
           Set.of(formDataKey, systemSignKey).stream()
            .map(key -> REDIS_KEY_PREFIX + key)
            .collect(Collectors.toSet());

    when(redisTemplate.opsForSet()).thenReturn(redisSetOperationsMock);
    when(redisSetOperationsMock.members("bpm-form-submissions:process-instance-id:id"))
        .thenReturn(new HashSet<>(redisExistingKeys));

    storageService.deleteByProcessInstance(procInstId, startFormKey);

    var redisKeysToDelete = Set.of(formDataKey, systemSignKey, startFormKey).stream()
            .map(key -> REDIS_KEY_PREFIX + key)
                    .collect(Collectors.toSet());
    redisKeysToDelete.add("bpm-form-submissions:process-instance-id:id");
    verify(redisTemplate).delete(redisKeysToDelete);
  }

  @Test
  @SneakyThrows
  void testGetFromDataWithStorageKey() {
    var taskDefKey = "taskKey";
    var procInstId = "id";
    var key = formDataKeyProvider.generateKey(taskDefKey, procInstId);

    when(redisKeyValueRepository.findById(key))
        .thenReturn(
            Optional.of(
                FormDataRedis.builder()
                    .signature("signature")
                    .accessToken("token")
                    .data("{\"testField\": \"testValue\"}")
                    .build()));

    var result = storageService.getFormDataWithKey(taskDefKey, procInstId);

    assertThat(result).isPresent();
    assertThat(result.get().getFormData()).isNotNull();
    assertThat(result.get().getStorageKey()).isEqualTo(key);
    assertThat(result.get().getFormData().getSignature()).isEqualTo("signature");
    assertThat(result.get().getFormData().getAccessToken()).isEqualTo("token");
    assertThat(result.get().getFormData().getData().get("testField")).isEqualTo("testValue");
  }

  @Test
  void shouldDelete() {
    var keys = Set.of(formDataKeyProvider.generateSystemSignatureKey("procInstId", "procInstId"));

    storageService.delete(keys);

    var keysToDelete = keys.stream()
        .map(key -> REDIS_KEY_PREFIX + key)
        .collect(Collectors.toSet());

    verify(redisTemplate).delete(keysToDelete);
  }
}