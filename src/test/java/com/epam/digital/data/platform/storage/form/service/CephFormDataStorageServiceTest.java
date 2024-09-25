/*
 * Copyright 2021 EPAM Systems.
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

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.epam.digital.data.platform.integration.ceph.exception.CephCommunicationException;
import com.epam.digital.data.platform.integration.ceph.exception.MisconfigurationException;
import com.epam.digital.data.platform.integration.ceph.service.CephService;
import com.epam.digital.data.platform.storage.form.dto.FormDataDto;
import com.epam.digital.data.platform.storage.form.exception.FormDataRepositoryCommunicationException;
import com.epam.digital.data.platform.storage.form.exception.FormDataRepositoryMisconfigurationException;
import com.epam.digital.data.platform.storage.form.repository.CephFormDataRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class CephFormDataStorageServiceTest {

  private final String bucketName = "bucket";

  @Mock
  private CephService cephService;
  private FormDataStorageService<?> storageService;
  private FormDataKeyProvider formDataKeyProvider;

  @BeforeEach
  void init() {
    var repository = CephFormDataRepository.builder()
        .objectMapper(new ObjectMapper())
        .cephBucketName(bucketName)
        .cephService(cephService)
        .build();
    formDataKeyProvider = new FormDataKeyProviderImpl();
    storageService = CephFormDataStorageService.builder()
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
    var formDataAsStr = new String(Objects.requireNonNull(
            CephFormDataStorageServiceTest.class.getResourceAsStream("/json/testFormData.json"))
        .readAllBytes());

    when(cephService.getAsString(bucketName, key)).thenReturn(Optional.of(formDataAsStr));

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
    var formDataStr = "{\"data\":{\"testField\":\"testValue\"}}";
    var key = formDataKeyProvider.generateKey(taskDefKey, processInstanceId);

    storageService.putFormData(taskDefKey, processInstanceId, formData);
    verify(cephService).put(bucketName, key, formDataStr);
  }

  @Test
  void testPutStartFormData() {
    var procDefKey = "procDefKey";
    var uuid = UUID.randomUUID().toString();
    var formData = FormDataDto.builder()
        .data(new LinkedHashMap<>(Map.of("testField", "testValue")))
        .build();
    var formDataStr = "{\"data\":{\"testField\":\"testValue\"}}";
    var key = formDataKeyProvider.generateStartFormKey(procDefKey, uuid);

    storageService.putStartFormData(procDefKey, uuid, formData);
    verify(cephService).put(bucketName, key, formDataStr);
  }

  @Test
  void testPutExternalSystemStartFormData() {
    var procDefKey = "procDefKey";
    var uuid = UUID.randomUUID().toString();
    var formData = FormDataDto.builder()
        .data(new LinkedHashMap<>(Map.of("testField", "testValue")))
        .build();
    var formDataStr = "{\"data\":{\"testField\":\"testValue\"}}";
    var key = formDataKeyProvider.generateKeyForExternalSystem(procDefKey, uuid);

    storageService.putExternalSystemFormData(procDefKey, uuid, formData);
    verify(cephService).put(bucketName, key, formDataStr);
  }

  @Test
  void testDeleteByProcInstId() {
    var procInstId = "id";
    var formDataPrefix = formDataKeyProvider.getKeyPrefixByProcessInstanceId(procInstId);
    var formDataKey = formDataKeyProvider.generateKey(procInstId, "taskDefId");
    var systemSignPrefix = formDataKeyProvider.getSystemSignatureKeyPrefix(procInstId);
    var systemSignKey = formDataKeyProvider.generateSystemSignatureKey(procInstId, procInstId);

    when(cephService.getKeys(bucketName, formDataPrefix)).thenReturn(Set.of(formDataKey));
    when(cephService.getKeys(bucketName, systemSignPrefix)).thenReturn(Set.of(systemSignKey));

    storageService.deleteByProcessInstance(procInstId);

    verify(cephService).delete(bucketName, Set.of(formDataKey, systemSignKey));
  }

  @Test
  @SneakyThrows
  void testGetFromDataWithStorageKey() {
    var taskDefKey = "taskKey";
    var procInstId = "id";
    var key = formDataKeyProvider.generateKey(taskDefKey, procInstId);
    var formDataAsStr = new String(Objects.requireNonNull(
            CephFormDataStorageServiceTest.class.getResourceAsStream("/json/testFormData.json"))
        .readAllBytes());

    when(cephService.getAsString(bucketName, key)).thenReturn(Optional.of(formDataAsStr));

    var result = storageService.getFormDataWithKey(taskDefKey, procInstId);

    assertThat(result).isPresent();
    assertThat(result.get().getFormData()).isNotNull();
    assertThat(result.get().getStorageKey()).isEqualTo(key);
    assertThat(result.get().getFormData().getSignature()).isEqualTo("signature");
    assertThat(result.get().getFormData().getAccessToken()).isEqualTo("token");
    assertThat(result.get().getFormData().getData().get("testField")).isEqualTo("testValue");
  }

  @Test
  void shouldThrowFormDataRepositoryCommunicationException() {
    var procInstId = "id";
    var prefix = formDataKeyProvider.getKeyPrefixByProcessInstanceId(procInstId);

    when(cephService.getKeys(bucketName, prefix)).thenThrow(CephCommunicationException.class);

    assertThrows(
        FormDataRepositoryCommunicationException.class,
        () -> storageService.deleteByProcessInstance(procInstId));
  }

  @Test
  void shouldThrowFormDataRepositoryMisconfigurationException() {
    var procInstId = "id";
    var prefix = formDataKeyProvider.getKeyPrefixByProcessInstanceId(procInstId);

    when(cephService.getKeys(bucketName, prefix)).thenThrow(MisconfigurationException.class);

    assertThrows(
        FormDataRepositoryMisconfigurationException.class,
        () -> storageService.deleteByProcessInstance(procInstId));
  }

  @Test
  void shouldDelete() {
    var keys = Set.of(formDataKeyProvider.generateSystemSignatureKey("procInstId", "procInstId"));

    storageService.delete(keys);

    verify(cephService).delete(bucketName, keys);
  }
}