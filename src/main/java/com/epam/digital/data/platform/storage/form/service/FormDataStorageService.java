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

import com.epam.digital.data.platform.storage.form.dto.FormDataDto;
import com.epam.digital.data.platform.storage.form.dto.FormDataWrapperDto;
import com.epam.digital.data.platform.storage.form.repository.FormDataRepository;
import java.util.Collections;
import java.util.Optional;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

/**
 * The service for managing form data
 */
@Slf4j
@Builder
public class FormDataStorageService {

  private final FormDataRepository repository;
  private final FormDataKeyProvider keyProvider;

  /**
   * Get from data from storage by task definition key and process instance id
   *
   * @param taskDefinitionKey specified task key
   * @param processInstanceId specified process instance id
   * @return {@link FormDataDto} content representation (optional)
   */
  public Optional<FormDataDto> getFormData(String taskDefinitionKey, String processInstanceId) {
    log.info("Get from data by task definition key {} and process instance id {}",
        taskDefinitionKey, processInstanceId);
    var key = keyProvider.generateKey(taskDefinitionKey, processInstanceId);
    return this.getFormData(key);
  }

  /**
   * Get form data with storage key by task definition id and process instance id.
   *
   * @param taskDefinitionKey specified task definition id
   * @param processInstanceId specified process instance id
   * @return {@link FormDataWrapperDto} content with storage key representation (optional)
   */
  public Optional<FormDataWrapperDto> getFormDataWithKey(String taskDefinitionKey,
      String processInstanceId) {
    log.info("Get from data by task definition key {} and process instance id {}",
        taskDefinitionKey, processInstanceId);
    var key = keyProvider.generateKey(taskDefinitionKey, processInstanceId);
    return this.getFormDataWithKey(key);
  }

  /**
   * Get form data with storage key by provided key.
   *
   * @param key specified storage key
   * @return {@link FormDataWrapperDto} content with storage key representation (optional)
   */
  public Optional<FormDataWrapperDto> getFormDataWithKey(String key) {
    var formData = this.getFormData(key);
    return formData.map(fd -> FormDataWrapperDto.builder().storageKey(key).formData(fd).build());
  }

  /**
   * Get from data from storage by key
   *
   * @param key specified form data key
   * @return {@link FormDataDto} content representation (optional)
   */
  public Optional<FormDataDto> getFormData(String key) {
    log.info("Get form data by key {}", key);
    var result = repository.getFormData(key);
    log.info("Form data was found by key {}", key);
    return result;
  }

  /**
   * Put form data to storage with key generation based on specified task definition id and process
   * instance id
   *
   * @param taskDefinitionKey specified task key
   * @param processInstanceId specified process instance id
   */
  public void putFormData(String taskDefinitionKey, String processInstanceId, FormDataDto content) {
    log.info("Put form data by task definition key {}, process instance id {}", taskDefinitionKey,
        processInstanceId);
    var key = keyProvider.generateKey(taskDefinitionKey, processInstanceId);
    this.putFormData(key, content);
  }

  /**
   * Put start form data with generated key based on process definition key and uuid then return
   * generated key
   *
   * @param processDefinitionKey specified process definition key
   * @param uuid                 specified uuid
   * @param content              to put
   * @return generated content key
   */
  public String putStartFormData(String processDefinitionKey, String uuid, FormDataDto content) {
    log.info("Put start form by process definition key {}, uuid {}", processDefinitionKey, uuid);
    var key = keyProvider.generateStartFormKey(processDefinitionKey, uuid);
    this.putFormData(key, content);
    return key;
  }

  /**
   * Put start form data passed by external system with specific generated key based on process
   * definition key and uuid then return generated key
   *
   * @param processDefinitionKey specified process definition key
   * @param uuid                 specified uuid
   * @param content              to put
   * @return generated content key
   */
  public String putExternalSystemFormData(String processDefinitionKey, String uuid,
      FormDataDto content) {
    log.info("Put external system form data by process definition key {}, uuid {}",
        processDefinitionKey, uuid);
    var key = keyProvider.generateKeyForExternalSystem(processDefinitionKey, uuid);
    this.putFormData(key, content);
    return key;
  }

  /**
   * Put form data to storage
   *
   * @param key     document id
   * @param content {@link FormDataDto} content representation
   */
  public void putFormData(String key, FormDataDto content) {
    log.info("Put form data by key {}", key);
    repository.putFormData(key, content);
    log.info("Form data was put to storage by key {}", key);
  }

  /**
   * Delete all forms attached to provided process instance id and specified additional keys
   *
   * @param processInstanceId      specified process instance id
   * @param additionalKeysToDelete additional keys to delete
   */
  public void deleteByProcessInstanceId(String processInstanceId,
      String... additionalKeysToDelete) {
    log.info("Delete form data by process instance id {}", processInstanceId);
    var prefix = keyProvider.getKeyPrefixByProcessInstanceId(processInstanceId);
    var keys = repository.getKeys(prefix);
    Collections.addAll(keys, additionalKeysToDelete);
    if (!keys.isEmpty()) {
      repository.delete(keys);
      log.debug("Deleted next forms data from storage - {}, processInstanceId={}", keys,
          processInstanceId);
    }
  }

  /**
   * Delete all system signatures associated with provided root process instance id
   *
   * @param processInstanceId specified root process instance id
   */
  public void deleteSystemSignaturesByRootProcessInstanceId(String processInstanceId) {
    log.info("Deleting system signatures by root process instance id {}", processInstanceId);
    var prefix = keyProvider.getSystemSignatureKeyPrefix(processInstanceId);
    var keys = repository.getKeys(prefix);
    if (!keys.isEmpty()) {
      repository.delete(keys);
      log.debug("Deleted next system signatures from storage - {}, processInstanceId={}", keys,
          processInstanceId);
    }
  }
}
