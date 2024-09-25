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
import com.epam.digital.data.platform.storage.form.dto.FormDataInputWrapperDto;
import com.epam.digital.data.platform.storage.form.dto.FormDataWrapperDto;
import com.epam.digital.data.platform.storage.form.repository.FormDataRepository;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

/**
 * The service for managing form data
 */
@Slf4j
@SuperBuilder
public abstract class FormDataStorageService<T> {

  protected final FormDataRepository<T> repository;
  protected final FormDataKeyProvider keyProvider;

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
    return repository.getFormData(key);
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
    var formDataInputWrapperDto = FormDataInputWrapperDto.builder().key(key).formData(content)
        .processInstanceId(processInstanceId).build();
    this.putFormData(formDataInputWrapperDto);
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
    var formDataInputWrapperDto = FormDataInputWrapperDto.builder().key(key).formData(content).build();
    this.putFormData(formDataInputWrapperDto);
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
    var formDataInputWrapperDto =
        FormDataInputWrapperDto.builder().key(key).formData(content).build();
    this.putFormData(formDataInputWrapperDto);
    return key;
  }

  /**
   * Put form data to storage
   *
   * @param formDataInputWrapperDto {@link FormDataInputWrapperDto}    form data to be put into a storage and metadata
   */
  public void putFormData(FormDataInputWrapperDto formDataInputWrapperDto) {
    log.info("Put form data by key {}", formDataInputWrapperDto.getKey());
    repository.putFormData(formDataInputWrapperDto);
    log.info("Form data was put to storage by key {}", formDataInputWrapperDto.getKey());
  }

  /**
   * Delete all forms and system signatures attached to provided process instance id and specified
   * additional keys
   *
   * @param processInstanceId specified process instance id
   * @param additionalKeysToDelete additional keys to delete
   */
  public void deleteByProcessInstance(
      String processInstanceId, String... additionalKeysToDelete) {
    log.info("Delete form data and system signatures by process instance id {}", processInstanceId);
    var keysByProcessInstanceId = findKeysByProcessInstanceId(processInstanceId);
    var keysToDelete = new HashSet<>(keysByProcessInstanceId);
    Collections.addAll(keysToDelete, additionalKeysToDelete);
    if (!keysToDelete.isEmpty()) {
      repository.delete(keysToDelete);
      log.debug(
          "Deleted next keys from storage - {}, processInstanceId={}", keysToDelete, processInstanceId);
    }
  }

  protected abstract Set<String> findKeysByProcessInstanceId(String processInstanceId);

  /**
   * Delete data from storage by keys
   *
   * @param keys specified keys
   */
  public void delete(Set<String> keys) {
    log.info("Deleting data by keys {}", keys);
    repository.delete(keys);
    log.info("Deleting is finished");
  }
}
