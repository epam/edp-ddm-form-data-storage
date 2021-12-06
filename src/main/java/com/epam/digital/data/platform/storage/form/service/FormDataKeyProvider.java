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

/**
 * The class represents a provider that is used to generate the key to get or store form data.
 */
public interface FormDataKeyProvider {

  /**
   * Method for generating the key, uses task definition key and process instance identifier to
   * construct the key
   *
   * @param taskDefinitionKey task definition key
   * @param processInstanceId process instance identifier
   * @return generated key
   */
  String generateKey(String taskDefinitionKey, String processInstanceId);

  /**
   * Method for generating the key to save start form data, uses process definition key and
   * generated unique identifier to construct the key
   *
   * @param processDefinitionKey process definition key
   * @param uuid                 unique identifier
   * @return generated key
   */
  String generateStartFormKey(String processDefinitionKey, String uuid);

  /**
   * Method for generating the key to save input parameters passed by external system that started
   * business process, uses process definition key and generated unique identifier to construct the
   * key
   *
   * @param processDefinitionKey process definition key
   * @param uuid                 unique identifier
   * @return generated key
   */
  String generateKeyForExternalSystem(String processDefinitionKey, String uuid);

  /**
   * Get key prefix with process instance id
   *
   * @param processInstanceId specified process instance id
   * @return generated prefix
   */
  String getKeyPrefixByProcessInstanceId(String processInstanceId);

  /**
   * Get system signature prefix key by root process instance id
   *
   * @param processInstanceId specified process instance id
   * @return generated prefix
   */
  String getSystemSignatureKeyPrefix(String processInstanceId);

  /**
   * Generate system signature storage key
   *
   * @param rootProcessInstanceId specified root process instance id
   * @param processInstanceId     specified process instance id
   * @return generated key
   */
  String generateSystemSignatureKey(String rootProcessInstanceId, String processInstanceId);

  /**
   * Generate system signature key for one element from batch
   *
   * @param processInstanceId specified process instance id
   * @param elemIndex         element index in the batch
   * @return generated key
   */
  String generateBatchSystemSignatureKey(String processInstanceId, Integer elemIndex);
}
