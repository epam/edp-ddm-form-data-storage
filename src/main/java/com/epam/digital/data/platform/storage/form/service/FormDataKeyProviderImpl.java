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

public class FormDataKeyProviderImpl implements FormDataKeyProvider {

  public static final String TASK_FORM_DATA_KEY_FORMAT = "process/%s/task/%s";
  public static final String START_FORM_DATA_KEY_FORMAT = "process-definition/%s/start-form/%s";
  public static final String START_FORM_DATA_STRING_FORMAT = "start_form_%s";
  public static final String START_FORM_DATA_VALUE_FORMAT = "lowcode_%s_%s";
  public static final String TASK_FORM_DATA_PREFIX_FORMAT = "process/%s/";
  public static final String SYSTEM_SIGNATURE_STORAGE_KEY = "lowcode_%s_%s_system_signature_ceph_key";
  public static final String BATCH_SYSTEM_SIGNATURE_STORAGE_KEY = "lowcode_%s_system_signature_ceph_key_%s";
  public static final String SYSTEM_SIGNATURE_STORAGE_KEY_PREFIX = "lowcode_%s";

  @Override
  public String generateKey(String taskDefinitionKey, String processInstanceId) {
    return String.format(TASK_FORM_DATA_KEY_FORMAT, processInstanceId, taskDefinitionKey);
  }

  @Override
  public String generateStartFormKey(String processDefinitionKey, String uuid) {
    return String.format(START_FORM_DATA_KEY_FORMAT, processDefinitionKey, uuid);
  }

  @Override
  public String generateKeyForExternalSystem(String processDefinitionKey, String uuid) {
    var startFormDataVariableName = String.format(START_FORM_DATA_STRING_FORMAT, uuid);
    return String.format(START_FORM_DATA_VALUE_FORMAT, processDefinitionKey,
        startFormDataVariableName);
  }

  @Override
  public String getKeyPrefixByProcessInstanceId(String processInstanceId) {
    return String.format(TASK_FORM_DATA_PREFIX_FORMAT, processInstanceId);
  }

  @Override
  public String getSystemSignatureKeyPrefix(String processInstanceId) {
    return String.format(SYSTEM_SIGNATURE_STORAGE_KEY_PREFIX, processInstanceId);
  }

  @Override
  public String generateSystemSignatureKey(String rootProcessInstanceId, String processInstanceId) {
    return String.format(SYSTEM_SIGNATURE_STORAGE_KEY, rootProcessInstanceId, processInstanceId);
  }

  @Override
  public String generateBatchSystemSignatureKey(String processInstanceId, Integer elemIndex) {
    return String.format(BATCH_SYSTEM_SIGNATURE_STORAGE_KEY, processInstanceId, elemIndex);
  }
}
