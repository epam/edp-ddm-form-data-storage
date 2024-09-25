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

import com.epam.digital.data.platform.storage.form.model.CephKeysSearchParams;
import com.google.common.collect.Sets;
import lombok.experimental.SuperBuilder;

import java.util.Set;

@SuperBuilder
public class CephFormDataStorageService extends FormDataStorageService<CephKeysSearchParams> {

  @Override
  protected Set<String> findKeysByProcessInstanceId(String processInstanceId) {
    var taskFormDataPrefix = keyProvider.getKeyPrefixByProcessInstanceId(processInstanceId);
    var taskFormDataKeys = repository.getKeysBySearchParams(CephKeysSearchParams.builder().prefix(taskFormDataPrefix).build());
    var systemSignaturePrefix = keyProvider.getSystemSignatureKeyPrefix(processInstanceId);
    var systemSignatureKeys = repository.getKeysBySearchParams(CephKeysSearchParams.builder().prefix(systemSignaturePrefix).build());
    return Sets.union(taskFormDataKeys, systemSignatureKeys);
  }
}
