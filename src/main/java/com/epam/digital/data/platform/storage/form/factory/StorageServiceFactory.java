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

package com.epam.digital.data.platform.storage.form.factory;

import com.epam.digital.data.platform.integration.ceph.factory.CephS3Factory;
import com.epam.digital.data.platform.integration.ceph.service.CephService;
import com.epam.digital.data.platform.storage.form.config.CephStorageConfiguration;
import com.epam.digital.data.platform.storage.form.repository.CephFormDataRepository;
import com.epam.digital.data.platform.storage.form.repository.FormDataRepository;
import com.epam.digital.data.platform.storage.form.service.FormDataKeyProviderImpl;
import com.epam.digital.data.platform.storage.form.service.FormDataStorageService;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;

/**
 * The class for creation storage services based on supported configuration
 */
@RequiredArgsConstructor
public class StorageServiceFactory {

  private final ObjectMapper objectMapper;
  private final CephS3Factory cephFactory;

  public FormDataStorageService formDataStorageService(CephStorageConfiguration config) {
    return FormDataStorageService.builder()
        .repository(newCephFormDataRepository(config))
        .keyProvider(newFormDataKeyProvider())
        .build();
  }

  private FormDataKeyProviderImpl newFormDataKeyProvider() {
    return new FormDataKeyProviderImpl();
  }

  private FormDataRepository newCephFormDataRepository(CephStorageConfiguration config) {
    return CephFormDataRepository.builder()
        .cephBucketName(config.getBucket())
        .cephService(newCephServiceS3(config))
        .objectMapper(objectMapper)
        .build();
  }

  private CephService newCephServiceS3(CephStorageConfiguration config) {
    return cephFactory.createCephService(config.getHttpEndpoint(),
        config.getAccessKey(), config.getSecretKey());
  }
}
