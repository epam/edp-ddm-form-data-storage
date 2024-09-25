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

package com.epam.digital.data.platform.storage.form.repository;

import com.epam.digital.data.platform.integration.ceph.service.CephService;
import com.epam.digital.data.platform.storage.form.dto.FormDataDto;
import com.epam.digital.data.platform.storage.form.dto.FormDataInputWrapperDto;
import com.epam.digital.data.platform.storage.form.model.CephKeysSearchParams;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Optional;
import java.util.Set;

@Slf4j
@Builder
public class CephFormDataRepository extends BaseCephRepository implements FormDataRepository<CephKeysSearchParams> {

  private final String cephBucketName;
  private final CephService cephService;
  private final ObjectMapper objectMapper;

  @Override
  public Optional<FormDataDto> getFormData(String key) {
    return execute(
        () -> cephService.getAsString(cephBucketName, key).map(this::deserializeFormData));
  }

  @Override
  public void putFormData(FormDataInputWrapperDto formDataInputWrapperDto) {
    execute(
        () ->
            cephService.put(
                cephBucketName,
                formDataInputWrapperDto.getKey(),
                serializeFormData(formDataInputWrapperDto.getFormData())));
  }

  @Override
  public Set<String> getKeysBySearchParams(CephKeysSearchParams cephKeysSearchParams) {
    return execute(() -> cephService.getKeys(cephBucketName, cephKeysSearchParams.getPrefix()));
  }

  @Override
  public void delete(Set<String> keys) {
    execute(() -> cephService.delete(cephBucketName, keys));
  }

  private FormDataDto deserializeFormData(String formData) {
    try {
      return objectMapper.readValue(formData, FormDataDto.class);
    } catch (IOException e) {
      throw new IllegalArgumentException("Couldn't deserialize form data", e);
    }
  }

  private String serializeFormData(FormDataDto formData) {
    try {
      return objectMapper.writeValueAsString(formData);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Couldn't serialize form data", e);
    }
  }
}
