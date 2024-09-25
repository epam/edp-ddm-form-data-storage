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
import org.springframework.cloud.sleuth.annotation.NewSpan;

import java.util.Optional;
import java.util.Set;

/**
 * The repository for getting and storing form data.
 */
public interface FormDataRepository<T> {

  /**
   * Retrieve formData by key
   *
   * @param key document id
   * @return {@link FormDataDto} content representation (optional)
   * @throws IllegalArgumentException if stored content couldn't be parsed to {@link FormDataDto}
   */
  @NewSpan
  Optional<FormDataDto> getFormData(String key);

  /**
   * Put formData to repository
   *
   * @param formDataInputWrapperDto {@link FormDataInputWrapperDto} form data and additional information, required for storing
   */
  @NewSpan
  void putFormData(FormDataInputWrapperDto formDataInputWrapperDto);

  /**
   * Get storage keys by provided search parameters
   *
   * @param searchParams provided search parameters
   * @return set of keys
   */
  @NewSpan("getKeysBySearchParams")
  Set<String> getKeysBySearchParams(T searchParams);

  /**
   * Delete forms by provided keys
   *
   * @param keys specified form keys
   */
  @NewSpan("deleteFormDataByKeys")
  void delete(Set<String> keys);
}
