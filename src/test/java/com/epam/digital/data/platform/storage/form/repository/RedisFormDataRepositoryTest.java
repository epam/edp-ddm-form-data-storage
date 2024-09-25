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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.epam.digital.data.platform.storage.form.dto.FormDataDto;
import com.epam.digital.data.platform.storage.form.dto.FormDataInputWrapperDto;
import com.epam.digital.data.platform.storage.form.exception.FormDataRepositoryCommunicationException;
import com.epam.digital.data.platform.storage.form.model.FormDataRedis;
import com.epam.digital.data.platform.storage.form.model.RedisKeysSearchParams;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.RedisTemplate;

@ExtendWith(MockitoExtension.class)
class RedisFormDataRepositoryTest {

  @Mock
  private FormDataKeyValueRepository repository;
  @Mock
  private RedisTemplate<String, Object> template;
  private FormDataRepository<RedisKeysSearchParams> formDataRepository;

  @BeforeEach
  void init() {
    formDataRepository = RedisFormDataRepository.builder()
        .objectMapper(new ObjectMapper())
        .template(template)
        .repository(repository)
        .build();
  }

  @Test
  void testGetFromData() {
    var key = "key";
    var formData = FormDataRedis.builder()
        .data("{\"testField\":\"testValue\"}")
        .accessToken("token")
        .signature("signature")
        .id(key).build();

    when(repository.findById(key)).thenReturn(Optional.of(formData));

    var result = formDataRepository.getFormData(key);
    assertThat(result).isPresent();
    assertThat(result.get().getSignature()).isEqualTo("signature");
    assertThat(result.get().getAccessToken()).isEqualTo("token");
    assertThat(result.get().getData().get("testField")).isEqualTo("testValue");
  }

  @Test
  void testPutFormData() {
    var key = "key";
    var formDataDto = FormDataDto.builder()
        .data(new LinkedHashMap<>(Map.of("testField", "testValue")))
        .build();
    var formDataInputWrapperDto = FormDataInputWrapperDto.builder()
          .key(key)
          .formData(formDataDto)
          .build();
    var formData = FormDataRedis.builder()
        .data("{\"testField\":\"testValue\"}")
        .id(key).build();

    formDataRepository.putFormData(formDataInputWrapperDto);
    verify(repository).save(formData);
  }

  @Test
  void testDelete() {
    var key = "bpm-form-submissions:key";

    formDataRepository.delete(Set.of(key));

    verify(template).delete(Set.of(key));
  }
  @Test
  void shouldThrowRepositoryCommunicationException() {
    var keys = Set.of("bpm-form-submissions:key1", "bpm-form-submissions:key2");
    when(template.delete(keys)).thenThrow(new RuntimeException("exception message"));

    var ex = assertThrows(FormDataRepositoryCommunicationException.class,
        () -> formDataRepository.delete(keys));

    assertThat(ex.getMessage()).isEqualTo("exception message");
  }

  @Test
  void testGetFromDataWithNullFields() {
    var key = "key";
    var formData = FormDataRedis.builder()
        .signature("signature")
        .id(key).build();

    when(repository.findById(key)).thenReturn(Optional.of(formData));

    var result = formDataRepository.getFormData(key);
    assertThat(result).isPresent();
    assertThat(result.get().getSignature()).isEqualTo("signature");
  }
}