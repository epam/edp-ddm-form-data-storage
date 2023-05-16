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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import redis.embedded.RedisServer;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EmbeddedRedisFormDataRepositoryTest {

  private static RedisServer redisServer;
  private static RedisTemplate<String, Object> redisTemplate;
  private static FormDataRepository formDataRepository;

  @BeforeAll
  public static void setUp() {
    redisServer = new RedisServer(1499);
    redisServer.start();

    var cf = new LettuceConnectionFactory("localhost", 1499);
    cf.afterPropertiesSet();
    redisTemplate = new RedisTemplate<>();
    redisTemplate.setConnectionFactory(cf);
    redisTemplate.setKeySerializer(new StringRedisSerializer());
    redisTemplate.afterPropertiesSet();

    formDataRepository = RedisFormDataRepository.builder()
        .template(redisTemplate)
        .build();
  }

  @AfterAll
  public static void tearDown() {
    redisServer.stop();
  }

  @Test
  public void testGetKeysMethodByPrefix() {
    redisTemplate.opsForValue().set("bpm-form-submissions:process/1/1", "foo");
    redisTemplate.opsForValue().set("bpm-form-submissions:process/1/2", "foo");
    redisTemplate.opsForValue().set("bpm-form-submissions:process/2/1", "foo");

    var res = formDataRepository.getKeys("process/1/");

    assertEquals(res.size(), 2);
    assertTrue(res.containsAll(Set.of("bpm-form-submissions:process/1/1", "bpm-form-submissions:process/1/2")));
  }

}
