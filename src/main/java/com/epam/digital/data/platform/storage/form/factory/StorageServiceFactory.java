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
import com.epam.digital.data.platform.storage.form.config.RedisStorageConfiguration;
import com.epam.digital.data.platform.storage.form.repository.CephFormDataRepository;
import com.epam.digital.data.platform.storage.form.repository.FormDataKeyValueRepository;
import com.epam.digital.data.platform.storage.form.repository.FormDataRepository;
import com.epam.digital.data.platform.storage.form.repository.RedisFormDataRepository;
import com.epam.digital.data.platform.storage.form.service.FormDataKeyProviderImpl;
import com.epam.digital.data.platform.storage.form.service.FormDataStorageService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Splitter;
import io.lettuce.core.internal.HostAndPort;
import java.util.stream.Collectors;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisSentinelConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisKeyValueAdapter;
import org.springframework.data.redis.core.RedisKeyValueTemplate;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.mapping.RedisMappingContext;
import org.springframework.data.redis.repository.support.RedisRepositoryFactory;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;

/**
 * The class for creation storage services based on supported configuration
 */
public class StorageServiceFactory {

  private final ObjectMapper objectMapper;
  private CephS3Factory cephFactory;

  public StorageServiceFactory(ObjectMapper objectMapper) {
    this.objectMapper = objectMapper;
  }

  public StorageServiceFactory(ObjectMapper objectMapper,
      CephS3Factory cephFactory) {
    this.objectMapper = objectMapper;
    this.cephFactory = cephFactory;
  }

  public FormDataStorageService formDataStorageService(CephStorageConfiguration config) {
    return FormDataStorageService.builder()
        .repository(newCephFormDataRepository(config))
        .keyProvider(newFormDataKeyProvider())
        .build();
  }

  public FormDataStorageService formDataStorageService(RedisStorageConfiguration config) {
    return FormDataStorageService.builder()
        .repository(newRedisFormDataRepository(config))
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

  private FormDataRepository newRedisFormDataRepository(RedisStorageConfiguration config) {
    var template = newRedisTemplate(config);

    return RedisFormDataRepository.builder()
        .repository(newFormDataKeyValueRepository(template))
        .template(template)
        .objectMapper(objectMapper)
        .build();
  }

  private FormDataKeyValueRepository newFormDataKeyValueRepository(
      RedisTemplate<String, Object> template) {
    RedisKeyValueAdapter keyValueAdapter = new RedisKeyValueAdapter(
        template.opsForHash().getOperations());
    RedisKeyValueTemplate keyValueTemplate = new RedisKeyValueTemplate(keyValueAdapter,
        new RedisMappingContext());

    RepositoryFactorySupport factory = new RedisRepositoryFactory(keyValueTemplate);
    return factory.getRepository(FormDataKeyValueRepository.class);
  }

  private RedisConnectionFactory redisConnectionFactory(RedisStorageConfiguration configuration) {
    var redisSentinelConfig = new RedisSentinelConfiguration();

    redisSentinelConfig.setMaster(configuration.getSentinel().getMaster());
    setSentinelNodes(redisSentinelConfig, configuration);
    redisSentinelConfig.setUsername(configuration.getUsername());
    redisSentinelConfig.setPassword(configuration.getPassword());

    var connectionFactory = new LettuceConnectionFactory(redisSentinelConfig);
    connectionFactory.afterPropertiesSet();
    return connectionFactory;
  }

  private RedisTemplate<String, Object> newRedisTemplate(RedisStorageConfiguration configuration) {
    RedisTemplate<String, Object> redisTemplate = new RedisTemplate<>();
    redisTemplate.setConnectionFactory(redisConnectionFactory(configuration));
    redisTemplate.setKeySerializer(new StringRedisSerializer());
    redisTemplate.afterPropertiesSet();
    return redisTemplate;
  }

  private void setSentinelNodes(RedisSentinelConfiguration sentinelConfiguration,
      RedisStorageConfiguration storageConfiguration) {
    var nodes = Splitter.on(',')
        .trimResults()
        .omitEmptyStrings()
        .splitToList(storageConfiguration.getSentinel().getNodes())
        .stream()
        .map(HostAndPort::parse)
        .collect(Collectors.toList());

    for (HostAndPort node : nodes) {
      sentinelConfiguration.sentinel(node.getHostText(), node.getPort());
    }
  }
}
