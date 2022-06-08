package com.epam.digital.data.platform.storage.form.config;

import lombok.Data;

@Data
public class SentinelConfiguration {

  private String password;
  private String username;
  private String nodes;
  private String master;
}
