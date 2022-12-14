/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.geekbang.demo.backend.services;

import com.geekbang.demo.backend.model.Alert;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class KafkaAlertsPusher implements Consumer<Alert> {

  private KafkaTemplate<String, Object> kafkaTemplate;

  @Value("${kafka.topic.alerts}")
  private String topic;

  @Autowired
  public KafkaAlertsPusher(KafkaTemplate<String, Object> kafkaTemplateForJson) {
    this.kafkaTemplate = kafkaTemplateForJson;
  }

  @Override
  public void accept(Alert alert) {
    log.info("{}", alert);
    kafkaTemplate.send(topic, alert);
  }
}
