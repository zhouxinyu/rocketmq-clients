/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <cstdint>
#include <string>

#include "absl/container/flat_hash_map.h"
#include "google/protobuf/struct.pb.h"
#include "google/protobuf/util/json_util.h"

#include "rocketmq/RocketMQ.h"

ROCKETMQ_NAMESPACE_BEGIN

struct BrokerData {
  std::string cluster_;
  std::string broker_name_;
  absl::flat_hash_map<std::int64_t, std::string> broker_addresses_;

  static BrokerData decode(const google::protobuf::Struct& root);
};

ROCKETMQ_NAMESPACE_END