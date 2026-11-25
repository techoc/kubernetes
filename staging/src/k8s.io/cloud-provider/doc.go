/*
Copyright 2014 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package cloudprovider 提供云服务提供商的接口与实现。
//
// 本包只定义云提供商需要实现的接口（Instances/InstancesV2、LoadBalancer、
// Routes、Zones 等）及其返回类型，不包含任何具体云厂商逻辑。控制器通过可选接口
// 断言（如 cloud.Instances() 返回的 ok）来判断某项能力是否可用，未实现时调用方
// 应降级而不是报错；需要表达“能力未实现”时返回 NotImplemented 错误。
package cloudprovider
