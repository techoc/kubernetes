/*
Copyright 2016 The Kubernetes Authors.

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

// Package core 实现核心 API 组资源的配额评估器（quota evaluators）。
//
// Kubernetes 资源配额（ResourceQuota）系统通过 Evaluator 接口来评估各种资源
// 对配额的占用情况。此包提供了以下核心资源的评估器：
//
//   - PodEvaluator：评估 Pod 的 CPU、内存、存储、Pod 数量等配额
//   - ServiceEvaluator：评估 Service 的数量、NodePort、LoadBalancer 配额
//   - PVCEvaluator：评估 PVC 的数量和存储配额，支持按 StorageClass 细分
//   - ResourceClaimEvaluator：评估 DRA（动态资源分配）的 ResourceClaim 配额
//
// 每个评估器实现 quota.Evaluator 接口，提供以下能力：
//   - Constraints：验证资源是否满足配额要求
//   - Matches：判断资源是否匹配给定的 ResourceQuota
//   - Usage：计算单个资源的配额占用
//   - UsageStats：计算命名空间内所有资源的聚合配额使用统计
//
// Registry（NewEvaluators）是评估器的工厂函数，根据启用的 API 资源创建对应的评估器列表。
package core
