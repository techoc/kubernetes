/*
Copyright 2018 The Kubernetes Authors.

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

// Package podtolerationrestriction 实现 Pod 容忍限制插件：先检查 Pod 的容忍与命名空间的
// 容忍是否冲突，冲突则拒绝；否则合并两者，再依据命名空间容忍白名单校验合并结果，通过才准入。
// 命名空间未配置默认容忍或白名单时，退而使用集群级别的配置（如有）。命名空间的容忍通过
// scheduler.alpha.kubernetes.io/defaultTolerations 和
// scheduler.alpha.kubernetes.io/tolerationsWhitelist 注解设置。
package podtolerationrestriction
