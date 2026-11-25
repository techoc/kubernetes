/*
Copyright The Kubernetes Authors.

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

// client-go 中的 ktesting 包为 ktesting 增加了使用 client-go 进行测试的支持：
// - 存储和获取客户端实例及 REST 配置。
// - TODO: 创建命名空间。
//
// 当然，这些内容也可以作为单独的参数传递。
// 使用 TContext 时，一个参数就足够了。
//
// TODO: 移至 k8s.io/client-go/ktesting
package ktesting
