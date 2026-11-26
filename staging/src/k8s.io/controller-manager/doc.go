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

// 本包提供构建 controller-manager（控制器管理器）所需的公共库代码。
// 它最初是从 kube-controller-manager 与 cloud-controller-manager 之间的通用代码中抽取出来的，
// 目的是让这两类组件以及第三方控制器管理器都能复用同一套基础设施，主要包括：
//   - 通用配置类型与命令行参数（config、options 包）；
//   - 控制器客户端构建器（pkg/clientbuilder），负责为各控制器创建带独立身份的客户端；
//   - Informer 工厂（pkg/informerfactory），统一处理结构化资源与“仅元数据”资源；
//   - 可动态追加健康检查项的 /healthz 框架（pkg/healthz）；
//   - 调试、指标等 HTTP 处理器（app/serve.go）；
//   - 领导者迁移（pkg/leadermigration），用于把控制器在 KCM 与 CCM 之间平滑迁移。
//
// 本包本身不实现任何具体控制器，只提供可组合的构件。
package controllermanager
