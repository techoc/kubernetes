/*
Copyright 2021 The Kubernetes Authors.

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

// Package kubelet 是 kubelet 对外发布的 Go 包集合的根包，本身不放实现。
// 真正可被引用的内容包括：config/v1、v1alpha1、v1beta1 下的 kubelet 配置 API，
// 以及 pkg/apis 下的 credentialprovider、deviceplugin、dra、pluginregistration、podresources、stats 等插件与状态 API。
// 把配置与插件 API 放在独立的 staging 仓库，是为了让外部组件无需依赖 k8s.io/kubernetes 主模块即可引用。
package kubelet
