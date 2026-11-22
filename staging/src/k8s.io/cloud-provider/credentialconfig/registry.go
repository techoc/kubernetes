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

// Package credentialconfig 提供最小的镜像仓库凭据文件结构，
// 使 cloud-provider 相关组件无需依赖 kubelet 的内部凭据包即可解析 dockercfg。
package credentialconfig

// Code taken from /pkg/credentialprovider/config.go

// RegistryConfig represents the config file used by the docker CLI.
// This config that represents the credentials that should be used
// when pulling images from specific image repositories.
// 键是镜像仓库地址（通常为 registry host[:port]），值是该仓库的凭据。
type RegistryConfig map[string]RegistryConfigEntry

// RegistryConfigEntry wraps a docker config as a entry
// 字段与旧式 .dockercfg 格式对应；空字段表示该凭据未提供对应信息。
type RegistryConfigEntry struct {
	Username string
	Password string
	Email    string
}
