//go:build windows

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

package v1

const (
	// DefaultCACertPath 定义 Windows 上 CA 证书的默认位置。
	DefaultCACertPath = "C:/etc/kubernetes/pki/ca.crt"
	// DefaultContainerRuntimeURLScheme 定义默认的套接字 URL 前缀。
	DefaultContainerRuntimeURLScheme = "npipe"
)
