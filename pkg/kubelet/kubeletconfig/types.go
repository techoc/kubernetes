/*
Copyright 2017 The Kubernetes Authors.

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

package kubeletconfig

// ContainerRuntimeOptions 定义容器运行时的选项。
type ContainerRuntimeOptions struct {
	// 通用选项。

	// RuntimeCgroups 是容器运行时预期隔离到的 cgroup。
	RuntimeCgroups string
	// 镜像凭据提供者插件选项

	// ImageCredentialProviderConfigPath 是凭据提供者插件配置文件或目录的路径。
	// 若指定目录，则按字典序加载并合并目录下所有 .json、.yaml 或 .yml 文件。
	// 配置文件说明 kubelet 启用并调用哪些凭据提供者，插件配置应包含要执行的插件二进制
	// 以及该插件应处理哪些容器镜像。
	// +optional
	ImageCredentialProviderConfigPath string
	// ImageCredentialProviderBinDir 是凭据提供者插件二进制所在目录的路径。
	// 每个插件二进制的名称应与 imageCredentialProviderConfigFile 中指定的插件名称一致。
	// +optional
	ImageCredentialProviderBinDir string
}
