/*
Copyright 2020 The Kubernetes Authors.

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

package genericclioptions

import (
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

var (
	// ErrEmptyConfig 是配置缺失或不完整时展示的错误信息。
	// ErrEmptyConfig 是配置缺失或不完整时展示给用户的错误信息。
	// 这里在 client-go 的通用错误上补充了可操作的三条查找路径，避免命令行用户
	// 只看到“config 为空”却不知道去哪里配。
	ErrEmptyConfig = clientcmd.NewEmptyConfigError(`Missing or incomplete configuration info.  Please point to an existing, complete config file:


  1. Via the command-line flag --kubeconfig
  2. Via the KUBECONFIG environment variable
  3. In your home directory as ~/.kube/config

To view or setup config directly use the 'config' command.`)
)

// 编译期断言：clientConfig 必须实现 clientcmd.ClientConfig 接口。
var _ = clientcmd.ClientConfig(&clientConfig{})

// clientConfig 是对 clientcmd.ClientConfig 的轻量包装，唯一职责是把
// client-go 默认的 ErrEmptyConfig 替换成 CLI 场景下更易读的 ErrEmptyConfig。
type clientConfig struct {
	defaultClientConfig clientcmd.ClientConfig
}

// RawConfig 返回原始的 kubeconfig；若 client-go 报告配置为空，则换成带指引的版本。
func (c *clientConfig) RawConfig() (clientcmdapi.Config, error) {
	config, err := c.defaultClientConfig.RawConfig()
	// 用带操作指引的版本替换 client-go 的空配置错误。
	if clientcmd.IsEmptyConfig(err) {
		return config, ErrEmptyConfig
	}
	return config, err
}

// ClientConfig 构造用于发起请求的 rest.Config，同样屏蔽 client-go 的空配置错误。
func (c *clientConfig) ClientConfig() (*restclient.Config, error) {
	config, err := c.defaultClientConfig.ClientConfig()
	// 用带操作指引的版本替换 client-go 的空配置错误。
	if clientcmd.IsEmptyConfig(err) {
		return config, ErrEmptyConfig
	}
	return config, err
}

// Namespace 解析当前命名空间；bool 表示是否来自显式配置而非默认值。
func (c *clientConfig) Namespace() (string, bool, error) {
	namespace, ok, err := c.defaultClientConfig.Namespace()
	// 用带操作指引的版本替换 client-go 的空配置错误。
	if clientcmd.IsEmptyConfig(err) {
		return namespace, ok, ErrEmptyConfig
	}
	return namespace, ok, err
}

// ConfigAccess 暴露底层的 kubeconfig 读写入口，供 config 子命令修改配置。
func (c *clientConfig) ConfigAccess() clientcmd.ConfigAccess {
	return c.defaultClientConfig.ConfigAccess()
}
