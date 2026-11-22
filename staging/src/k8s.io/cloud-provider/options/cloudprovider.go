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

package options

import (
	"github.com/spf13/pflag"
	cpconfig "k8s.io/cloud-provider/config"
)

// CloudProviderOptions holds the cloudprovider options.
//
// 中文说明：内嵌内部配置类型，命令行解析结果直接写入其字段，再由 ApplyTo
// 复制到组件配置中。
type CloudProviderOptions struct {
	*cpconfig.CloudProviderConfiguration
}

// Validate checks validation of cloudprovider options.
//
// 中文说明：目前无需校验——云厂商名不存在时由 plugins 加载阶段报错，
// 配置文件路径的合法性也留到实际读取时判断。
func (s *CloudProviderOptions) Validate() []error {
	allErrors := []error{}
	return allErrors
}

// AddFlags adds flags related to cloudprovider for controller manager to the specified FlagSet.
func (s *CloudProviderOptions) AddFlags(fs *pflag.FlagSet) {
	fs.StringVar(&s.Name, "cloud-provider", s.Name,
		"The provider for cloud services. Empty string for no provider.")

	fs.StringVar(&s.CloudConfigFile, "cloud-config", s.CloudConfigFile,
		"The path to the cloud provider configuration file. Empty string for no configuration file.")
}

// ApplyTo fills up cloudprovider config with options.
func (s *CloudProviderOptions) ApplyTo(cfg *cpconfig.CloudProviderConfiguration) error {
	// nil receiver 表示该配置段未启用，静默跳过而不是 panic。
	if s == nil {
		return nil
	}

	cfg.Name = s.Name
	cfg.CloudConfigFile = s.CloudConfigFile

	return nil
}
