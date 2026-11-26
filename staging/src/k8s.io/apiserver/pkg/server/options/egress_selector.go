/*
Copyright 2019 The Kubernetes Authors.

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
	"fmt"

	"github.com/spf13/pflag"
	"k8s.io/utils/path"

	"k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/server/egressselector"
)

// EgressSelectorOptions 保存 api server 的出口选择器选项。
// 参见 https://github.com/kubernetes/enhancements/blob/master/keps/sig-api-machinery/1281-network-proxy/README.md
type EgressSelectorOptions struct {
	// ConfigFile 是 api-server 出口选择器配置文件路径。
	ConfigFile string
}

// NewEgressSelectorOptions 创建一个 EgressSelectorOptions 实例。
//
// 该选项指向 egress/konnectivity 配置文件。
// 它决定哪些类型的请求使用 egress/konnectivity，以及如何使用。
// 为空时 API Server 将尝试直接用网络连通。
func NewEgressSelectorOptions() *EgressSelectorOptions {
	return &EgressSelectorOptions{}
}

// AddFlags 把本 APIServer 的 egress 选择器相关标志注册到指定的 FlagSet。
// 注：原注释中的 "admission" 是历史遗留，实际注册的是出站流量选择器参数。
func (o *EgressSelectorOptions) AddFlags(fs *pflag.FlagSet) {
	if o == nil {
		return
	}

	fs.StringVar(&o.ConfigFile, "egress-selector-config-file", o.ConfigFile,
		"File with apiserver egress selector configuration.")
}

// ApplyTo 把 egress 选择器设置写入 server 配置；
// 若集群管理员未提供 egress 选择器设置，则从推荐/默认/no-op 值准备。
// 读取配置后必须先校验再构造选择器：非法配置在启动期失败，好过运行期所有出站请求失败。
func (o *EgressSelectorOptions) ApplyTo(c *server.Config) error {
	if o == nil {
		return nil
	}

	npConfig, err := egressselector.ReadEgressSelectorConfiguration(o.ConfigFile)
	if err != nil {
		return fmt.Errorf("failed to read egress selector config: %v", err)
	}
	errs := egressselector.ValidateEgressSelectorConfiguration(npConfig)
	if len(errs) > 0 {
		return fmt.Errorf("failed to validate egress selector configuration: %v", errs.ToAggregate())
	}

	cs, err := egressselector.NewEgressSelector(npConfig)
	if err != nil {
		return fmt.Errorf("failed to setup egress selector with config %#v: %v", npConfig, err)
	}
	c.EgressSelector = cs
	return nil
}

// Validate 校验传给 EgressSelectorOptions 的参数；
// 未配置文件时视为使用直连网络，无需进一步校验。
func (o *EgressSelectorOptions) Validate() []error {
	if o == nil || o.ConfigFile == "" {
		return nil
	}

	errs := []error{}

	if exists, err := path.Exists(path.CheckFollowSymlink, o.ConfigFile); !exists || err != nil {
		errs = append(errs, fmt.Errorf("egress-selector-config-file %s does not exist", o.ConfigFile))
	}

	return errs
}
