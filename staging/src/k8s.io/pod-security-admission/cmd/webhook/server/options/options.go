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

package options

import (
	"github.com/spf13/pflag"

	apiserveroptions "k8s.io/apiserver/pkg/server/options"
)

// 默认值集中定义，避免命令行标志与代码初始化两处写死不同数字。
const (
	// DefaultPort 是 webhook 默认的安全监听端口。
	DefaultPort = 8443
	// DefaultInsecurePort 是非安全监听的默认端口。
	DefaultInsecurePort = 8080
	// DefaultClientQPSLimit 是访问 API server 的默认每秒查询数上限。
	DefaultClientQPSLimit = 20
	// DefaultClientQPSBurst 是访问 API server 的默认突发请求数上限。
	DefaultClientQPSBurst = 50
)

// Options 聚合运行 PodSecurity webhook 所需的参数。
// 本结构体同时包含 webhook 专属参数与 apiserver 的安全监听配置，便于统一注册到命令行。
type Options struct {
	// Kubeconfig 是集群外运行时使用的 KubeConfig 文件路径；留空则使用集群内配置。
	Kubeconfig string

	// Config 是 PodSecurity 配置文件的路径。
	Config string

	// ClientQPSLimit 限制访问 API server 的每秒查询数。
	ClientQPSLimit float32
	// ClientQPSBurst 限制访问 API server 的突发请求数。
	ClientQPSBurst int

	// SecureServing 提供安全监听、证书与 TLS 配置。
	SecureServing apiserveroptions.SecureServingOptions
}

// NewOptions 设定 webhook 的默认端口与客户端限流阈值；证书名固定为 webhook，
// 便于部署时按统一名称挂载服务端证书。
func NewOptions() *Options {
	secureServing := apiserveroptions.NewSecureServingOptions()
	secureServing.ServerCert.PairName = "webhook"
	o := &Options{
		SecureServing:  *secureServing,
		ClientQPSLimit: DefaultClientQPSLimit,
		ClientQPSBurst: DefaultClientQPSBurst,
	}
	o.SecureServing.BindPort = DefaultPort
	return o
}

// AddFlags 把配置与限流参数挂到命令行，安全监听相关标志复用 apiserver 标准实现，
// 保持与其他组件的标志命名和行为一致。
func (o *Options) AddFlags(fs *pflag.FlagSet) {
	fs.StringVar(&o.Kubeconfig, "kubeconfig", o.Kubeconfig, "Path to the kubeconfig file specifying how to connect to the API server. Leave empty to use an in-cluster config.")
	fs.StringVar(&o.Config, "config", o.Config, "The path to the PodSecurity configuration file.")
	fs.Float32Var(&o.ClientQPSLimit, "client-qps-limit", o.ClientQPSLimit, "Client QPS limit for throttling requests to the API server.")
	fs.IntVar(&o.ClientQPSBurst, "client-qps-burst", o.ClientQPSBurst, "Client QPS burst limit for throttling requests to the API server.")

	o.SecureServing.AddFlags(fs)
}

// Validate 校验所有必填选项。
// 本 webhook 没有额外的必填参数，校验集中在安全监听配置上。
func (o *Options) Validate() []error {
	var errs []error

	errs = append(errs, o.SecureServing.Validate()...)

	return errs
}
