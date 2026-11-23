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
	DefaultPort           = 8443
	DefaultInsecurePort   = 8080
	DefaultClientQPSLimit = 20
	DefaultClientQPSBurst = 50
)

// Options has all the params needed to run a PodSecurity webhook.
// 本结构体聚合 webhook 专属参数与 apiserver 的安全监听配置，便于统一注册到命令行。
type Options struct {
	// Kubeconfig is the file path to the KubeConfig file to use. Only for out-of-cluster configuration.
	Kubeconfig string

	// Config is the file path to the PodSecurity configuration file.
	Config string

	ClientQPSLimit float32
	ClientQPSBurst int

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

// Validate validates all the required options.
// 本 webhook 没有额外的必填参数，校验集中在安全监听配置上。
func (o *Options) Validate() []error {
	var errs []error

	errs = append(errs, o.SecureServing.Validate()...)

	return errs
}
