/*
Copyright 2016 The Kubernetes Authors.

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

package server

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	openapinamer "k8s.io/apiserver/pkg/endpoints/openapi"

	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/sets"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/server/filters"
	genericoptions "k8s.io/apiserver/pkg/server/options"
	"k8s.io/kube-aggregator/pkg/apis/apiregistration/v1beta1"
	"k8s.io/kube-aggregator/pkg/apiserver"
	aggregatorscheme "k8s.io/kube-aggregator/pkg/apiserver/scheme"
	"k8s.io/kube-aggregator/pkg/generated/openapi"
)

// 存储前缀单独用 kube-aggregator 的名字，而不是借 apiextensions 之类的前缀，
// 是为了让不同组件在同一个 etcd 里各有各的命名空间，互不踩踏。
const defaultEtcdPathPrefix = "/registry/kube-aggregator.kubernetes.io/"

// AggregatorOptions contains everything necessary to create and run an API Aggregator.
type AggregatorOptions struct {
	ServerRunOptions   *genericoptions.ServerRunOptions
	RecommendedOptions *genericoptions.RecommendedOptions
	APIEnablement      *genericoptions.APIEnablementOptions

	// ProxyClientCert/Key are the client cert used to identify this proxy. Backing APIServices use
	// this to confirm the proxy's identity
	ProxyClientCertFile string
	ProxyClientKeyFile  string

	StdOut io.Writer
	StdErr io.Writer
}

// NewCommandStartAggregator provides a CLI handler for 'start master' command
// with a default AggregatorOptions.
// 这里对 options 做的是值拷贝：同一份默认值可以用于构造多个命令，
// 每个命令解析 flag 时只改自己的那份，不会串到别的命令上。
func NewCommandStartAggregator(ctx context.Context, defaults *AggregatorOptions) *cobra.Command {
	o := *defaults
	cmd := &cobra.Command{
		Short: "Launch a API aggregator and proxy server",
		Long:  "Launch a API aggregator and proxy server",
		// 组件全局变量必须在 RunE 之前设置好，否则后续解析 flag 时
		// 可能读不到 feature gate 之类的开关值。放在 PersistentPreRunE 里
		// 也能覆盖存在子命令的情况。
		PersistentPreRunE: func(*cobra.Command, []string) error {
			return o.ServerRunOptions.ComponentGlobalsRegistry.Set()
		},
		RunE: func(c *cobra.Command, args []string) error {
			if err := o.Complete(); err != nil {
				return err
			}
			if err := o.Validate(args); err != nil {
				return err
			}
			if err := o.RunAggregator(c.Context()); err != nil {
				return err
			}
			return nil
		},
	}
	cmd.SetContext(ctx)

	o.AddFlags(cmd.Flags())
	return cmd
}

// AddFlags is necessary because hyperkube doesn't work using cobra, so we have to have different registration and execution paths
// 把 flag 注册拆成独立方法，是为了让 cobra 路径和 hyperkube 路径共用同一套开关定义；
// 否则两种入口迟早会出现"某个参数只有一边认"的漂移。
func (o *AggregatorOptions) AddFlags(fs *pflag.FlagSet) {
	o.ServerRunOptions.AddUniversalFlags(fs)
	o.RecommendedOptions.AddFlags(fs)
	o.APIEnablement.AddFlags(fs)
	fs.StringVar(&o.ProxyClientCertFile, "proxy-client-cert-file", o.ProxyClientCertFile, "client certificate used identify the proxy to the API server")
	fs.StringVar(&o.ProxyClientKeyFile, "proxy-client-key-file", o.ProxyClientKeyFile, "client certificate key used identify the proxy to the API server")
}

// NewDefaultOptions builds a "normal" set of options.  You wouldn't normally expose this, but hyperkube isn't cobra compatible
func NewDefaultOptions(out, err io.Writer) *AggregatorOptions {
	o := &AggregatorOptions{
		ServerRunOptions: genericoptions.NewServerRunOptions(),
		RecommendedOptions: genericoptions.NewRecommendedOptions(
			defaultEtcdPathPrefix,
			aggregatorscheme.Codecs.LegacyCodec(v1beta1.SchemeGroupVersion),
		),
		APIEnablement: genericoptions.NewAPIEnablementOptions(),

		StdOut: out,
		StdErr: err,
	}

	return o
}

// Validate validates all the required options.
// 把各子配置的错误汇总后再返回，而不是遇到第一个就退出：
// 启动一个 API server 要填的参数很多，逐个报错能省掉反复改配置的循环。
func (o AggregatorOptions) Validate(args []string) error {
	errors := []error{}
	errors = append(errors, o.ServerRunOptions.Validate()...)
	errors = append(errors, o.RecommendedOptions.Validate()...)
	errors = append(errors, o.APIEnablement.Validate(aggregatorscheme.Scheme)...)
	return utilerrors.NewAggregate(errors)
}

// Complete fills in missing Options.
func (o *AggregatorOptions) Complete() error {
	return o.ServerRunOptions.Complete()
}

// RunAggregator runs the API Aggregator.
func (o AggregatorOptions) RunAggregator(ctx context.Context) error {
	// TODO have a "real" external address
	// Aggregator 作为独立进程部署时未必有人配好证书，所以这里先用自签名证书兜底，
	// 保证服务能起来。真正的对外地址要等部署侧确定后再补上。
	if err := o.RecommendedOptions.SecureServing.MaybeDefaultWithSelfSignedCerts("localhost", nil, nil); err != nil {
		return fmt.Errorf("error creating self-signed certificates: %v", err)
	}

	serverConfig := genericapiserver.NewRecommendedConfig(aggregatorscheme.Codecs)

	if err := o.ServerRunOptions.ApplyTo(&serverConfig.Config); err != nil {
		return err
	}
	if err := o.RecommendedOptions.ApplyTo(serverConfig); err != nil {
		return err
	}
	if err := o.APIEnablement.ApplyTo(&serverConfig.Config, apiserver.DefaultAPIResourceConfigSource(), aggregatorscheme.Scheme); err != nil {
		return err
	}
	// watch 天然是长连接，proxy 则可能把请求转到任意后端，两者都不能按普通请求对待；
	// 后面那组动词出现在子路径里时同样要走长连接通道，避免被超时中断。
	serverConfig.LongRunningFunc = filters.BasicLongRunningRequestCheck(
		sets.NewString("watch", "proxy"),
		sets.NewString("attach", "exec", "proxy", "log", "portforward"),
	)
	// 这里配置好 OpenAPI 定义，是为了让聚合器自己的类型能进入文档；
	// 但真正对外提供 /openapi 的不能是通用 server 的默认 handler，
	// 因为聚合器要合并各后端 APIService 的文档，所以把默认安装关掉。
	serverConfig.OpenAPIConfig = genericapiserver.DefaultOpenAPIConfig(openapi.GetOpenAPIDefinitions, openapinamer.NewDefinitionNamer(aggregatorscheme.Scheme))
	serverConfig.OpenAPIConfig.Info.Title = "kube-aggregator"
	// prevent generic API server from installing the OpenAPI handler. Aggregator server
	// has its own customized OpenAPI handler.
	serverConfig.SkipOpenAPIInstallation = true

	serviceResolver := apiserver.NewClusterIPServiceResolver(serverConfig.SharedInformerFactory.Core().V1().Services().Lister())

	config := apiserver.Config{
		GenericConfig: serverConfig,
		ExtraConfig: apiserver.ExtraConfig{
			ServiceResolver: serviceResolver,
		},
	}

	// 后端 APIService 靠这张客户端证书确认请求确实来自聚合器，
	// 缺了它整个代理链路的身份验证就无从谈起，所以必须在启动前硬性要求。
	if len(o.ProxyClientCertFile) == 0 || len(o.ProxyClientKeyFile) == 0 {
		return errors.New("missing a client certificate along with a key to identify the proxy to the API server")
	}

	// 证书信息在这里才真正落到 config 上：前面的校验只保证"填了"，
	// 到这里才把它交给代理层用于向后端证明身份。
	config.ExtraConfig.ProxyClientCertFile = o.ProxyClientCertFile
	config.ExtraConfig.ProxyClientKeyFile = o.ProxyClientKeyFile

	server, err := config.Complete().NewWithDelegate(genericapiserver.NewEmptyDelegate())
	if err != nil {
		return err
	}

	prepared, err := server.PrepareRun()
	if err != nil {
		return err
	}
	return prepared.Run(ctx)
}
