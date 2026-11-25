/*
Copyright 2014 The Kubernetes Authors.

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

// Package app 组装 Kubernetes API 服务器所需的 API、控制平面和服务端基础设施。
// 调用方既可直接配置和运行，也可通过 hyperkube 框架接入。
package app

import (
	"context"
	"fmt"
	"net/url"
	"os"

	"github.com/spf13/cobra"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apiserver/pkg/admission"
	genericapifilters "k8s.io/apiserver/pkg/endpoints/filters"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/server/egressselector"
	"k8s.io/apiserver/pkg/server/flagz"
	serverstorage "k8s.io/apiserver/pkg/server/storage"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/apiserver/pkg/util/notfoundhandler"
	"k8s.io/apiserver/pkg/util/proxy"
	"k8s.io/apiserver/pkg/util/webhook"
	clientgoinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	cliflag "k8s.io/component-base/cli/flag"
	"k8s.io/component-base/cli/globalflag"
	basecompatibility "k8s.io/component-base/compatibility"
	"k8s.io/component-base/featuregate"
	"k8s.io/component-base/logs"
	logsapi "k8s.io/component-base/logs/api/v1"
	_ "k8s.io/component-base/metrics/prometheus/workqueue"
	"k8s.io/component-base/term"
	utilversion "k8s.io/component-base/version"
	"k8s.io/component-base/version/verflag"
	"k8s.io/klog/v2"
	aggregatorapiserver "k8s.io/kube-aggregator/pkg/apiserver"
	"k8s.io/kubernetes/cmd/kube-apiserver/app/options"
	"k8s.io/kubernetes/pkg/capabilities"
	"k8s.io/kubernetes/pkg/controlplane"
	controlplaneapiserver "k8s.io/kubernetes/pkg/controlplane/apiserver"
	"k8s.io/kubernetes/pkg/controlplane/reconcilers"
	kubeapiserveradmission "k8s.io/kubernetes/pkg/kubeapiserver/admission"
)

func init() {
	utilruntime.Must(logsapi.AddFeatureGates(utilfeature.DefaultMutableFeatureGate))
}

// NewAPIServerCommand 创建使用默认参数的 kube-apiserver 命令。
func NewAPIServerCommand() *cobra.Command {
	s := options.NewServerRunOptions()
	ctx := genericapiserver.SetupSignalContext()
	featureGate := s.GenericServerRunOptions.ComponentGlobalsRegistry.FeatureGateFor(basecompatibility.DefaultKubeComponent)

	cmd := &cobra.Command{
		Use: "kube-apiserver",
		Long: `The Kubernetes API server validates and configures data
for the api objects which include pods, services, replicationcontrollers, and
others. The API Server services REST operations and provides the frontend to the
cluster's shared state through which all other components interact.`,

		// 命令执行失败时不打印用法，避免错误信息被大段帮助文本淹没。
		SilenceUsage: true,
		PersistentPreRunE: func(*cobra.Command, []string) error {
			if err := s.GenericServerRunOptions.ComponentGlobalsRegistry.Set(); err != nil {
				return err
			}
			// 回环客户端不应记录自身引发的告警。
			rest.SetDefaultWarningHandler(rest.NoWarnings{})
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			verflag.PrintAndExitIfRequested()
			fs := cmd.Flags()
			// 尽早应用日志配置，随后按最终配置打印参数。
			if err := logsapi.ValidateAndApply(s.Logs, featureGate); err != nil {
				return err
			}
			cliflag.PrintFlags(fs)

			// 填充默认值。
			completedOptions, err := s.Complete(ctx)
			if err != nil {
				return err
			}

			// 校验参数。
			if errs := completedOptions.Validate(); len(errs) != 0 {
				return utilerrors.NewAggregate(errs)
			}
			// 注册特性开关指标。
			featureGate.(featuregate.MutableFeatureGate).AddMetrics()
			// 注册组件版本指标。
			s.GenericServerRunOptions.ComponentGlobalsRegistry.AddMetrics()
			return Run(ctx, completedOptions)
		},
		Args: func(cmd *cobra.Command, args []string) error {
			for _, arg := range args {
				if len(arg) > 0 {
					return fmt.Errorf("%q does not take any arguments, got %q", cmd.CommandPath(), args)
				}
			}
			return nil
		},
	}
	cmd.SetContext(ctx)

	fs := cmd.Flags()
	namedFlagSets := s.Flags()
	s.Flagz = flagz.NamedFlagSetsReader{
		FlagSets: namedFlagSets,
	}
	verflag.AddFlags(namedFlagSets.FlagSet("global"))
	globalflag.AddGlobalFlags(namedFlagSets.FlagSet("global"), cmd.Name(), logs.SkipLoggingConfigurationFlags())
	for _, f := range namedFlagSets.FlagSets {
		fs.AddFlagSet(f)
	}

	cols, _, _ := term.TerminalSize(cmd.OutOrStdout())
	cliflag.SetUsageAndHelpFunc(cmd, namedFlagSets, cols)

	return cmd
}

// Run 运行指定的 API 服务器，正常退出前不会主动返回。
func Run(ctx context.Context, opts options.CompletedOptions) error {
	// 立即记录版本，便于排查问题。
	klog.Infof("Version: %+v", utilversion.Get())

	klog.InfoS("Golang settings", "GOGC", os.Getenv("GOGC"), "GOMAXPROCS", os.Getenv("GOMAXPROCS"), "GOTRACEBACK", os.Getenv("GOTRACEBACK"))

	if opts.InformerName == nil {
		informerName, err := cache.NewInformerName("kube-apiserver")
		if err != nil {
			return err
		}
		opts.InformerName = informerName
	}

	config, err := NewConfig(opts)
	if err != nil {
		return err
	}
	completed, err := config.Complete()
	if err != nil {
		return err
	}
	server, err := CreateServerChain(completed)
	if err != nil {
		return err
	}

	prepared, err := server.PrepareRun()
	if err != nil {
		return err
	}

	return prepared.Run(ctx)
}

// CreateServerChain 创建通过委托机制串联的 API 服务器链。
func CreateServerChain(config CompletedConfig) (*aggregatorapiserver.APIAggregator, error) {
	notFoundHandler := notfoundhandler.New(config.KubeAPIs.ControlPlane.Generic.Serializer, genericapifilters.NoMuxAndDiscoveryIncompleteKey)
	apiExtensionsServer, err := config.ApiExtensions.New(genericapiserver.NewEmptyDelegateWithCustomHandler(notFoundHandler))
	if err != nil {
		return nil, err
	}
	crdAPIEnabled := config.ApiExtensions.GenericConfig.MergedResourceConfig.ResourceEnabled(apiextensionsv1.SchemeGroupVersion.WithResource("customresourcedefinitions"))

	kubeAPIServer, err := config.KubeAPIs.New(apiExtensionsServer.GenericAPIServer)
	if err != nil {
		return nil, err
	}

	// 聚合服务器位于委托链末端。
	aggregatorServer, err := controlplaneapiserver.CreateAggregatorServer(config.Aggregator, kubeAPIServer.ControlPlane.GenericAPIServer, apiExtensionsServer.Informers.Apiextensions().V1().CustomResourceDefinitions(), crdAPIEnabled, apiVersionPriorities)
	if err != nil {
		// 聚合服务器不创建 goroutine，无需特殊处理 innerStopCh。
		return nil, err
	}

	return aggregatorServer, nil
}

// CreateKubeAPIServerConfig 创建运行 API 服务器所需的全部资源，但不启动它们。
func CreateKubeAPIServerConfig(
	opts options.CompletedOptions,
	genericConfig *genericapiserver.Config,
	versionedInformers clientgoinformers.SharedInformerFactory,
	storageFactory *serverstorage.DefaultStorageFactory,
) (
	*controlplane.Config,
	aggregatorapiserver.ServiceResolver,
	[]admission.PluginInitializer,
	error,
) {
	// 初始化进程级能力配置。
	capabilities.Setup(opts.AllowPrivileged, opts.MaxConnectionBytesPerSec)

	// 创建额外的准入插件初始化器。
	kubeAdmissionConfig := &kubeapiserveradmission.Config{}
	kubeInitializers, err := kubeAdmissionConfig.New()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to create admission plugin initializer: %w", err)
	}

	endpointSliceGetter, err := proxy.NewEndpointSliceIndexerGetter(versionedInformers.Discovery().V1().EndpointSlices())
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to create endpoint slice getter: %w", err)
	}

	serviceResolver, err := buildServiceResolver(opts.EnableAggregatorRouting, genericConfig.LoopbackClientConfig.Host, versionedInformers, endpointSliceGetter)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("error building service resolver: %w", err)
	}
	controlplaneConfig, admissionInitializers, err := controlplaneapiserver.CreateConfig(opts.CompletedOptions, genericConfig, versionedInformers, endpointSliceGetter, storageFactory, serviceResolver, kubeInitializers)
	if err != nil {
		return nil, nil, nil, err
	}

	config := &controlplane.Config{
		ControlPlane: *controlplaneConfig,
		Extra: controlplane.Extra{
			KubeletClientConfig: opts.KubeletConfig,

			ServiceIPRange:          opts.PrimaryServiceClusterIPRange,
			APIServerServiceIP:      opts.APIServerServiceIP,
			SecondaryServiceIPRange: opts.SecondaryServiceClusterIPRange,

			APIServerServicePort: 443,

			ServiceNodePortRange:      opts.ServiceNodePortRange,
			KubernetesServiceNodePort: opts.KubernetesServiceNodePort,

			EndpointReconcilerType: reconcilers.Type(opts.EndpointReconcilerType),
			MasterCount:            opts.MasterCount,
		},
	}

	if config.ControlPlane.Generic.EgressSelector != nil {
		// 通过出口选择器查找连接 kubelet 所需的拨号器。
		config.Extra.KubeletClientConfig.Lookup = config.ControlPlane.Generic.EgressSelector.Lookup

		// "proxy" 子资源也使用出口选择器对应的传输层。
		networkContext := egressselector.Cluster.AsNetworkContext()
		dialer, err := config.ControlPlane.Generic.EgressSelector.Lookup(networkContext)
		if err != nil {
			return nil, nil, nil, err
		}
		c := config.ControlPlane.Extra.ProxyTransport.Clone()
		c.DialContext = dialer
		config.ControlPlane.ProxyTransport = c
	}

	return config, serviceResolver, admissionInitializers, nil
}

var testServiceResolver webhook.ServiceResolver

// SetServiceResolverForTests 允许测试替换 Service 解析器。
// 它修改全局状态且不能与服务器启动并发，使用该函数的测试必须串行运行。
func SetServiceResolverForTests(resolver webhook.ServiceResolver) func() {
	if testServiceResolver != nil {
		panic("test service resolver is set: tests are either running concurrently or clean up was skipped")
	}

	testServiceResolver = resolver

	return func() {
		testServiceResolver = nil
	}
}

func buildServiceResolver(enabledAggregatorRouting bool, hostname string, informer clientgoinformers.SharedInformerFactory, endpointSliceGetter proxy.EndpointSliceGetter) (webhook.ServiceResolver, error) {
	if testServiceResolver != nil {
		return testServiceResolver, nil
	}

	var serviceResolver webhook.ServiceResolver
	if enabledAggregatorRouting {
		serviceResolver = aggregatorapiserver.NewEndpointServiceResolver(
			informer.Core().V1().Services().Lister(),
			endpointSliceGetter,
		)
	} else {
		serviceResolver = aggregatorapiserver.NewClusterIPServiceResolver(
			informer.Core().V1().Services().Lister(),
		)
	}

	// 在本地解析 kubernetes.default.svc。
	if localHost, err := url.Parse(hostname); err == nil {
		serviceResolver = aggregatorapiserver.NewLoopbackServiceResolver(serviceResolver, localHost)
	}
	return serviceResolver, nil
}
