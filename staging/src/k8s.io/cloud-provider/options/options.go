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

// Package options 定义 cloud-controller-manager 的命令行选项：
// 负责默认值、flag 注册、校验，以及把选项落成 config.Config 供运行时使用。
package options

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	apiserveroptions "k8s.io/apiserver/pkg/server/options"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/record"
	cloudprovider "k8s.io/cloud-provider"
	"k8s.io/cloud-provider/app/config"
	ccmconfig "k8s.io/cloud-provider/config"
	ccmconfigscheme "k8s.io/cloud-provider/config/install"
	ccmconfigv1alpha1 "k8s.io/cloud-provider/config/v1alpha1"
	"k8s.io/cloud-provider/names"
	cliflag "k8s.io/component-base/cli/flag"
	cmoptions "k8s.io/controller-manager/options"
	"k8s.io/controller-manager/pkg/clientbuilder"
	netutils "k8s.io/utils/net"

	// add the related feature gates
	_ "k8s.io/controller-manager/pkg/features/register"
)

const (
	// CloudControllerManagerUserAgent is the userAgent name when starting cloud-controller managers.
	// 固定 user-agent 便于 API Server 审计与限流识别组件来源。
	CloudControllerManagerUserAgent = "cloud-controller-manager"
)

// CloudControllerManagerOptions is the main context object for the controller manager.
// 各字段分别对应一个 flag 分组；Options 是可变的（flag 解析会写入），
// 校验完成后才通过 ApplyTo 复制到不可变的运行时 Config。
type CloudControllerManagerOptions struct {
	Generic                 *cmoptions.GenericControllerManagerConfigurationOptions
	KubeCloudShared         *KubeCloudSharedOptions
	ServiceController       *ServiceControllerOptions
	NodeController          *NodeControllerOptions
	NodeLifecycleController *NodeLifecycleControllerOptions

	// 安全监听、认证、授权均复用 apiserver 的标准选项实现，保证行为与 kube-apiserver 一致。
	SecureServing  *apiserveroptions.SecureServingOptionsWithLoopback
	Authentication *apiserveroptions.DelegatingAuthenticationOptions
	Authorization  *apiserveroptions.DelegatingAuthorizationOptions

	// Master 覆盖 kubeconfig 中的 API Server 地址，便于测试与本地调试。
	Master string

	WebhookServing *WebhookServingOptions
	Webhook        *WebhookOptions

	// NodeStatusUpdateFrequency is the frequency at which the controller updates nodes' status
	// 节点状态写回频率；过低会拖慢调度，过高会放大 API Server 写压力。
	NodeStatusUpdateFrequency metav1.Duration
}

// ProviderDefaults are provided by the consumer when calling
// NewCloudControllerManagerOptions(), so that they can customize certain flag
// default values.
// 云提供商可在不回退到 flag 的情况下调整 webhook 端点的默认监听地址。
type ProviderDefaults struct {
	// WebhookBindAddress is the default address.  It can be overridden by "--webhook-bind-address".
	WebhookBindAddress *net.IP
	// WebhookBindPort is the default port.  It can be overridden by "--webhook-bind-port".
	WebhookBindPort *int
}

// NewCloudControllerManagerOptions creates a new ExternalCMServer with a default config.
// 使用空的 ProviderDefaults，即所有默认值均来自组件配置。
func NewCloudControllerManagerOptions() (*CloudControllerManagerOptions, error) {
	return NewCloudControllerManagerOptionsWithProviderDefaults(ProviderDefaults{})
}

// NewCloudControllerManagerOptionsWithProviderDefaults creates a new
// ExternalCMServer with a default config, but allows the cloud provider to
// override a select number of default option values.
// 允许云提供商定制 webhook 的默认绑定地址/端口。
func NewCloudControllerManagerOptionsWithProviderDefaults(defaults ProviderDefaults) (*CloudControllerManagerOptions, error) {
	// 先通过 scheme 默认化 v1alpha1 再转换为内部类型，确保默认值只有一处定义。
	componentConfig, err := NewDefaultComponentConfig()
	if err != nil {
		return nil, err
	}

	s := CloudControllerManagerOptions{
		Generic:         cmoptions.NewGenericControllerManagerConfigurationOptions(&componentConfig.Generic),
		KubeCloudShared: NewKubeCloudSharedOptions(&componentConfig.KubeCloudShared),
		NodeController: &NodeControllerOptions{
			NodeControllerConfiguration: &componentConfig.NodeController,
		},
		NodeLifecycleController: &NodeLifecycleControllerOptions{
			NodeLifecycleControllerConfiguration: &componentConfig.NodeLifecycleController,
		},
		ServiceController: &ServiceControllerOptions{
			ServiceControllerConfiguration: &componentConfig.ServiceController,
		},
		SecureServing:             apiserveroptions.NewSecureServingOptions().WithLoopback(),
		Webhook:                   NewWebhookOptions(),
		WebhookServing:            NewWebhookServingOptions(defaults),
		Authentication:            apiserveroptions.NewDelegatingAuthenticationOptions(),
		Authorization:             apiserveroptions.NewDelegatingAuthorizationOptions(),
		NodeStatusUpdateFrequency: componentConfig.NodeStatusUpdateFrequency,
	}

	// CCM 通常运行在集群内并使用 in-cluster 配置，远程 kubeconfig 不是必需的。
	s.Authentication.RemoteKubeConfigFileOptional = true
	s.Authorization.RemoteKubeConfigFileOptional = true

	// Set the PairName but leave certificate directory blank to generate in-memory by default
	s.SecureServing.ServerCert.CertDirectory = ""
	s.SecureServing.ServerCert.PairName = "cloud-controller-manager"
	s.SecureServing.BindPort = cloudprovider.CloudControllerManagerPort

	// 选主使用固定的 Lease 名称与命名空间，保证多副本 CCM 能竞争同一把锁。
	s.Generic.LeaderElection.ResourceName = "cloud-controller-manager"
	s.Generic.LeaderElection.ResourceNamespace = "kube-system"

	return &s, nil
}

// NewDefaultComponentConfig returns cloud-controller manager configuration object.
// 默认化在外部版本上执行，再转换到内部版本，避免默认值逻辑分散到两个版本。
func NewDefaultComponentConfig() (*ccmconfig.CloudControllerManagerConfiguration, error) {
	versioned := &ccmconfigv1alpha1.CloudControllerManagerConfiguration{}
	ccmconfigscheme.Scheme.Default(versioned)

	// 转换失败通常意味着 scheme 注册缺失或版本间字段不兼容，属于启动期致命错误。
	internal := &ccmconfig.CloudControllerManagerConfiguration{}
	if err := ccmconfigscheme.Scheme.Convert(versioned, internal, nil); err != nil {
		return nil, err
	}

	return internal, nil
}

// Flags returns flags for a specific CloudController by section name
// 每个控制器/子系统使用独立 FlagSet，使 --help 输出按功能分组。
func (o *CloudControllerManagerOptions) Flags(allControllers []string, disabledByDefaultControllers []string, controllerAliases map[string]string, allWebhooks, disabledByDefaultWebhooks []string) cliflag.NamedFlagSets {
	fss := cliflag.NamedFlagSets{}
	o.Generic.AddFlags(&fss, allControllers, disabledByDefaultControllers, controllerAliases)
	o.KubeCloudShared.AddFlags(fss.FlagSet("generic"))
	o.NodeController.AddFlags(fss.FlagSet(names.CloudNodeController))
	o.NodeLifecycleController.AddFlags(fss.FlagSet(names.CloudNodeLifecycleController))
	o.ServiceController.AddFlags(fss.FlagSet(names.ServiceLBController))
	if o.Webhook != nil {
		o.Webhook.AddFlags(fss.FlagSet("webhook"), allWebhooks, disabledByDefaultWebhooks)
	}
	if o.WebhookServing != nil {
		o.WebhookServing.AddFlags(fss.FlagSet("webhook serving"))
	}

	o.SecureServing.AddFlags(fss.FlagSet("secure serving"))
	o.Authentication.AddFlags(fss.FlagSet("authentication"))
	o.Authorization.AddFlags(fss.FlagSet("authorization"))

	// misc 组承载跨子系统的通用 flag（master/kubeconfig/节点状态频率）。
	fs := fss.FlagSet("misc")
	fs.StringVar(&o.Master, "master", o.Master, "The address of the Kubernetes API server (overrides any value in kubeconfig).")
	fs.StringVar(&o.Generic.ClientConnection.Kubeconfig, "kubeconfig", o.Generic.ClientConnection.Kubeconfig, "Path to kubeconfig file with authorization and master location information (the master location can be overridden by the master flag).")
	fs.DurationVar(&o.NodeStatusUpdateFrequency.Duration, "node-status-update-frequency", o.NodeStatusUpdateFrequency.Duration, "Specifies how often the controller updates nodes' status.")
	utilfeature.DefaultMutableFeatureGate.AddFlag(fss.FlagSet("generic"))

	return fss
}

// ApplyTo fills up cloud controller manager config with options.
// 把选项复制到运行时 Config；调用前必须已通过 Validate。
func (o *CloudControllerManagerOptions) ApplyTo(c *config.Config, allControllers []string, disabledByDefaultControllers []string, controllerAliases map[string]string, userAgent string) error {
	var err error

	// Build kubeconfig first to so that if it fails, it doesn't cause leaking
	// goroutines (started from initializing secure serving - which underneath
	// creates a queue which in its constructor starts a goroutine).
	// 先构建 kubeconfig：一旦失败立即返回，避免已启动的后台 goroutine 泄漏。
	c.Kubeconfig, err = clientcmd.BuildConfigFromFlags(o.Master, o.Generic.ClientConnection.Kubeconfig)
	if err != nil {
		return err
	}
	// 压缩会增加 API Server CPU 开销，CCM 场景下收益有限，故关闭。
	c.Kubeconfig.DisableCompression = true
	c.Kubeconfig.ContentConfig.AcceptContentTypes = o.Generic.ClientConnection.AcceptContentTypes
	c.Kubeconfig.ContentConfig.ContentType = o.Generic.ClientConnection.ContentType
	c.Kubeconfig.QPS = o.Generic.ClientConnection.QPS
	c.Kubeconfig.Burst = int(o.Generic.ClientConnection.Burst)

	if err = o.Generic.ApplyTo(&c.ComponentConfig.Generic, allControllers, disabledByDefaultControllers, controllerAliases); err != nil {
		return err
	}
	if err = o.KubeCloudShared.ApplyTo(&c.ComponentConfig.KubeCloudShared); err != nil {
		return err
	}
	if err = o.ServiceController.ApplyTo(&c.ComponentConfig.ServiceController); err != nil {
		return err
	}
	if o.Webhook != nil {
		if err = o.Webhook.ApplyTo(&c.ComponentConfig.Webhook); err != nil {
			return err
		}
	}
	if o.WebhookServing != nil {
		if err = o.WebhookServing.ApplyTo(&c.WebhookSecureServing, c.ComponentConfig.Webhook); err != nil {
			return err
		}
	}
	if err = o.SecureServing.ApplyTo(&c.SecureServing, &c.LoopbackClientConfig); err != nil {
		return err
	}
	// 只有真正监听安全端口时才需要委托认证/授权，否则跳过以避免无谓初始化。
	if o.SecureServing.BindPort != 0 || o.SecureServing.Listener != nil {
		if err = o.Authentication.ApplyTo(&c.Authentication, c.SecureServing, nil); err != nil {
			return err
		}
		if err = o.Authorization.ApplyTo(&c.Authorization); err != nil {
			return err
		}
	}

	// 所有共享客户端都带上组件 user-agent，便于审计与限流。
	c.Client, err = clientset.NewForConfig(restclient.AddUserAgent(c.Kubeconfig, userAgent))
	if err != nil {
		return err
	}

	c.EventBroadcaster = record.NewBroadcaster(record.WithContext(context.TODO())) // TODO: move broadcaster construction to a place where there is a proper context.
	c.EventRecorder = c.EventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: userAgent})

	rootClientBuilder := clientbuilder.SimpleControllerClientBuilder{
		ClientConfig: c.Kubeconfig,
	}
	// 启用 ServiceAccount 凭证时，每个控制器用独立身份并走 TokenRequest；
	// 否则所有控制器共用同一个凭据（兼容旧部署）。
	if c.ComponentConfig.KubeCloudShared.UseServiceAccountCredentials {
		c.ClientBuilder = clientbuilder.NewDynamicClientBuilder(
			restclient.AnonymousClientConfig(c.Kubeconfig),
			c.Client.CoreV1(),
			metav1.NamespaceSystem)
	} else {
		c.ClientBuilder = rootClientBuilder
	}
	// informer 使用固定身份，与各控制器自己的客户端解耦。
	c.VersionedClient = rootClientBuilder.ClientOrDie("shared-informers")
	// resync 周期加入随机抖动，避免所有 CCM 实例同时向 API Server 发起全量 list。
	c.SharedInformers = informers.NewSharedInformerFactory(c.VersionedClient, resyncPeriod(c)())

	// sync back to component config
	// TODO: find more elegant way than syncing back the values.
	// flag 值保存在 Options 中，这里回写到 ComponentConfig，保证两种表示一致。
	c.ComponentConfig.NodeStatusUpdateFrequency = o.NodeStatusUpdateFrequency
	if err = o.NodeController.ApplyTo(&c.ComponentConfig.NodeController); err != nil {
		return err
	}
	if err = o.NodeLifecycleController.ApplyTo(&c.ComponentConfig.NodeLifecycleController); err != nil {
		return err
	}

	return nil
}

// Validate is used to validate config before launching the cloud controller manager
// 汇总所有子选项的校验错误后一次性返回，避免用户反复修一个跑一次。
func (o *CloudControllerManagerOptions) Validate(allControllers []string, disabledByDefaultControllers []string, controllerAliases map[string]string, allWebhooks, disabledByDefaultWebhooks []string) error {
	errors := []error{}

	errors = append(errors, o.Generic.Validate(allControllers, disabledByDefaultControllers, controllerAliases)...)
	errors = append(errors, o.KubeCloudShared.Validate()...)
	errors = append(errors, o.NodeLifecycleController.Validate()...)
	errors = append(errors, o.ServiceController.Validate()...)
	errors = append(errors, o.SecureServing.Validate()...)
	errors = append(errors, o.Authentication.Validate()...)
	errors = append(errors, o.Authorization.Validate()...)

	if o.Webhook != nil {
		errors = append(errors, o.Webhook.Validate(allWebhooks, disabledByDefaultWebhooks)...)
	}
	if o.WebhookServing != nil {
		errors = append(errors, o.WebhookServing.Validate()...)

		// 两个端口若相同会导致监听失败，必须在启动前明确报错。
		if o.WebhookServing.BindPort == o.SecureServing.BindPort && o.WebhookServing.BindPort != 0 {
			errors = append(errors, fmt.Errorf("--webhook-secure-port cannot be the same value as --secure-port"))
		}
	}
	if len(o.KubeCloudShared.CloudProvider.Name) == 0 {
		errors = append(errors, fmt.Errorf("--cloud-provider cannot be empty"))
	}

	return utilerrors.NewAggregate(errors)
}

// resyncPeriod computes the time interval a shared informer waits before resyncing with the api server
// 在 [1,2) 倍 MinResyncPeriod 之间随机取值，打散多副本的重同步峰值。
func resyncPeriod(c *config.Config) func() time.Duration {
	return func() time.Duration {
		factor := rand.Float64() + 1
		return time.Duration(float64(c.ComponentConfig.Generic.MinResyncPeriod.Nanoseconds()) * factor)
	}
}

// Config return a cloud controller manager config objective
// 校验 -> 生成自签证书 -> 应用选项，产出可直接交给命令运行的 Config。
func (o *CloudControllerManagerOptions) Config(allControllers []string, disabledByDefaultControllers []string, controllerAliases map[string]string, allWebhooks, disabledByDefaultWebhooks []string) (*config.Config, error) {
	if err := o.Validate(allControllers, disabledByDefaultControllers, controllerAliases, allWebhooks, disabledByDefaultWebhooks); err != nil {
		return nil, err
	}

	// 未显式提供证书时生成自签证书，保证开箱即用；仅绑定 localhost/127.0.0.1。
	if err := o.SecureServing.MaybeDefaultWithSelfSignedCerts("localhost", nil, []net.IP{netutils.ParseIPSloppy("127.0.0.1")}); err != nil {
		return nil, fmt.Errorf("error creating self-signed certificates: %v", err)
	}

	if o.WebhookServing != nil {
		if err := o.WebhookServing.MaybeDefaultWithSelfSignedCerts("localhost", nil, []net.IP{netutils.ParseIPSloppy("127.0.0.1")}); err != nil {
			return nil, fmt.Errorf("error creating self-signed certificates for webhook: %v", err)
		}
	}

	c := &config.Config{}
	if err := o.ApplyTo(c, allControllers, disabledByDefaultControllers, controllerAliases, CloudControllerManagerUserAgent); err != nil {
		return nil, err
	}

	return c, nil
}
