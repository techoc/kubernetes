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
	"fmt"
	"io"
	"net"

	"github.com/spf13/cobra"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/version"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/endpoints/openapi"
	genericapiserver "k8s.io/apiserver/pkg/server"
	genericoptions "k8s.io/apiserver/pkg/server/options"
	"k8s.io/apiserver/pkg/util/compatibility"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	basecompatibility "k8s.io/component-base/compatibility"
	"k8s.io/component-base/featuregate"
	baseversion "k8s.io/component-base/version"
	"k8s.io/sample-apiserver/pkg/admission/plugin/banflunder"
	"k8s.io/sample-apiserver/pkg/admission/wardleinitializer"
	"k8s.io/sample-apiserver/pkg/apis/wardle/v1alpha1"
	"k8s.io/sample-apiserver/pkg/apiserver"
	clientset "k8s.io/sample-apiserver/pkg/generated/clientset/versioned"
	informers "k8s.io/sample-apiserver/pkg/generated/informers/externalversions"
	sampleopenapi "k8s.io/sample-apiserver/pkg/generated/openapi"
	netutils "k8s.io/utils/net"
)

// 示例服务使用独立的 etcd 前缀，避免与同集群的其他 API 服务争用键空间。
const defaultEtcdPathPrefix = "/registry/wardle.example.com"

// WardleServerOptions 保存启动 Wardle API 服务器所需的选项。
// 启动参数集中放在这里，便于命令层与 Config 之间传递，测试时也能整体替换。
type WardleServerOptions struct {
	// RecommendedOptions 提供通用 apiserver 的推荐配置。
	RecommendedOptions *genericoptions.RecommendedOptions
	// ComponentGlobalsRegistry 保存所有组件的有效版本与 feature gate 配置。
	ComponentGlobalsRegistry basecompatibility.ComponentGlobalsRegistry

	// SharedInformerFactory 由 ExtraAdmissionInitializers 创建，启动后再拉起，避免准入插件拿到未启动的工厂。
	SharedInformerFactory informers.SharedInformerFactory
	// StdOut 是注入的标准输出流，测试可替换成内存 buffer。
	StdOut io.Writer
	// StdErr 是注入的标准错误流，测试可替换成内存 buffer。
	StdErr io.Writer

	// AlternateDNS 是自签证书的备用 DNS 名，方便通过非 localhost 地址访问示例服务。
	AlternateDNS []string
}

// WardleVersionToKubeVersion 把 Wardle 版本映射到它模拟的 kube 版本。
// 主版本不一致时无从推断兼容性，返回 nil 表示没有映射。
func WardleVersionToKubeVersion(ver *version.Version) *version.Version {
	// 只有 1.x 有对应关系，跨主版本直接放弃。
	if ver.Major() != 1 {
		return nil
	}
	kubeVer := version.MustParse(baseversion.DefaultKubeBinaryVersion)
	// Wardle 1.2 对应二进制版本，其余 1.x 版本按次版本号偏移。
	offset := int(ver.Minor()) - 2
	mappedVer := kubeVer.OffsetMinor(offset)
	// 模拟版本不能超过真实二进制，否则会暴露不存在的能力。
	if mappedVer.GreaterThan(kubeVer) {
		return kubeVer
	}
	return mappedVer
}

// NewWardleServerOptions 创建 WardleServerOptions。
// 默认使用全局组件注册表，让命令行与测试看到同一份组件配置。
func NewWardleServerOptions(out, errOut io.Writer) *WardleServerOptions {
	o := &WardleServerOptions{
		RecommendedOptions: genericoptions.NewRecommendedOptions(
			defaultEtcdPathPrefix,
			apiserver.Codecs.LegacyCodec(v1alpha1.SchemeGroupVersion),
		),
		ComponentGlobalsRegistry: compatibility.DefaultComponentGlobalsRegistry,

		StdOut: out,
		StdErr: errOut,
	}
	// 落盘统一编码到 v1alpha1，不依赖客户端请求里携带的版本。
	o.RecommendedOptions.Etcd.StorageConfig.EncodeVersioner = runtime.NewMultiGroupVersioner(v1alpha1.SchemeGroupVersion, schema.GroupKind{Group: v1alpha1.GroupName})
	return o
}

// NewCommandStartWardleServer 创建使用默认 WardleServerOptions 的服务器启动命令。
// 命令本身也是示例：在这里演示 KEP-4330 的版本兼容与版本化 feature gate 配置。
func NewCommandStartWardleServer(ctx context.Context, defaults *WardleServerOptions, skipDefaultComponentGlobalsRegistrySet bool) *cobra.Command {
	// 按值拷贝，Complete/Validate 的修改不会污染调用方的默认值。
	o := *defaults
	cmd := &cobra.Command{
		Short: "Launch a wardle API server",
		Long:  "Launch a wardle API server",
		PersistentPreRunE: func(*cobra.Command, []string) error {
			// 集成测试会自行设置全局注册表，这里留出跳过入口。
			if skipDefaultComponentGlobalsRegistrySet {
				return nil
			}
			return defaults.ComponentGlobalsRegistry.Set()
		},
		RunE: func(c *cobra.Command, args []string) error {
			// 顺序固定：先补全默认值，再校验，最后才启动。
			if err := o.Complete(); err != nil {
				return err
			}
			if err := o.Validate(args); err != nil {
				return err
			}
			if err := o.RunWardleServer(c.Context()); err != nil {
				return err
			}
			return nil
		},
	}
	cmd.SetContext(ctx)

	flags := cmd.Flags()
	o.RecommendedOptions.AddFlags(flags)

	// 下面按 KEP-4330 为 Wardle 组件配置版本兼容与 feature gate。
	// EffectiveVersion 同时确定二进制版本、模拟版本和最低兼容版本：
	// 二进制版本是实际运行的代码版本，模拟版本是要模拟的能力版本，
	// 最低兼容版本是组件仍保证兼容的最早版本。
	// 详见 KEP-4330：https://github.com/kubernetes/enhancements/blob/master/keps/sig-architecture/4330-compatibility-versions
	// 选 1.2 作为基线，是因为 BanFlunder 在该版本已 GA 并锁定默认值。
	defaultWardleVersion := "1.2"
	// 把 Wardle 组件及其有效版本和 feature gate 注册到全局注册表。
	// 注册幂等：重复调用不会覆盖已有组件，集成测试因此可以放心复用。
	_, wardleFeatureGate := defaults.ComponentGlobalsRegistry.ComponentGlobalsOrRegister(
		apiserver.WardleComponentName, basecompatibility.NewEffectiveVersionFromString(defaultWardleVersion, "", ""),
		featuregate.NewVersionedFeatureGate(version.MustParse(defaultWardleVersion)))

	// 为 BanFlunder 添加版本化规格，规格与有效版本共同决定功能是否启用。
	// 按版本描述演进轨迹：1.0 默认关、1.1 默认开、1.2 GA 后锁定，用来演示版本化开关。
	utilruntime.Must(wardleFeatureGate.AddVersioned(map[featuregate.Feature]featuregate.VersionedSpecs{
		"BanFlunder": {
			{Version: version.MustParse("1.0"), Default: false, PreRelease: featuregate.Alpha},
			{Version: version.MustParse("1.1"), Default: true, PreRelease: featuregate.Beta},
			{Version: version.MustParse("1.2"), Default: true, PreRelease: featuregate.GA, LockToDefault: true},
		},
	}))

	// 若全局注册表尚无默认 kube 组件，则注册它；Wardle 的 feature gate 最终挂在它上面。
	_, _ = defaults.ComponentGlobalsRegistry.ComponentGlobalsOrRegister(basecompatibility.DefaultKubeComponent,
		basecompatibility.NewEffectiveVersionFromString(baseversion.DefaultKubeBinaryVersion, "", ""), utilfeature.DefaultMutableFeatureGate)

	// 设置 Wardle 到 kube 的模拟版本映射，让 kube 的模拟版本由 Wardle 决定。
	// 用映射把两侧模拟版本绑定，避免 Wardle 与 kube 各自开关出能力错配。
	utilruntime.Must(defaults.ComponentGlobalsRegistry.SetVersionMapping(apiserver.WardleComponentName, basecompatibility.DefaultKubeComponent, WardleVersionToKubeVersion))

	// 参数在 Set() 之前注册，执行时用户的覆盖值才会生效。
	defaults.ComponentGlobalsRegistry.AddFlags(flags)

	return cmd
}

// Validate 校验 WardleServerOptions。
// 聚合各来源的错误，一次运行把所有配置问题都报出来。
func (o WardleServerOptions) Validate(args []string) error {
	// 参数当前未参与校验，保留它只是为了对齐 apiserver 的命令模板。
	errors := []error{}
	errors = append(errors, o.RecommendedOptions.Validate()...)
	errors = append(errors, o.ComponentGlobalsRegistry.Validate()...)
	return utilerrors.NewAggregate(errors)
}

// Complete 补齐有效运行所需的字段。
// 插件是否注册由版本化开关决定，关掉时既不在插件表也不在推荐链里。
func (o *WardleServerOptions) Complete() error {
	// 用 wardle 自己的 feature gate 判断，与 kube 的默认开关互不影响。
	if o.ComponentGlobalsRegistry.FeatureGateFor(apiserver.WardleComponentName).Enabled("BanFlunder") {
		// 注册只把插件放进表，是否执行取决于下面追加的推荐顺序。
		banflunder.Register(o.RecommendedOptions.Admission.Plugins)

		// 必须同时追加推荐顺序，只注册不排序等于没启用。
		o.RecommendedOptions.Admission.RecommendedPluginOrder = append(o.RecommendedOptions.Admission.RecommendedPluginOrder, "BanFlunder")
	}
	return nil
}

// Config 根据 WardleServerOptions 构造 API 服务器配置。
// 把通用 apiserver 配置与 Wardle 自己的配置拼在一起。
func (o *WardleServerOptions) Config() (*apiserver.Config, error) {
	// 示例没有真实对外地址，先用 localhost 自签证书把安全端口跑通。
	// TODO：配置真实的外部地址。
	if err := o.RecommendedOptions.SecureServing.MaybeDefaultWithSelfSignedCerts("localhost", o.AlternateDNS, []net.IP{netutils.ParseIPSloppy("127.0.0.1")}); err != nil {
		return nil, fmt.Errorf("error creating self-signed certificates: %v", err)
	}

	// 准入插件需要 informer，用 loopback client 提前建好，启动阶段再统一拉起。
	o.RecommendedOptions.ExtraAdmissionInitializers = func(c *genericapiserver.RecommendedConfig) ([]admission.PluginInitializer, error) {
		// 走 loopback 复用自身 API，无需额外的 kubeconfig。
		client, err := clientset.NewForConfig(c.LoopbackClientConfig)
		if err != nil {
			return nil, err
		}
		informerFactory := informers.NewSharedInformerFactory(client, c.LoopbackClientConfig.Timeout)
		// 保存下来供 post-start hook 启动。
		o.SharedInformerFactory = informerFactory
		return []admission.PluginInitializer{wardleinitializer.New(informerFactory)}, nil
	}

	serverConfig := genericapiserver.NewRecommendedConfig(apiserver.Codecs)

	// OpenAPI 定义来自生成代码，标题与版本只是示例服务的元数据。
	serverConfig.OpenAPIConfig = genericapiserver.DefaultOpenAPIConfig(sampleopenapi.GetOpenAPIDefinitions, openapi.NewDefinitionNamer(apiserver.Scheme))
	serverConfig.OpenAPIConfig.Info.Title = "Wardle"
	serverConfig.OpenAPIConfig.Info.Version = "0.1"

	serverConfig.OpenAPIV3Config = genericapiserver.DefaultOpenAPIV3Config(sampleopenapi.GetOpenAPIDefinitions, openapi.NewDefinitionNamer(apiserver.Scheme))
	serverConfig.OpenAPIV3Config.Info.Title = "Wardle"
	serverConfig.OpenAPIV3Config.Info.Version = "0.1"

	// 版本与开关从注册表透传，避免 apiserver 与 wardle 各持一份配置而漂移。
	serverConfig.FeatureGate = o.ComponentGlobalsRegistry.FeatureGateFor(basecompatibility.DefaultKubeComponent)
	serverConfig.EffectiveVersion = o.ComponentGlobalsRegistry.EffectiveVersionFor(apiserver.WardleComponentName)

	if err := o.RecommendedOptions.ApplyTo(serverConfig); err != nil {
		return nil, err
	}

	// 把通用配置和 wardle 的空 ExtraConfig 组合成最终配置。
	config := &apiserver.Config{
		GenericConfig: serverConfig,
		ExtraConfig:   apiserver.ExtraConfig{},
	}
	return config, nil
}

// RunWardleServer 根据 WardleServerOptions 创建并运行 WardleServer。
// 先构建配置再建 server，错误逐层上抛，失败时不留半初始化对象。
func (o WardleServerOptions) RunWardleServer(ctx context.Context) error {
	config, err := o.Config()
	if err != nil {
		return err
	}

	server, err := config.Complete().New()
	if err != nil {
		return err
	}

	// informer 等存储就绪后再启动，避免第一轮 list/watch 打在还没准备好的后端上。
	server.GenericAPIServer.AddPostStartHookOrDie("start-sample-server-informers", func(context genericapiserver.PostStartHookContext) error {
		config.GenericConfig.SharedInformerFactory.Start(context.Done())
		o.SharedInformerFactory.Start(context.Done())
		return nil
	})

	// 用调用方传入的 ctx 驱动生命周期，关闭时能优雅停机。
	return server.GenericAPIServer.PrepareRun().RunWithContext(ctx)
}
