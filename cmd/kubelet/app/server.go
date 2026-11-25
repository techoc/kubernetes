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

// Package app 提供在不同场景下创建 Kubelet 服务的组件。
package app

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/coreos/go-systemd/v22/daemon"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	jsonpatch "gopkg.in/evanphx/json-patch.v4"

	"k8s.io/klog/v2"
	"k8s.io/mount-utils"

	cadvisorapi "github.com/google/cadvisor/lib/model"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	otelsdkresource "go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.41.0"
	oteltrace "go.opentelemetry.io/otel/trace"
	noopoteltrace "go.opentelemetry.io/otel/trace/noop"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation/field"
	utilversion "k8s.io/apimachinery/pkg/util/version"
	"k8s.io/apimachinery/pkg/util/wait"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/server/flagz"
	"k8s.io/apiserver/pkg/server/healthz"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	clientset "k8s.io/client-go/kubernetes"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/record"
	certutil "k8s.io/client-go/util/cert"
	"k8s.io/client-go/util/certificate"
	"k8s.io/client-go/util/connrotation"
	"k8s.io/client-go/util/keyutil"
	cliflag "k8s.io/component-base/cli/flag"
	"k8s.io/component-base/configz"
	"k8s.io/component-base/featuregate"
	"k8s.io/component-base/logs"
	logsapi "k8s.io/component-base/logs/api/v1"
	"k8s.io/component-base/metrics"
	metricsfeatures "k8s.io/component-base/metrics/features"
	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/component-base/tracing"
	"k8s.io/component-base/version"
	"k8s.io/component-base/version/verflag"
	zpagesfeatures "k8s.io/component-base/zpages/features"
	nodeutil "k8s.io/component-helpers/node/util"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	kubeletconfigv1beta1 "k8s.io/kubelet/config/v1beta1"
	"k8s.io/kubernetes/cmd/kubelet/app/options"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/capabilities"
	"k8s.io/kubernetes/pkg/credentialprovider"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/kubelet"
	kubeletconfiginternal "k8s.io/kubernetes/pkg/kubelet/apis/config"
	kubeletscheme "k8s.io/kubernetes/pkg/kubelet/apis/config/scheme"
	kubeletconfigv1beta1conversion "k8s.io/kubernetes/pkg/kubelet/apis/config/v1beta1"
	kubeletconfigvalidation "k8s.io/kubernetes/pkg/kubelet/apis/config/validation"
	"k8s.io/kubernetes/pkg/kubelet/cadvisor"
	kubeletcertificate "k8s.io/kubernetes/pkg/kubelet/certificate"
	"k8s.io/kubernetes/pkg/kubelet/certificate/bootstrap"
	"k8s.io/kubernetes/pkg/kubelet/cm"
	"k8s.io/kubernetes/pkg/kubelet/cm/cpumanager/topology"
	"k8s.io/kubernetes/pkg/kubelet/config"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	"k8s.io/kubernetes/pkg/kubelet/eviction"
	evictionapi "k8s.io/kubernetes/pkg/kubelet/eviction/api"
	"k8s.io/kubernetes/pkg/kubelet/kubeletconfig/configfiles"
	kubeletmetrics "k8s.io/kubernetes/pkg/kubelet/metrics"
	"k8s.io/kubernetes/pkg/kubelet/server"
	"k8s.io/kubernetes/pkg/kubelet/stats/pidlimit"
	kubelettypes "k8s.io/kubernetes/pkg/kubelet/types"
	kubeletutil "k8s.io/kubernetes/pkg/kubelet/util"
	"k8s.io/kubernetes/pkg/kubelet/watchdog"
	utilfs "k8s.io/kubernetes/pkg/util/filesystem"
	"k8s.io/kubernetes/pkg/util/flock"
	utilkernel "k8s.io/kubernetes/pkg/util/kernel"
	"k8s.io/kubernetes/pkg/util/oom"
	"k8s.io/kubernetes/pkg/util/rlimit"
	"k8s.io/kubernetes/pkg/volume/util/hostutil"
	"k8s.io/kubernetes/pkg/volume/util/subpath"
	"k8s.io/utils/cpuset"
	"k8s.io/utils/exec"
	netutils "k8s.io/utils/net"
	"sigs.k8s.io/yaml"
)

// 包初始化时登记日志 Feature Gate，并关闭本组件不使用的 OTel MeterProvider，避免无意义的指标资源。
func init() {
	utilruntime.Must(logsapi.AddFeatureGates(utilfeature.DefaultMutableFeatureGate))
	// 本组件不使用 OTel 指标；替换默认 Provider 以避免其资源泄漏：
	// https://github.com/open-telemetry/opentelemetry-go-contrib/issues/5190
	otel.SetMeterProvider(noop.NewMeterProvider())
}

// NewKubeletCommand 创建 Kubelet 命令。由于 Flag 优先级和配置文件合并顺序特殊，参数解析由 RunE 手动完成。
func NewKubeletCommand(ctx context.Context) *cobra.Command {
	logger := klog.FromContext(ctx)
	// 使用独立 FlagSet，避免 Cobra 默认把全局参数混入其中；规范化函数兼容下划线形式。
	cleanFlagSet := pflag.NewFlagSet(server.ComponentKubelet, pflag.ContinueOnError)
	cleanFlagSet.SetNormalizeFunc(cliflag.WordSepNormalizeFunc)
	kubeletFlags := options.NewKubeletFlags()

	// 先创建默认组件配置，第一轮解析命令行参数时即可把 Flag 值写入它。
	kubeletConfig, err := options.NewKubeletConfiguration()
	// 默认配置创建失败属于程序错误。
	if err != nil {
		logger.Error(err, "Failed to create a new kubelet configuration")
		os.Exit(1)
	}

	cmd := &cobra.Command{
		Use: server.ComponentKubelet,
		Long: `The kubelet is the primary "node agent" that runs on each
node. It can register the node with the apiserver using one of: the hostname; a flag to
override the hostname; or specific logic for a cloud provider.

The kubelet works in terms of a PodSpec. A PodSpec is a YAML or JSON object
that describes a pod. The kubelet takes a set of PodSpecs that are provided through
various mechanisms (primarily through the apiserver) and ensures that the containers
described in those PodSpecs are running and healthy. The kubelet doesn't manage
containers which were not created by Kubernetes.

Other than from an PodSpec from the apiserver, there are two ways that a container
manifest can be provided to the Kubelet.

File: Path passed as a flag on the command line. Files under this path will be monitored
periodically for updates. The monitoring period is 20s by default and is configurable
via a flag.

HTTP endpoint: HTTP endpoint passed as a parameter on the command line. This endpoint
is checked every 20 seconds (also configurable with a flag).`,
		// Kubelet 需要特殊的参数优先级规则，因此在 Run 中手动完成全部解析。
		// DisableFlagParsing=true 让 Run 的 args 收到完整参数，不受 Cobra 干预。
		DisableFlagParsing: true,
		SilenceUsage:       true,
		RunE: func(cmd *cobra.Command, args []string) error {
			// 手动完成第一轮解析，先取得配置文件路径和加载配置前需要校验的参数。
			if err := cleanFlagSet.Parse(args); err != nil {
				return fmt.Errorf("failed to parse kubelet flag: %w", err)
			}

			// 检查命令行是否包含非参数项。
			cmds := cleanFlagSet.Args()
			if len(cmds) > 0 {
				return fmt.Errorf("unknown command %+s", cmds[0])
			}

			// 请求帮助时直接返回。
			help, err := cleanFlagSet.GetBool("help")
			if err != nil {
				return errors.New(`"help" flag is non-bool, programmer error, please correct`)
			}
			if help {
				return cmd.Help()
			}

			// 请求版本信息时直接打印并退出。
			verflag.PrintAndExitIfRequested()

			// 根据首轮命令行参数设置 Feature Gate。
			if err := utilfeature.DefaultMutableFeatureGate.SetFromMap(kubeletConfig.FeatureGates); err != nil {
				return fmt.Errorf("failed to set feature gates from initial flags-based config: %w", err)
			}

			// 校验首轮解析出的节点级参数。
			if err := options.ValidateKubeletFlags(kubeletFlags); err != nil {
				return fmt.Errorf("failed to validate kubelet flags: %w", err)
			}

			// 组件配置文件作为基础配置；后续再把命令行中显式出现的值覆盖到新对象。
			if len(kubeletFlags.KubeletConfigFile) > 0 {
				kubeletConfig, err = loadConfigFile(ctx, kubeletFlags.KubeletConfigFile)
				if err != nil {
					return fmt.Errorf("failed to load kubelet config file, path: %s, error: %w", kubeletFlags.KubeletConfigFile, err)
				}
			}
			// Drop-in 文件按名字字典序合并，后加载的配置可以覆盖前面文件及基础配置的字段。
			var skippedDropinFiles []string
			if len(kubeletFlags.KubeletDropinConfigDirectory) > 0 {
				skippedDropinFiles, err = mergeKubeletConfigurations(kubeletConfig, kubeletFlags.KubeletDropinConfigDirectory)
				if err != nil {
					return fmt.Errorf("failed to merge kubelet configs: %w", err)
				}
			}

			if len(kubeletFlags.KubeletConfigFile) > 0 || len(kubeletFlags.KubeletDropinConfigDirectory) > 0 {
				// 文件加载后必须把原始命令行重新解析到新配置对象，保证 Flag 优先级高于配置文件。
				// 重新解析是为保持二进制升级时的向后兼容；参见 issue #56171。
				if err := kubeletConfigFlagPrecedence(kubeletConfig, args); err != nil {
					return fmt.Errorf("failed to precedence kubeletConfigFlag: %w", err)
				}
				// 根据合并后的配置更新 Feature Gate。
				if err := utilfeature.DefaultMutableFeatureGate.SetFromMap(kubeletConfig.FeatureGates); err != nil {
					return fmt.Errorf("failed to set feature gates from initial flags-based config: %w", err)
				}
			}

			// 配置来源和覆盖关系确定后再初始化日志，确保日志行为采用最终配置。
			logs.InitLogs()
			// 日志设置更新后重新获取 logger。
			logger = klog.FromContext(ctx)
			if err := logsapi.ValidateAndApplyAsField(&kubeletConfig.Logging, utilfeature.DefaultFeatureGate, field.NewPath("logging")); err != nil {
				return fmt.Errorf("initialize logging: %v", err)
			}
			// 输出完整生效配置，便于诊断。
			if cfgStr, err := marshalKubeletConfigForLog(kubeletConfig); err != nil {
				// 日志序列化失败不得阻止启动，记录错误后继续。
				logger.Error(err, "Failed to marshal effective KubeletConfiguration for logging")
			} else {
				logger.Info("Effective KubeletConfiguration", "config", cfgStr)
			}
			// 节点专属 Flag（如 --hostname-override、--kubeconfig、--node-ip、--cert-dir）
			// 这些参数直接来自命令行，仍准确有效，保留输出以便调试。
			cliflag.PrintFlags(cleanFlagSet)

			// 对最终合并配置做完整语义校验，而不只校验最初的命令行参数。
			if err := kubeletconfigvalidation.ValidateKubeletConfiguration(kubeletConfig, utilfeature.DefaultFeatureGate); err != nil {
				return fmt.Errorf("failed to validate kubelet configuration, error: %w, path: %s", err, kubeletConfig)
			}

			if (kubeletConfig.KubeletCgroups != "" && kubeletConfig.KubeReservedCgroup != "") && (strings.Index(kubeletConfig.KubeletCgroups, kubeletConfig.KubeReservedCgroup) != 0) {
				logger.Info("unsupported configuration:KubeletCgroups is not within KubeReservedCgroup")
			}

			// 合并后将节点本地 Flag 与组件配置聚合为 KubeletServer，供依赖构造和运行阶段统一消费。
			kubeletServer := &options.KubeletServer{
				KubeletFlags:         *kubeletFlags,
				KubeletConfiguration: *kubeletConfig,
			}

			// 提前构造基础依赖；需要 API 客户端的依赖在 Run 阶段补齐。
			kubeletDeps, err := UnsecuredDependencies(ctx, kubeletServer, utilfeature.DefaultFeatureGate)
			if err != nil {
				return fmt.Errorf("failed to construct kubelet dependencies: %w", err)
			}

			if utilfeature.DefaultFeatureGate.Enabled(zpagesfeatures.ComponentFlagz) {
				if cleanFlagSet != nil {
					namedFlagSet := map[string]*pflag.FlagSet{server.ComponentKubelet: cleanFlagSet}
					kubeletDeps.Flagz = flagz.NamedFlagSetsReader{FlagSets: cliflag.NamedFlagSets{FlagSets: namedFlagSet}}
				}
			}

			if err := checkPermissions(ctx); err != nil {
				logger.Error(err, "Kubelet running with insufficient permissions")
			}

			// 记录配置合并时跳过的 Drop-in 文件。
			if len(skippedDropinFiles) > 0 {
				for _, skippedFile := range skippedDropinFiles {
					logger.V(4).Info("Skipped file in drop-in directory (does not have .conf extension)", "file", skippedFile)
				}
			}

			// 从此处开始监听系统退出信号，所有长期运行组件共用这个取消上下文。
			ctx := genericapiserver.SetupSignalContext()

			utilfeature.DefaultMutableFeatureGate.AddMetrics()
			// 启动 Kubelet。
			return Run(ctx, kubeletServer, kubeletDeps, utilfeature.DefaultFeatureGate)
		},
	}

	// 独立维护 FlagSet，防止 Cobra 混入全局参数。
	kubeletFlags.AddFlags(cleanFlagSet)
	options.AddKubeletConfigFlags(cleanFlagSet, kubeletConfig)
	options.AddGlobalFlags(cleanFlagSet)
	cleanFlagSet.BoolP("help", "h", false, fmt.Sprintf("help for %s", cmd.Name()))

	// 必须自定义 Usage/Help，否则 Cobra 默认实现会把全局参数写入 FlagSet。
	const usageFmt = "Usage:\n  %s\n\nFlags:\n%s"
	cmd.SetUsageFunc(func(cmd *cobra.Command) error {
		fmt.Fprintf(cmd.OutOrStderr(), usageFmt, cmd.UseLine(), cleanFlagSet.FlagUsagesWrapped(2))
		return nil
	})
	cmd.SetHelpFunc(func(cmd *cobra.Command, args []string) {
		fmt.Fprintf(cmd.OutOrStdout(), "%s\n\n"+usageFmt, cmd.Long, cmd.UseLine(), cleanFlagSet.FlagUsagesWrapped(2))
	})

	return cmd
}

// mergeKubeletConfigurations 按文件名字典序合并目录及子目录下的 .conf 文件；后加载文件覆盖先前值。
// 例如 10-config.conf 先于 20-config.conf 应用，因此后者可以覆盖前者。
// 返回被跳过的文件列表和错误。
func mergeKubeletConfigurations(kubeletConfig *kubeletconfiginternal.KubeletConfiguration, kubeletDropInConfigDir string) ([]string, error) {
	const dropinFileExtension = ".conf"
	var skippedFiles []string

	// TODO: 将“internal → versioned → merge → default → internal”流程移入循环，
	// 并在配置文件支持 v1beta1 以外的版本后，以每个 Drop-in 文件的版本作为转换目标。
	// JSON Merge Patch 面向外部版本结构；先把内部配置转为 v1beta1，再逐个应用 Drop-in 补丁。
	versionedConfig := &kubeletconfigv1beta1.KubeletConfiguration{}
	if err := kubeletconfigv1beta1conversion.Convert_config_KubeletConfiguration_To_v1beta1_KubeletConfiguration(kubeletConfig, versionedConfig, nil); err != nil {
		return nil, err
	}

	baseKubeletConfigJSON, err := json.Marshal(versionedConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal base config: %w", err)
	}
	// WalkDir 递归遍历子目录；只有 .conf 文件参与合并，其余文件记录到 skippedFiles 供日志提示。
	if err := filepath.WalkDir(kubeletDropInConfigDir, func(path string, info fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if filepath.Ext(info.Name()) != dropinFileExtension {
			skippedFiles = append(skippedFiles, path)
			return nil
		}
		dropinConfigJSON, gvk, err := loadDropinConfigFileIntoJSON(path)
		if err != nil {
			return fmt.Errorf("failed to load kubelet dropin file, path: %s, error: %w", path, err)
		}
		if gvk == nil || gvk.Empty() {
			return fmt.Errorf("failed to load kubelet dropin file, path: %s, no apiVersion/kind", path)
		}
		// TODO: 配置文件支持多个版本后扩展此逻辑。
		if *gvk != kubeletconfigv1beta1.SchemeGroupVersion.WithKind("KubeletConfiguration") {
			return fmt.Errorf("failed to load kubelet dropin file, path: %s, unknown apiVersion/kind: %v", path, gvk.String())
		}
		// MergePatch 只覆盖补丁中出现的字段，未出现字段保留原值；因此加载顺序决定覆盖优先级。
		mergedConfigJSON, err := jsonpatch.MergePatch(baseKubeletConfigJSON, dropinConfigJSON)
		if err != nil {
			return fmt.Errorf("failed to merge drop-in and current config: %w", err)
		}
		baseKubeletConfigJSON = mergedConfigJSON
		return nil
	}); err != nil {
		return nil, fmt.Errorf("failed to walk through kubelet dropin directory %q: %w", kubeletDropInConfigDir, err)
	}

	// 重置版本化配置后重新解码。
	versionedConfig = &kubeletconfigv1beta1.KubeletConfiguration{}
	if err := json.Unmarshal(baseKubeletConfigJSON, versionedConfig); err != nil {
		return nil, fmt.Errorf("failed to unmarshal merged JSON into kubelet configuration: %w", err)
	}
	// 所有补丁合并后再填充默认值，避免中间文件的默认值影响后续覆盖。
	kubeletconfigv1beta1conversion.SetDefaults_KubeletConfiguration(versionedConfig)

	// 最后转回内部类型，后续 Kubelet 运行逻辑只需处理一套内部结构。
	if err := kubeletconfigv1beta1conversion.Convert_v1beta1_KubeletConfiguration_To_config_KubeletConfiguration(versionedConfig, kubeletConfig, nil); err != nil {
		return nil, fmt.Errorf("failed to convert merged config to internal kubelet configuration: %w", err)
	}

	return skippedFiles, nil
}

// newFlagSetWithGlobals 创建独立参数集合，并显式加入依赖库注册在 Go 全局 FlagSet 上的参数。
func newFlagSetWithGlobals() *pflag.FlagSet {
	fs := pflag.NewFlagSet("", pflag.ExitOnError)
	// 设置规范化函数，兼容 k8s.io/component-base/cli//flags.go:InitFlags 的行为。
	fs.SetNormalizeFunc(cliflag.WordSepNormalizeFunc)
	// 显式加入通过全局 FlagSet 注册参数的依赖库。
	options.AddGlobalFlags(fs)
	return fs
}

// newFakeFlagSet 复制参数名称和帮助信息，但把每个 Value 替换成 NoOp，供二次解析时避免副作用。
func newFakeFlagSet(fs *pflag.FlagSet) *pflag.FlagSet {
	ret := pflag.NewFlagSet("", pflag.ExitOnError)
	ret.SetNormalizeFunc(fs.GetNormalizeFunc())
	fs.VisitAll(func(f *pflag.Flag) {
		ret.VarP(cliflag.NoOp{}, f.Name, f.Shorthand, f.Usage)
	})
	return ret
}

// kubeletConfigFlagPrecedence 把原始命令行重新解析到配置文件对象，以维持“命令行优先于配置文件”的兼容约定。
// 该重解析行为用于保持二进制升级时的向后兼容；参见 issue #56171。
func kubeletConfigFlagPrecedence(kc *kubeletconfiginternal.KubeletConfiguration, args []string) error {
	// 使用临时 FlagSet 和 NoOp 全局参数，避免重复解析那些 Set 会累加值的 Flag 实现。
	fs := newFakeFlagSet(newFlagSetWithGlobals())
	// 注册仅用于二次解析的 KubeletFlags。
	options.NewKubeletFlags().AddFlags(fs)
	// 注册新的 KubeletConfiguration。
	options.AddKubeletConfigFlags(fs, kc)
	// 保存文件中原有的 Feature Gate；重新解析命令行后再补回没有被 Flag 指定的项。
	original := kc.FeatureGates
	// 避免重解析时重复打印参数弃用警告。
	fs.SetOutput(io.Discard)
	// 重新解析参数。
	if err := fs.Parse(args); err != nil {
		return err
	}
	// 只补回 Flag 没有显式覆盖的 Feature Gate，命令行中出现的值保留为最终值。
	for k, v := range original {
		if _, ok := kc.FeatureGates[k]; !ok {
			kc.FeatureGates[k] = v
		}
	}
	return nil
}

// loadConfigFile 将相对路径解析为绝对路径，再通过 Loader 解码为内部 KubeletConfiguration。
func loadConfigFile(ctx context.Context, name string) (*kubeletconfiginternal.KubeletConfiguration, error) {
	const errFmt = "failed to load Kubelet config file %s, error %v"
	// 根据当前工作目录计算绝对路径。
	kubeletConfigFile, err := filepath.Abs(name)
	if err != nil {
		return nil, fmt.Errorf(errFmt, name, err)
	}
	loader, err := configfiles.NewFsLoader(&utilfs.DefaultFs{}, kubeletConfigFile)
	if err != nil {
		return nil, fmt.Errorf(errFmt, name, err)
	}
	kc, err := loader.Load(ctx)
	if err != nil {
		return nil, fmt.Errorf(errFmt, name, err)
	}

	// EvictionHard 可按操作系统保留不同默认项；缺省时采用当前平台值，开启合并时只补缺失键。
	// 参见 https://github.com/kubernetes/kubernetes/pull/110263。
	if kc.EvictionHard == nil {
		kc.EvictionHard = eviction.DefaultEvictionHard
	} else if kc.MergeDefaultEvictionSettings {
		for k, v := range eviction.DefaultEvictionHard {
			if _, exists := kc.EvictionHard[k]; !exists {
				kc.EvictionHard[k] = v
			}
		}
	}
	return kc, err
}

// loadDropinConfigFileIntoJSON 读取单个 Drop-in 文件，返回 JSON 补丁及其 GVK，合并函数据此校验版本。
func loadDropinConfigFileIntoJSON(name string) ([]byte, *schema.GroupVersionKind, error) {
	const errFmt = "failed to load drop-in kubelet config file %s, error %v"
	// 根据当前工作目录计算绝对路径。
	kubeletConfigFile, err := filepath.Abs(name)
	if err != nil {
		return nil, nil, fmt.Errorf(errFmt, name, err)
	}
	loader, err := configfiles.NewFsLoader(&utilfs.DefaultFs{}, kubeletConfigFile)
	if err != nil {
		return nil, nil, fmt.Errorf(errFmt, name, err)
	}
	return loader.LoadIntoJSON()
}

// UnsecuredDependencies 构造不启动后台进程、也不包含认证/授权控制器的基础依赖集合；配置无效时返回错误。
func UnsecuredDependencies(ctx context.Context, s *options.KubeletServer, featureGate featuregate.FeatureGate) (*kubelet.Dependencies, error) {
	// 初始化 TLS 选项。
	tlsOptions, err := InitializeTLS(ctx, &s.KubeletFlags, &s.KubeletConfiguration)
	if err != nil {
		return nil, err
	}

	mounter := mount.New(s.ExperimentalMounterPath)
	subpather := subpath.New(mounter)
	hu := hostutil.NewHostUtil()
	pluginRunner := exec.New()

	// 卷插件在依赖构造阶段完成发现；后续 NewMainKubelet 会将它们交给卷管理子系统初始化。
	plugins, err := ProbeVolumePlugins(ctx, featureGate)
	if err != nil {
		return nil, err
	}
	tp, err := newTracerProvider(s)
	if err != nil {
		return nil, err
	}
	return &kubelet.Dependencies{
		Auth:                nil, // 默认不启用认证和授权。
		CAdvisorInterface:   nil, // cadvisor.New 会启动 HTTP 服务和清理协程，因此此处不注入。
		ContainerManager:    nil,
		KubeClient:          nil,
		HeartbeatClient:     nil,
		EventClient:         nil,
		TracerProvider:      tp,
		HostUtil:            hu,
		Mounter:             mounter,
		Subpather:           subpather,
		OOMAdjuster:         oom.NewOOMAdjuster(),
		OSInterface:         kubecontainer.RealOS{},
		VolumePlugins:       plugins,
		DynamicPluginProber: GetDynamicPluginProber(ctx, s.VolumePluginDir, pluginRunner),
		TLSOptions:          tlsOptions}, nil
}

// Run 启动指定配置的 Kubelet；通常只会在初始化失败或 Context 被取消时返回。
// kubeDeps 可为 nil，此时从 KubeletServer 的设置构造；否则沿用调用方已初始化的 Dependencies。
func Run(ctx context.Context, s *options.KubeletServer, kubeDeps *kubelet.Dependencies, featureGate featuregate.FeatureGate) error {
	logger := klog.FromContext(ctx)
	// 立即记录版本信息以便诊断。
	logger.Info("Kubelet version", "kubeletVersion", version.Get())

	logger.Info("Golang settings", "GOGC", os.Getenv("GOGC"), "GOMAXPROCS", os.Getenv("GOMAXPROCS"), "GOTRACEBACK", os.Getenv("GOTRACEBACK"))

	// 平台初始化必须早于通用运行流程，Windows 服务模式和优先级设置在对应构建文件中实现。
	if err := initForOS(ctx, s.WindowsService, s.WindowsPriorityClass); err != nil {
		return fmt.Errorf("failed OS init: %w", err)
	}
	if err := run(ctx, s, kubeDeps, featureGate); err != nil {
		return fmt.Errorf("failed to run Kubelet: %w", err)
	}
	return nil
}

// convertToVersionedKubeletConfig 将内部配置转为对外公开的 v1beta1 对象，供 /configz 和日志共享同一转换路径。
// 转换结果与 /configz 的表示一致，并写入 GroupVersionKind。
func convertToVersionedKubeletConfig(kc *kubeletconfiginternal.KubeletConfiguration) (*kubeletconfigv1beta1.KubeletConfiguration, error) {
	scheme, _, err := kubeletscheme.NewSchemeAndCodecs()
	if err != nil {
		return nil, err
	}
	versioned := &kubeletconfigv1beta1.KubeletConfiguration{}
	if err := scheme.Convert(kc, versioned, nil); err != nil {
		return nil, err
	}
	versioned.GetObjectKind().SetGroupVersionKind(kubeletconfigv1beta1.SchemeGroupVersion.WithKind("KubeletConfiguration"))
	return versioned, nil
}

// setConfigz 先执行内部类型到 API 版本类型的转换，再将最终生效配置注册到 configz。
func setConfigz(cz *configz.Config, kc *kubeletconfiginternal.KubeletConfiguration) error {
	versioned, err := convertToVersionedKubeletConfig(kc)
	if err != nil {
		return err
	}
	return cz.Set(versioned)
}

// marshalKubeletConfigForLog 将生效配置序列化为易读的 YAML，用于启动日志。
// 输出与 /configz 一致，但会脱敏 StaticPodURLHeader，而 /configz 会输出该字段。
func marshalKubeletConfigForLog(kc *kubeletconfiginternal.KubeletConfiguration) (string, error) {
	// 在不修改调用方副本的前提下脱敏，确保日志安全。
	safe := kc.DeepCopy()
	for k := range safe.StaticPodURLHeader {
		safe.StaticPodURLHeader[k] = []string{"<masked>"}
	}
	versioned, err := convertToVersionedKubeletConfig(safe)
	if err != nil {
		return "", err
	}
	data, err := yaml.Marshal(versioned)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// initConfigz 注册 kubeletconfig 配置快照，方便通过组件调试端点检查运行时实际使用的参数。
func initConfigz(ctx context.Context, kc *kubeletconfiginternal.KubeletConfiguration) error {
	logger := klog.FromContext(ctx)
	cz, err := configz.New("kubeletconfig")
	if err != nil {
		logger.Error(err, "Failed to register configz")
		return err
	}
	if err := setConfigz(cz, kc); err != nil {
		logger.Error(err, "Failed to register config")
		return err
	}
	return nil
}

// makeEventRecorder 仅在 Dependencies 尚未注入 Recorder 时创建事件广播器；已有 Recorder 时保持调用方配置。
func makeEventRecorder(ctx context.Context, kubeDeps *kubelet.Dependencies, nodeName types.NodeName) {
	if kubeDeps.Recorder != nil {
		return
	}
	logger := klog.FromContext(ctx)
	eventBroadcaster := record.NewBroadcaster(record.WithContext(ctx))
	kubeDeps.Recorder = eventBroadcaster.NewRecorder(legacyscheme.Scheme, v1.EventSource{Component: server.ComponentKubelet, Host: string(nodeName)})
	eventBroadcaster.StartStructuredLogging(3)
	if kubeDeps.EventClient != nil {
		logger.V(4).Info("Sending events to api server")
		eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: kubeDeps.EventClient.Events("")})
	} else {
		logger.Info("No api server defined - no events will be sent to API server")
	}
}

// getReservedCPUs 将 reserved-cpus 字符串解析为 CPUSet，并校验每个 CPU 都属于当前节点在线 CPU 拓扑。
func getReservedCPUs(logger klog.Logger, machineInfo *cadvisorapi.MachineInfo, cpus string) (cpuset.CPUSet, error) {
	emptyCPUSet := cpuset.New()

	if cpus == "" {
		return emptyCPUSet, nil
	}

	topo, err := topology.Discover(logger, machineInfo)
	if err != nil {
		return emptyCPUSet, fmt.Errorf("unable to discover CPU topology info: %s", err)
	}
	reservedCPUSet, err := cpuset.Parse(cpus)
	if err != nil {
		return emptyCPUSet, fmt.Errorf("unable to parse reserved-cpus list: %s", err)
	}
	allCPUSet := topo.CPUDetails.CPUs()
	if !reservedCPUSet.IsSubsetOf(allCPUSet) {
		return emptyCPUSet, fmt.Errorf("reserved-cpus: %s is not a subset of online-cpus: %s", cpus, allCPUSet.String())
	}
	return reservedCPUSet, nil
}

// run 是 Kubelet 初始化主流程：Feature Gate/校验 → 文件锁 → 客户端与认证 → CRI/cAdvisor → ContainerManager → RunKubelet。
func run(ctx context.Context, s *options.KubeletServer, kubeDeps *kubelet.Dependencies, featureGate featuregate.FeatureGate) (err error) {
	logger := klog.FromContext(ctx)
	// 看门狗配置错误时 NewHealthChecker 会返回错误，Kubelet 不启动。
	healthChecker, err := watchdog.NewHealthChecker(logger)
	if err != nil {
		return fmt.Errorf("create health checker: %w", err)
	}
	kubeDeps.HealthChecker = healthChecker
	healthChecker.Start(ctx)
	// 先应用组件配置中的 Feature Gate，因为后续配置校验和依赖创建会根据开关选择行为。
	err = utilfeature.DefaultMutableFeatureGate.SetFromMap(s.KubeletConfiguration.FeatureGates)
	if err != nil {
		return err
	}
	// 将 Feature Gate 状态同步到指标子系统；必须在设置开关后、注册直方图指标前调用。
	metricsfeatures.ApplyFeatureGates(featureGate)
	// 先校验初始 KubeletServer；校验依赖已设置的 Feature Gate。
	if err := options.ValidateKubeletServer(s); err != nil {
		return err
	}

	// 警告 MemoryQoS 兼容性问题。
	if utilfeature.DefaultFeatureGate.Enabled(features.MemoryQoS) {
		if !kubeletutil.IsCgroup2UnifiedMode() {
			logger.Info("Warning: MemoryQoS feature only works with cgroups v2 on Linux, but enabled with cgroups v1")
		} else {
			kernelVersion, err := utilkernel.GetVersion()
			if err != nil {
				logger.Error(err, "Failed to detect kernel version for MemoryQoS compatibility check")
			} else if kernelVersion.LessThan(utilversion.MustParseGeneric(utilkernel.MemoryQoSMinKernelVersion)) {
				logger.Info("Warning: MemoryQoS memory.high throttling may cause process livelock on older kernels",
					"currentKernel", kernelVersion,
					"minimumKernel", utilkernel.MemoryQoSMinKernelVersion)
			}
		}
	}
	// 文件锁用于保证同一节点不会并发运行多个 Kubelet；配置锁竞争退出时还会监视锁文件事件。
	if s.ExitOnLockContention && s.LockFilePath == "" {
		return errors.New("cannot exit on lock file contention: no lock file specified")
	}
	done := make(chan struct{})
	if s.LockFilePath != "" {
		logger.Info("Acquiring file lock", "path", s.LockFilePath)
		if err := flock.Acquire(s.LockFilePath); err != nil {
			return fmt.Errorf("unable to acquire file lock on %q: %w", s.LockFilePath, err)
		}
		if s.ExitOnLockContention {
			logger.Info("Watching for inotify events", "path", s.LockFilePath)
			if err := watchForLockfileContention(ctx, s.LockFilePath, done); err != nil {
				return err
			}
		}
	}

	if len(s.ShowHiddenMetricsForVersion) > 0 {
		metrics.SetShowHidden()
	}

	// 没有 kubeconfig 表示独立模式；此时不会创建 API、事件和心跳客户端。
	standaloneMode := true
	if len(s.KubeConfig) > 0 {
		standaloneMode = false
	}

	if kubeDeps == nil {
		kubeDeps, err = UnsecuredDependencies(ctx, s, featureGate)
		if err != nil {
			return err
		}
	}

	hostName, err := nodeutil.GetHostname(s.HostnameOverride)
	if err != nil {
		return err
	}
	nodeName := types.NodeName(hostName)

	// 独立模式显式清空所有 API 客户端，避免后续误把零值依赖当成集群模式使用。
	switch {
	case standaloneMode:
		kubeDeps.KubeClient = nil
		kubeDeps.EventClient = nil
		kubeDeps.HeartbeatClient = nil
		logger.Info("Standalone mode, no API client")

	case kubeDeps.KubeClient == nil, kubeDeps.EventClient == nil, kubeDeps.HeartbeatClient == nil:
		clientConfig, onHeartbeatFailure, err := buildKubeletClientConfig(ctx, s, kubeDeps.TracerProvider, nodeName)
		if err != nil {
			return err
		}
		if onHeartbeatFailure == nil {
			return errors.New("onHeartbeatFailure must be a valid function other than nil")
		}
		kubeDeps.OnHeartbeatFailure = onHeartbeatFailure

		kubeDeps.KubeClient, err = clientset.NewForConfig(clientConfig)
		if err != nil {
			return fmt.Errorf("failed to initialize kubelet client: %w", err)
		}

		// 事件客户端使用独立 QPS/Burst，防止事件突发影响 Pod、Node 等关键 API 请求。
		eventClientConfig := *clientConfig
		eventClientConfig.QPS = float32(s.EventRecordQPS)

		// EventRecordQPS 为 0 表示不限流。
		if int(eventClientConfig.QPS) == 0 {
			eventClientConfig.QPS = float32(-1)
		}

		eventClientConfig.Burst = int(s.EventBurst)
		kubeDeps.EventClient, err = v1core.NewForConfig(&eventClientConfig)
		if err != nil {
			return fmt.Errorf("failed to initialize kubelet event client: %w", err)
		}

		// 心跳客户端关闭客户端侧限流，并把超时限制在 Node 状态更新周期与 Lease 时长之内。
		heartbeatClientConfig := *clientConfig
		heartbeatClientConfig.Timeout = s.KubeletConfiguration.NodeStatusUpdateFrequency.Duration
		// 超时取 Lease 时长与状态更新频率中的较小值。
		leaseTimeout := time.Duration(s.KubeletConfiguration.NodeLeaseDurationSeconds) * time.Second
		if heartbeatClientConfig.Timeout > leaseTimeout {
			heartbeatClientConfig.Timeout = leaseTimeout
		}

		heartbeatClientConfig.QPS = float32(-1)
		kubeDeps.HeartbeatClient, err = clientset.NewForConfig(&heartbeatClientConfig)
		if err != nil {
			return fmt.Errorf("failed to initialize kubelet heartbeat client: %w", err)
		}
	}

	// 调用方未注入认证依赖时，根据配置创建认证/授权器，并启动客户端 CA 热加载。
	if kubeDeps.Auth == nil {
		auth, runAuthenticatorCAReload, err := BuildAuth(nodeName, kubeDeps.KubeClient, s.KubeletConfiguration)
		if err != nil {
			return err
		}
		kubeDeps.Auth = auth
		runAuthenticatorCAReload(ctx.Done())
	}

	if err := kubelet.PreInitRuntimeService(ctx, &s.KubeletConfiguration, kubeDeps); err != nil {
		return err
	}

	if utilfeature.DefaultFeatureGate.Enabled(features.PodAndContainerStatsFromCRI) && !cadvisor.UsingLegacyCadvisorStats(s.ContainerRuntimeEndpoint) {
		_, err := kubeDeps.RemoteRuntimeService.PodSandboxStats(ctx, "")
		if err != nil {
			s, ok := status.FromError(err)
			if ok && s.Code() == codes.Unimplemented {
				logger.Info("CRI implementation does not support PodSandboxStats, falling back to cadvisor stats")
				kubeDeps.PodSandboxStatsUnimplemented = true
			}
		}
	}

	// 读取 CRI 的 cgroup driver，确保 Kubelet 与容器运行时操作同一套 cgroup 层级。
	if err := getCgroupDriverFromCRI(ctx, s, kubeDeps); err != nil {
		return err
	}

	// 注册最终生效配置供 /configz 调试；注册失败记录错误，但不阻止节点代理启动。
	err = initConfigz(ctx, &s.KubeletConfiguration)
	if err != nil {
		logger.Error(err, "Failed to register kubelet configuration with configz")
	}

	var cgroupRoots []string
	nodeAllocatableRoot := cm.NodeAllocatableRoot(s.CgroupRoot, s.CgroupsPerQOS, s.CgroupDriver)
	cgroupRoots = append(cgroupRoots, nodeAllocatableRoot)
	kubeletCgroup, err := cm.GetKubeletContainer(logger, s.KubeletCgroups)
	if err != nil {
		logger.Info("Failed to get the kubelet's cgroup. Kubelet system container metrics may be missing", "err", err)
	} else if kubeletCgroup != "" {
		cgroupRoots = append(cgroupRoots, kubeletCgroup)
	}

	if s.RuntimeCgroups != "" {
		// RuntimeCgroups 为可选项，未指定时忽略。
		cgroupRoots = append(cgroupRoots, s.RuntimeCgroups)
	}

	if s.SystemCgroups != "" {
		// SystemCgroups 为可选项，未指定时忽略。
		cgroupRoots = append(cgroupRoots, s.SystemCgroups)
	}

	// 仅在调用方没有注入 cAdvisor 时创建，并依据 CRI 统计能力决定是否关闭容器发现。
	if kubeDeps.CAdvisorInterface == nil {
		imageFsInfoProvider := cadvisor.NewImageFsInfoProvider(s.ContainerRuntimeEndpoint)
		disableContainerDiscovery := utilfeature.DefaultFeatureGate.Enabled(features.PodAndContainerStatsFromCRI) && !kubeDeps.PodSandboxStatsUnimplemented
		kubeDeps.CAdvisorInterface, err = cadvisor.New(klog.FromContext(ctx), imageFsInfoProvider, s.RootDirectory, cgroupRoots, cadvisor.UsingLegacyCadvisorStats(s.ContainerRuntimeEndpoint), s.LocalStorageCapacityIsolation, disableContainerDiscovery)
		if err != nil {
			return err
		}
	}

	// 按需初始化事件记录器。
	makeEventRecorder(ctx, kubeDeps, nodeName)

	// ContainerManager 负责 QoS/cgroup、CPU/内存拓扑和 Node Allocatable 等节点资源管理。
	if kubeDeps.ContainerManager == nil {
		if s.CgroupsPerQOS && s.CgroupRoot == "" {
			logger.Info("--cgroups-per-qos enabled, but --cgroup-root was not specified.  Defaulting to /")
			s.CgroupRoot = "/"
		}

		machineInfo, err := kubeDeps.CAdvisorInterface.MachineInfo(logger)
		if err != nil {
			return err
		}
		reservedSystemCPUs, err := getReservedCPUs(logger, machineInfo, s.ReservedSystemCPUs)
		if err != nil {
			return err
		}
		if reservedSystemCPUs.Size() > 0 {
			// 参数校验阶段已确认至少指定了 --system-reserved-cgroup 或 --kube-reserved-cgroup，因此可直接覆盖。
			logger.Info("Option --reserved-cpus is specified, it will overwrite the cpu setting in KubeReserved and SystemReserved", "kubeReserved", s.KubeReserved, "systemReserved", s.SystemReserved)
			if s.KubeReserved != nil {
				delete(s.KubeReserved, "cpu")
			}
			if s.SystemReserved == nil {
				s.SystemReserved = make(map[string]string)
			}
			s.SystemReserved["cpu"] = strconv.Itoa(reservedSystemCPUs.Size())
			logger.Info("After cpu setting is overwritten", "kubeReserved", s.KubeReserved, "systemReserved", s.SystemReserved)
		}

		kubeReserved, err := parseResourceList(s.KubeReserved)
		if err != nil {
			return fmt.Errorf("--kube-reserved value failed to parse: %w", err)
		}
		systemReserved, err := parseResourceList(s.SystemReserved)
		if err != nil {
			return fmt.Errorf("--system-reserved value failed to parse: %w", err)
		}

		var hardEvictionThresholds []evictionapi.Threshold
		// 用户要求忽略驱逐阈值时，不为此处硬驱逐阈值设置有效值。
		if !s.ExperimentalNodeAllocatableIgnoreEvictionThreshold {
			hardEvictionThresholds, err = eviction.ParseThresholdConfig([]string{}, s.EvictionHard, nil, nil, nil)
			if err != nil {
				return err
			}
		}
		experimentalQOSReserved, err := cm.ParseQOSReserved(s.QOSReserved)
		if err != nil {
			return fmt.Errorf("--qos-reserved value failed to parse: %w", err)
		}

		var topologyManagerPolicyOptions map[string]string
		if utilfeature.DefaultFeatureGate.Enabled(features.TopologyManagerPolicyOptions) {
			topologyManagerPolicyOptions = s.TopologyManagerPolicyOptions
		} else if s.TopologyManagerPolicyOptions != nil {
			return fmt.Errorf("topology manager policy options %v require feature gates %q enabled",
				s.TopologyManagerPolicyOptions, features.TopologyManagerPolicyOptions)
		}
		if utilfeature.DefaultFeatureGate.Enabled(features.NodeSwap) {
			if !kubeletutil.IsCgroup2UnifiedMode() && s.MemorySwap.SwapBehavior == string(kubelettypes.LimitedSwap) {
				// cgroup v1 不支持该特性，因此提前失败。
				return fmt.Errorf("swap feature is enabled and LimitedSwap but it is only supported with cgroupv2")
			}
			if !s.FailSwapOn && s.MemorySwap.SwapBehavior == "" {
				// 当前默认使用 NoSwap，此处仅记录日志。
				logger.Info("NoSwap is set due to memorySwapBehavior not specified", "memorySwapBehavior", s.MemorySwap.SwapBehavior, "FailSwapOn", s.FailSwapOn)
			}
		}

		kubeDeps.ContainerManager, err = cm.NewContainerManager(
			ctx,
			kubeDeps.Mounter,
			kubeDeps.CAdvisorInterface,
			cm.NodeConfig{
				NodeName:              nodeName,
				RuntimeCgroupsName:    s.RuntimeCgroups,
				SystemCgroupsName:     s.SystemCgroups,
				KubeletCgroupsName:    s.KubeletCgroups,
				KubeletOOMScoreAdj:    s.OOMScoreAdj,
				CgroupsPerQOS:         s.CgroupsPerQOS,
				CgroupRoot:            s.CgroupRoot,
				CgroupDriver:          s.CgroupDriver,
				KubeletRootDir:        s.RootDirectory,
				ProtectKernelDefaults: s.ProtectKernelDefaults,
				NodeAllocatableConfig: cm.NodeAllocatableConfig{
					KubeReservedCgroupName:   s.KubeReservedCgroup,
					SystemReservedCgroupName: s.SystemReservedCgroup,
					EnforceNodeAllocatable:   sets.New(s.EnforceNodeAllocatable...),
					KubeReserved:             kubeReserved,
					SystemReserved:           systemReserved,
					ReservedSystemCPUs:       reservedSystemCPUs,
					HardEvictionThresholds:   hardEvictionThresholds,
				},
				QOSReserved:                  *experimentalQOSReserved,
				CPUManagerPolicy:             s.CPUManagerPolicy,
				CPUManagerPolicyOptions:      s.CPUManagerPolicyOptions,
				CPUManagerReconcilePeriod:    s.CPUManagerReconcilePeriod.Duration,
				MemoryManagerPolicy:          s.MemoryManagerPolicy,
				MemoryManagerReservedMemory:  s.ReservedMemory,
				MemoryReservationPolicy:      s.MemoryReservationPolicy,
				MemoryThrottlingFactor:       s.MemoryThrottlingFactor,
				PodPidsLimit:                 s.PodPidsLimit,
				EnforceCPULimits:             s.CPUCFSQuota,
				CPUCFSQuotaPeriod:            s.CPUCFSQuotaPeriod.Duration,
				TopologyManagerPolicy:        s.TopologyManagerPolicy,
				TopologyManagerScope:         s.TopologyManagerScope,
				TopologyManagerPolicyOptions: topologyManagerPolicyOptions,
			},
			s.FailSwapOn,
			kubeDeps.Recorder,
			kubeDeps.KubeClient,
		)

		if err != nil {
			return err
		}
	}

	if kubeDeps.PodStartupLatencyTracker == nil {
		kubeDeps.PodStartupLatencyTracker = kubeletutil.NewPodStartupLatencyTracker()
	}

	if kubeDeps.NodeStartupLatencyTracker == nil {
		kubeDeps.NodeStartupLatencyTracker = kubeletutil.NewNodeStartupLatencyTracker(logger)
	}

	// TODO(vmarmol): 改为通过容器配置完成。
	oomAdjuster := kubeDeps.OOMAdjuster
	if err := oomAdjuster.ApplyOOMScoreAdj(0, int(s.OOMScoreAdj)); err != nil {
		logger.Info("Failed to ApplyOOMScoreAdj", "err", err)
	}

	if err := RunKubelet(ctx, s, kubeDeps); err != nil {
		return err
	}

	if s.HealthzPort > 0 {
		mux := http.NewServeMux()
		healthz.InstallHandler(mux)
		go wait.UntilWithContext(ctx, func(ctx context.Context) {
			err := http.ListenAndServe(net.JoinHostPort(s.HealthzBindAddress, strconv.Itoa(int(s.HealthzPort))), mux)
			if err != nil {
				logger.Error(err, "Failed to start healthz server")
			}
		}, 5*time.Second)
	}

	// 使用 systemd 时通知其 Kubelet 已启动。
	go daemon.SdNotify(false, "READY=1")

	select {
	case <-done:
		break
	case <-ctx.Done():
		break
	}

	return nil
}

// buildKubeletClientConfig 处理 kubeconfig、BootstrapKubeconfig 和客户端证书轮换，返回 API 客户端配置及心跳失败后的连接清理回调。
// 具体路径取决于是否启用证书引导或客户端证书轮换。
func buildKubeletClientConfig(ctx context.Context, s *options.KubeletServer, tp oteltrace.TracerProvider, nodeName types.NodeName) (*restclient.Config, func(), error) {
	logger := klog.FromContext(ctx)
	// 开启客户端证书轮换后，先准备证书申请凭据和由证书管理器维护的客户端配置。
	if s.RotateCertificates {
		// 证书轮换与 kubeconfig 处理遵循以下边界：
		//
		// 1. 客户端只提供 kubeconfig 时，将其作为初始客户端配置；kubeadm 需要把其中的初始数据写入证书存储。
		// 2. 客户端只提供初始 bootstrap kubeconfig 时，在目标位置创建指向证书存储的 kubeconfig；
		//    文件生成前，客户端配置不包含证书。
		// 3. 客户端同时提供两者且 kubeconfig 有效时，忽略 bootstrap kubeconfig。
		// 4. 客户端同时提供两者但 kubeconfig 已过期或无效时，用指向证书目录的新文件替换 kubeconfig。
		//
		// 引导使用 bootstrap kubeconfig，并由当前进程管理 kubeconfig。
		// 为兼容 kubeadm 在 master 上提供含证书和私钥的 kubeconfig，必须用初始客户端配置引导证书管理器。
		logger.Info("Client rotation is on, will bootstrap in background")
		// certConfig 用于申请/轮换证书，clientConfig 最终指向证书存储；二者可能使用不同凭据。
		certConfig, clientConfig, err := bootstrap.LoadClientConfig(logger, s.KubeConfig, s.BootstrapKubeconfig, s.CertDirectory)
		if err != nil {
			return nil, nil, err
		}

		// 使用证书轮换所需的 Content-Type，但不设置 QPS。
		setContentTypeForClient(certConfig, s.ContentType)

		kubeClientConfigOverrides(s, clientConfig)

		// 证书管理器负责检查当前客户端证书、提交 CSR 并把签发证书写入证书目录。
		clientCertificateManager, err := buildClientCertificateManager(certConfig, clientConfig, s.CertDirectory, nodeName)
		if err != nil {
			return nil, nil, err
		}

		legacyregistry.RawMustRegister(metrics.NewGaugeFunc(
			&metrics.GaugeOpts{
				Subsystem: kubeletmetrics.KubeletSubsystem,
				Name:      "certificate_manager_client_ttl_seconds",
				Help: "Gauge of the TTL (time-to-live) of the Kubelet's client certificate. " +
					"The value is in seconds until certificate expiry (negative if already expired). " +
					"If client certificate is invalid or unused, the value will be +INF.",
				StabilityLevel: metrics.ALPHA,
			},
			func() float64 {
				if c := clientCertificateManager.Current(); c != nil && c.Leaf != nil {
					return math.Trunc(time.Until(c.Leaf.NotAfter).Seconds())
				}
				return math.Inf(1)
			},
		))

		// 轮换 Transport 从证书管理器实时获取证书，因此底层匿名配置不直接携带静态证书文件。
		transportConfig := restclient.AnonymousClientConfig(clientConfig)

		// UpdateTransport 安装动态证书 Transport；连续五分钟无法申请新证书时退出，让外部引导机制有机会修复凭据。
		closeAllConns, err := kubeletcertificate.UpdateTransport(logger, wait.NeverStop, transportConfig, clientCertificateManager, 5*time.Minute)
		if err != nil {
			return nil, nil, err
		}
		var onHeartbeatFailure func()
		// Kubelet 需要从失效 HTTP 连接中恢复。HTTP/2 通过定期 ping 检测断连；
		// HTTP/1 只有一条持久连接，心跳失败后会关闭空闲连接。由于仍存在无法控制的边界情况，
		// 用户可通过 DISABLE_HTTP2 环境变量选择恢复旧的连接关闭行为。
		if s := os.Getenv("DISABLE_HTTP2"); len(s) > 0 {
			logger.Info("HTTP2 has been explicitly disabled, Kubelet will forcefully close active connections on heartbeat failures")
			onHeartbeatFailure = closeAllConns
		} else {
			onHeartbeatFailure = func() { utilnet.CloseIdleConnectionsFor(transportConfig.Transport) }
		}

		logger.V(2).Info("Starting client certificate rotation")
		clientCertificateManager.Start()

		return transportConfig, onHeartbeatFailure, nil
	}

	// 未启用持续轮换但提供了 BootstrapKubeconfig 时，先用引导凭据获取正式客户端证书。
	if len(s.BootstrapKubeconfig) > 0 {
		if err := bootstrap.LoadClientCert(ctx, s.KubeConfig, s.BootstrapKubeconfig, s.CertDirectory, nodeName); err != nil {
			return nil, nil, err
		}
	}

	// 读取最终 kubeconfig；NonInteractive 模式不会尝试打开交互式凭据输入。
	clientConfig, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		&clientcmd.ClientConfigLoadingRules{ExplicitPath: s.KubeConfig},
		&clientcmd.ConfigOverrides{},
	).ClientConfig()
	if err != nil {
		return nil, nil, fmt.Errorf("invalid kubeconfig: %w", err)
	}

	kubeClientConfigOverrides(s, clientConfig)
	// Kubelet 需要从失效 HTTP 连接中恢复。HTTP/2 通过定期 ping 检测断连；
	// HTTP/1 只有一条持久连接，心跳失败后会关闭空闲连接。由于仍存在无法控制的边界情况，
	// 用户可通过 DISABLE_HTTP2 环境变量选择恢复旧的连接关闭行为。
	var onHeartbeatFailure func()
	if s := os.Getenv("DISABLE_HTTP2"); len(s) > 0 {
		logger.Info("HTTP2 has been explicitly disabled, updating Kubelet client Dialer to forcefully close active connections on heartbeat failures")
		onHeartbeatFailure, err = updateDialer(clientConfig)
		if err != nil {
			return nil, nil, err
		}
	} else {
		onHeartbeatFailure = func() {
			utilnet.CloseIdleConnectionsFor(clientConfig.Transport)
		}
	}
	// 为后续 REST 请求注入 OpenTelemetry Transport 包装器，记录 API 调用链路。
	clientConfig.Wrap(tracing.WrapperFor(tp))
	return clientConfig, onHeartbeatFailure, nil
}

// updateDialer 为 REST Client 安装可轮换连接的 Dialer；返回的回调可在心跳失败时强制关闭旧连接。
func updateDialer(clientConfig *restclient.Config) (func(), error) {
	if clientConfig.Transport != nil || clientConfig.Dial != nil {
		return nil, fmt.Errorf("there is already a transport or dialer configured")
	}
	d := connrotation.NewDialer((&net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}).DialContext)
	clientConfig.Dial = d.DialContext
	return d.CloseAll, nil
}

// buildClientCertificateManager 创建客户端证书生命周期管理器：首次申请使用引导凭据，已有证书时使用正式客户端配置。
func buildClientCertificateManager(certConfig, clientConfig *restclient.Config, certDir string, nodeName types.NodeName) (certificate.Manager, error) {
	// 申请首张证书时使用 bootstrap 凭据；证书签发后切换为正式 kubeconfig 中的身份。
	newClientsetFn := func(current *tls.Certificate) (clientset.Interface, error) {
		// 已有有效证书时用它提交 CSR，否则使用引导凭据。
		// 未来可由云 VM 身份或 HSM 等基础信任系统始终提供外部引导凭据。
		config := certConfig
		if current != nil {
			config = clientConfig
		}
		return clientset.NewForConfig(config)
	}

	return kubeletcertificate.NewKubeletClientCertificateManager(
		certDir,
		nodeName,

		// 保持与 kubeadm 的兼容：它通过 --kubeconfig 传入有效证书，并期望该证书立即被轮换。
		clientConfig.CertData,
		clientConfig.KeyData,

		clientConfig.CertFile,
		clientConfig.KeyFile,
		newClientsetFn,
	)
}

// kubeClientConfigOverrides 将 Kubelet 的 API Content-Type、QPS 和 Burst 参数覆盖到 kubeconfig 生成的 REST 配置上。
func kubeClientConfigOverrides(s *options.KubeletServer, clientConfig *restclient.Config) {
	setContentTypeForClient(clientConfig, s.ContentType)
	// 用参数覆盖 kubeconfig 中的 QPS/Burst 设置。
	clientConfig.QPS = float32(s.KubeAPIQPS)
	clientConfig.Burst = int(s.KubeAPIBurst)
}

// InitializeTLS 检查 HTTPS 服务端证书配置；未指定且未启用证书引导时生成并保存自签名证书。
// 返回已配置的 server.TLSOptions。
func InitializeTLS(ctx context.Context, kf *options.KubeletFlags, kc *kubeletconfiginternal.KubeletConfiguration) (*server.TLSOptions, error) {
	logger := klog.FromContext(ctx)
	// 未启用 API Server 证书引导且用户未提供证书时，使用节点证书目录中的默认文件路径。
	if !kc.ServerTLSBootstrap && kc.TLSCertFile == "" && kc.TLSPrivateKeyFile == "" {
		kc.TLSCertFile = filepath.Join(kf.CertDirectory, "kubelet.crt")
		kc.TLSPrivateKeyFile = filepath.Join(kf.CertDirectory, "kubelet.key")

		canReadCertAndKey, err := certutil.CanReadCertAndKey(kc.TLSCertFile, kc.TLSPrivateKeyFile)
		if err != nil {
			return nil, err
		}
		// 证书或私钥缺失时生成一对自签名证书；已有文件则复用，避免每次重启改变服务端身份。
		if !canReadCertAndKey {
			hostName, err := nodeutil.GetHostname(kf.HostnameOverride)
			if err != nil {
				return nil, err
			}
			cert, key, err := certutil.GenerateSelfSignedCertKey(hostName, nil, nil)
			if err != nil {
				return nil, fmt.Errorf("unable to generate self signed cert: %w", err)
			}

			if err := certutil.WriteCert(kc.TLSCertFile, cert); err != nil {
				return nil, err
			}

			if err := keyutil.WriteKey(kc.TLSPrivateKeyFile, key); err != nil {
				return nil, err
			}

			logger.V(4).Info("Using self-signed cert", "TLSCertFile", kc.TLSCertFile, "TLSPrivateKeyFile", kc.TLSPrivateKeyFile)
		}
	}

	tlsCipherSuites, err := cliflag.TLSCipherSuites(kc.TLSCipherSuites)
	if err != nil {
		return nil, err
	}

	if len(tlsCipherSuites) > 0 {
		insecureCiphers := cliflag.InsecureTLSCiphers()
		for i := range tlsCipherSuites {
			for cipherName, cipherID := range insecureCiphers {
				if tlsCipherSuites[i] == cipherID {
					logger.Info("Use of insecure cipher detected.", "cipher", cipherName)
				}
			}
		}
	}

	// TLS 最低版本、套件和曲线偏好统一解析为 crypto/tls 使用的数值常量。
	minTLSVersion, err := cliflag.TLSVersion(kc.TLSMinVersion)
	if err != nil {
		return nil, err
	}

	if minTLSVersion == tls.VersionTLS13 {
		if len(tlsCipherSuites) != 0 {
			logger.Info("Warning: TLS 1.3 cipher suites are not configurable, ignoring --tls-cipher-suites")
		}
	}

	curvePreferences, err := cliflag.TLSCurvePreferences(kc.TLSCurvePreferences)
	if err != nil {
		return nil, err
	}

	tlsOptions := &server.TLSOptions{
		MinVersion:       minTLSVersion,
		CipherSuites:     tlsCipherSuites,
		CurvePreferences: curvePreferences,
		CertFile:         kc.TLSCertFile,
		KeyFile:          kc.TLSPrivateKeyFile,
		ClientCAFile:     kc.Authentication.X509.ClientCAFile,
	}

	return tlsOptions, nil
}

// setContentTypeForClient 设置请求 Content-Type；若使用 Protobuf，同时允许 JSON 响应以兼容 API Server。
func setContentTypeForClient(cfg *restclient.Config, contentType string) {
	if len(contentType) == 0 {
		return
	}
	cfg.ContentType = contentType
	switch contentType {
	case runtime.ContentTypeProtobuf:
		cfg.AcceptContentTypes = strings.Join([]string{runtime.ContentTypeProtobuf, runtime.ContentTypeJSON}, ",")
	default:
		// 其他情况交由 REST Client 执行默认值处理。
	}
}

// RunKubelet 根据节点身份和本地配置创建 MainKubelet，检查 Pod 配置源后启动工作循环与端点。
// 供集成测试、Kubelet 二进制和独立 kubernetes 二进制使用；未来后两者可能统一。
func RunKubelet(ctx context.Context, kubeServer *options.KubeletServer, kubeDeps *kubelet.Dependencies) error {
	logger := klog.FromContext(ctx)
	hostname, err := nodeutil.GetHostname(kubeServer.HostnameOverride)
	if err != nil {
		return err
	}
	nodeName := types.NodeName(hostname)
	// 按需初始化事件记录器。
	makeEventRecorder(ctx, kubeDeps, nodeName)

	nodeIPs, invalidNodeIps, err := nodeutil.ParseNodeIPArgument(kubeServer.NodeIP, kubeServer.CloudProvider)
	if err != nil {
		return fmt.Errorf("bad --node-ip %q: %v", kubeServer.NodeIP, err)
	}
	if len(invalidNodeIps) != 0 {
		logger.Info("Could not parse some node IP(s), ignoring them", "IPs", invalidNodeIps)
	}

	capabilities.Initialize(capabilities.Capabilities{
		AllowPrivileged: true,
	})

	credentialprovider.SetPreferredDockercfgPath(kubeServer.RootDirectory)
	logger.V(2).Info("Using root directory", "path", kubeServer.RootDirectory)

	if kubeDeps.OSInterface == nil {
		kubeDeps.OSInterface = kubecontainer.RealOS{}
	}

	// MainKubelet 装配 Pod 管理、运行时、卷、探针和状态管理等核心子系统。
	k, err := createAndInitKubelet(ctx,
		kubeServer,
		kubeDeps,
		hostname,
		nodeName,
		nodeIPs)
	if err != nil {
		return fmt.Errorf("failed to create kubelet: %w", err)
	}

	// NewMainKubelet 应保证 PodConfig 已创建；此处再次检查，避免启动 worker 时缺少更新源。
	if kubeDeps.PodConfig == nil {
		return fmt.Errorf("failed to create kubelet, pod source config was nil")
	}
	podCfg := kubeDeps.PodConfig

	if err := rlimit.SetNumFiles(uint64(kubeServer.MaxOpenFiles)); err != nil {
		logger.Error(err, "Failed to set rlimit on max file handles")
	}

	startKubelet(ctx, k, podCfg, &kubeServer.KubeletConfiguration, kubeDeps, kubeServer.EnableServer)
	logger.Info("Started kubelet")

	return nil
}

// startKubelet 并发启动 Pod 同步主循环、可选 HTTPS/只读服务以及 PodResources 和 Pods 辅助端点。
func startKubelet(ctx context.Context, k kubelet.Bootstrap, podCfg *config.PodConfig, kubeCfg *kubeletconfiginternal.KubeletConfiguration, kubeDeps *kubelet.Dependencies, enableServer bool) {
	// 主循环消费 API Server、静态文件和 HTTP 等来源汇总后的 Pod 更新。
	go k.Run(ctx, podCfg.Updates())

	// 启动 Kubelet 服务。
	if enableServer {
		go k.ListenAndServe(ctx, kubeCfg, kubeDeps.TLSConfig, kubeDeps.Auth, kubeDeps.TracerProvider)
	}
	if kubeCfg.ReadOnlyPort > 0 {
		go k.ListenAndServeReadOnly(ctx, netutils.ParseIPSloppy(kubeCfg.Address), uint(kubeCfg.ReadOnlyPort), kubeDeps.TracerProvider)
	}

	go k.ListenAndServePodResources(ctx)
	go k.ListenAndServePods(ctx)
}

// createAndInitKubelet 创建 MainKubelet 并启动启动事件和垃圾回收；具体模块依赖由 Dependencies 注入。
func createAndInitKubelet(
	ctx context.Context,
	kubeServer *options.KubeletServer,
	kubeDeps *kubelet.Dependencies,
	hostname string,
	nodeName types.NodeName,
	nodeIPs []net.IP) (k kubelet.Bootstrap, err error) {
	// TODO: 阻塞等待所有配置源至少向通道发送一次更新，或将同步循环拆分为按配置源分别同步。

	k, err = kubelet.NewMainKubelet(
		ctx,
		&kubeServer.KubeletConfiguration,
		kubeDeps,
		&kubeServer.ContainerRuntimeOptions,
		hostname,
		nodeName,
		nodeIPs,
		kubeServer.ProviderID,
		kubeServer.CloudProvider,
		kubeServer.CertDirectory,
		kubeServer.RootDirectory,
		kubeServer.PodLogsDir,
		kubeServer.ImageCredentialProviderConfigPath,
		kubeServer.ImageCredentialProviderBinDir,
		kubeServer.RegisterNode,
		kubeServer.RegisterWithTaints,
		kubeServer.AllowedUnsafeSysctls,
		kubeServer.ExperimentalMounterPath,
		kubeServer.KernelMemcgNotification,
		kubeServer.ExperimentalNodeAllocatableIgnoreEvictionThreshold,
		kubeServer.MinimumGCAge,
		kubeServer.MaxPerPodContainerCount,
		kubeServer.MaxContainerCount,
		kubeServer.NodeLabels,
		kubeServer.NodeStatusMaxImages,
		kubeServer.KubeletFlags.SeccompDefault || kubeServer.KubeletConfiguration.SeccompDefault)
	if err != nil {
		return nil, err
	}

	k.BirthCry()

	k.StartGarbageCollection(ctx)

	return k, nil
}

// parseResourceList 将资源字符串转换为 API ResourceList，并拒绝未知资源和负数。
func parseResourceList(m map[string]string) (v1.ResourceList, error) {
	if len(m) == 0 {
		return nil, nil
	}
	rl := make(v1.ResourceList)
	for k, v := range m {
		switch v1.ResourceName(k) {
		// 支持 CPU、内存、本地存储和 PID 资源。
		case v1.ResourceCPU, v1.ResourceMemory, v1.ResourceEphemeralStorage, pidlimit.PIDs:
			q, err := resource.ParseQuantity(v)
			if err != nil {
				return nil, fmt.Errorf("failed to parse quantity %q for %q resource: %w", v, k, err)
			}
			if q.Sign() == -1 {
				return nil, fmt.Errorf("resource quantity for %q cannot be negative: %v", k, v)
			}
			if v1.ResourceName(k) == v1.ResourceCPU {
				q.SetMilli((q.ScaledValue(resource.Micro) + 500) / 1000)
			}
			rl[v1.ResourceName(k)] = q
		default:
			return nil, fmt.Errorf("cannot reserve %q resource", k)
		}
	}
	return rl, nil
}

// newTracerProvider 按 tracing 配置创建 OTLP TracerProvider；未配置时返回 No-op 实现，避免调用方判断 nil。
func newTracerProvider(s *options.KubeletServer) (oteltrace.TracerProvider, error) {
	if s.KubeletConfiguration.Tracing == nil {
		return noopoteltrace.NewTracerProvider(), nil
	}
	hostname, err := nodeutil.GetHostname(s.HostnameOverride)
	if err != nil {
		return nil, fmt.Errorf("could not determine hostname for tracer provider: %w", err)
	}
	resourceOpts := []otelsdkresource.Option{
		otelsdkresource.WithAttributes(
			semconv.ServiceNameKey.String(server.ComponentKubelet),
			semconv.HostNameKey.String(hostname),
		),
	}
	tp, err := tracing.NewProvider(context.Background(), s.KubeletConfiguration.Tracing, []otlptracegrpc.Option{}, resourceOpts)
	if err != nil {
		return nil, fmt.Errorf("could not configure tracer provider: %w", err)
	}
	return tp, nil
}

// getCgroupDriverFromCRI 通过 CRI RuntimeConfig 获取容器运行时的 cgroup driver，并据此完成 Kubelet 配置一致性检查。
func getCgroupDriverFromCRI(ctx context.Context, s *options.KubeletServer, kubeDeps *kubelet.Dependencies) error {
	logger := klog.FromContext(ctx)
	logger.V(4).Info("Getting CRI runtime configuration information")

	var (
		runtimeConfig *runtimeapi.RuntimeConfigResponse
		err           error
	)
	// 重试数次，以应对瞬时错误。
	// 已知的非瞬时错误应快速返回。
	for range 3 {
		runtimeConfig, err = kubeDeps.RemoteRuntimeService.RuntimeConfig(ctx)
		if err != nil {
			s, ok := status.FromError(err)
			if !ok || s.Code() != codes.Unimplemented {
				// 可以引入退避延迟或随机抖动，但这里主要处理运行时仍在启动、请求过早的情况。
				// 稍等片刻再重试。
				time.Sleep(time.Second * 2)
				continue
			}
			// CRI 实现不支持 RuntimeConfig 时，回退使用 Kubelet 配置中的 cgroupDriver。
			legacyregistry.MustRegister(kubeletmetrics.CRILosingSupport)
			kubeletmetrics.CRILosingSupport.WithLabelValues("1.38.0").Inc()
			logger.Info("CRI implementation should be updated to support RuntimeConfig. Falling back to using cgroupDriver from kubelet config.")
			return nil
		}
	}
	if err != nil {
		return err
	}

	// 调用 GetLinux().GetCgroupDriver() 不会导致 panic，但字段未填充时它总会默认返回 systemd，
	// 这并不是字段缺省时的预期行为。
	linuxConfig := runtimeConfig.GetLinux()
	if linuxConfig == nil {
		return nil
	}

	switch d := linuxConfig.GetCgroupDriver(); d {
	case runtimeapi.CgroupDriver_SYSTEMD:
		s.CgroupDriver = "systemd"
	case runtimeapi.CgroupDriver_CGROUPFS:
		s.CgroupDriver = "cgroupfs"
	default:
		return fmt.Errorf("runtime returned an unknown cgroup driver %d", d)
	}
	logger.Info("Using cgroup driver setting received from the CRI runtime", "cgroupDriver", s.CgroupDriver)
	return nil
}
