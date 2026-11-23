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

// Package app makes it easy to create a kubelet server for various contexts.
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
	// Prevent memory leak from OTel metrics, which we don't use:
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
	// programmer error
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
		// The Kubelet has special flag parsing requirements to enforce flag precedence rules,
		// so we do all our parsing manually in Run, below.
		// DisableFlagParsing=true provides the full set of flags passed to the kubelet in the
		// `args` arg to Run, without Cobra's interference.
		// Kubelet 需要加载配置文件后再次解析 Flag，以维持“命令行覆盖配置文件”的历史优先级。
		DisableFlagParsing: true,
		SilenceUsage:       true,
		RunE: func(cmd *cobra.Command, args []string) error {
			// 手动完成第一轮解析，先取得配置文件路径和加载配置前需要校验的参数。
			if err := cleanFlagSet.Parse(args); err != nil {
				return fmt.Errorf("failed to parse kubelet flag: %w", err)
			}

			// check if there are non-flag arguments in the command line
			cmds := cleanFlagSet.Args()
			if len(cmds) > 0 {
				return fmt.Errorf("unknown command %+s", cmds[0])
			}

			// short-circuit on help
			help, err := cleanFlagSet.GetBool("help")
			if err != nil {
				return errors.New(`"help" flag is non-bool, programmer error, please correct`)
			}
			if help {
				return cmd.Help()
			}

			// short-circuit on verflag
			verflag.PrintAndExitIfRequested()

			// set feature gates from initial flags-based config
			if err := utilfeature.DefaultMutableFeatureGate.SetFromMap(kubeletConfig.FeatureGates); err != nil {
				return fmt.Errorf("failed to set feature gates from initial flags-based config: %w", err)
			}

			// validate the initial KubeletFlags
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
				// This is necessary to preserve backwards-compatibility across binary upgrades.
				// See issue #56171 for more details.
				if err := kubeletConfigFlagPrecedence(kubeletConfig, args); err != nil {
					return fmt.Errorf("failed to precedence kubeletConfigFlag: %w", err)
				}
				// update feature gates based on new config
				if err := utilfeature.DefaultMutableFeatureGate.SetFromMap(kubeletConfig.FeatureGates); err != nil {
					return fmt.Errorf("failed to set feature gates from initial flags-based config: %w", err)
				}
			}

			// 配置来源和覆盖关系确定后再初始化日志，确保日志行为采用最终配置。
			logs.InitLogs()
			// Retrieve the logger again to reflect the updated log settings
			logger = klog.FromContext(ctx)
			if err := logsapi.ValidateAndApplyAsField(&kubeletConfig.Logging, utilfeature.DefaultFeatureGate, field.NewPath("logging")); err != nil {
				return fmt.Errorf("initialize logging: %v", err)
			}
			// Dump the full effective KubeletConfiguration.
			if cfgStr, err := marshalKubeletConfigForLog(kubeletConfig); err != nil {
				// Logging must never block startup; log the error and continue.
				logger.Error(err, "Failed to marshal effective KubeletConfiguration for logging")
			} else {
				logger.Info("Effective KubeletConfiguration", "config", cfgStr)
			}
			// Node-specific flags that are not part of KubeletConfiguration (e.g.
			// --hostname-override, --kubeconfig, --node-ip, --cert-dir) come straight from the
			// command line and remain accurate, so they stay available for debugging.
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

			// Log skipped drop-in files if any were encountered during configuration merge
			if len(skippedDropinFiles) > 0 {
				for _, skippedFile := range skippedDropinFiles {
					logger.V(4).Info("Skipped file in drop-in directory (does not have .conf extension)", "file", skippedFile)
				}
			}

			// 从此处开始监听系统退出信号，所有长期运行组件共用这个取消上下文。
			ctx := genericapiserver.SetupSignalContext()

			utilfeature.DefaultMutableFeatureGate.AddMetrics()
			// run the kubelet
			return Run(ctx, kubeletServer, kubeletDeps, utilfeature.DefaultFeatureGate)
		},
	}

	// keep cleanFlagSet separate, so Cobra doesn't pollute it with the global flags
	kubeletFlags.AddFlags(cleanFlagSet)
	options.AddKubeletConfigFlags(cleanFlagSet, kubeletConfig)
	options.AddGlobalFlags(cleanFlagSet)
	cleanFlagSet.BoolP("help", "h", false, fmt.Sprintf("help for %s", cmd.Name()))

	// ugly, but necessary, because Cobra's default UsageFunc and HelpFunc pollute the flagset with global flags
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

// mergeKubeletConfigurations merges the provided drop-in configurations with the base kubelet configuration.
// The drop-in configurations are processed in lexical order based on the file names. This means that the
// configurations in files with lower numeric prefixes are applied first, followed by higher numeric prefixes.
// For example, if the drop-in directory contains files named "10-config.conf" and "20-config.conf",
// the configurations in "10-config.conf" will be applied first, and then the configurations in "20-config.conf" will be applied,
// potentially overriding the previous values. Files in subdirectories are also processed in lexical order.
// Returns a list of skipped files and an error.
// mergeKubeletConfigurations 按文件名字典序合并目录及子目录下的 .conf 文件；后加载文件覆盖先前值。
func mergeKubeletConfigurations(kubeletConfig *kubeletconfiginternal.KubeletConfiguration, kubeletDropInConfigDir string) ([]string, error) {
	const dropinFileExtension = ".conf"
	var skippedFiles []string

	// TODO: move the "internal --> versioned --> merge --> default --> internal" operation inside the loop,
	// and use the version of each drop-in file as the versioned target once config files can be in versions other than just v1beta1
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
		// TODO: expand this once more than a single kubelet config version exists
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

	// reset versioned config and decode
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

// newFlagSetWithGlobals constructs a new pflag.FlagSet with global flags registered
// on it.
// newFlagSetWithGlobals 创建独立参数集合，并显式加入依赖库注册在 Go 全局 FlagSet 上的参数。
func newFlagSetWithGlobals() *pflag.FlagSet {
	fs := pflag.NewFlagSet("", pflag.ExitOnError)
	// set the normalize func, similar to k8s.io/component-base/cli//flags.go:InitFlags
	fs.SetNormalizeFunc(cliflag.WordSepNormalizeFunc)
	// explicitly add flags from libs that register global flags
	options.AddGlobalFlags(fs)
	return fs
}

// newFakeFlagSet constructs a pflag.FlagSet with the same flags as fs, but where
// all values have noop Set implementations
// newFakeFlagSet 复制参数名称和帮助信息，但把每个 Value 替换成 NoOp，供二次解析时避免副作用。
func newFakeFlagSet(fs *pflag.FlagSet) *pflag.FlagSet {
	ret := pflag.NewFlagSet("", pflag.ExitOnError)
	ret.SetNormalizeFunc(fs.GetNormalizeFunc())
	fs.VisitAll(func(f *pflag.Flag) {
		ret.VarP(cliflag.NoOp{}, f.Name, f.Shorthand, f.Usage)
	})
	return ret
}

// kubeletConfigFlagPrecedence re-parses flags over the KubeletConfiguration object.
// We must enforce flag precedence by re-parsing the command line into the new object.
// This is necessary to preserve backwards-compatibility across binary upgrades.
// See issue #56171 for more details.
// kubeletConfigFlagPrecedence 把原始命令行重新解析到配置文件对象，以维持“命令行优先于配置文件”的兼容约定。
func kubeletConfigFlagPrecedence(kc *kubeletconfiginternal.KubeletConfiguration, args []string) error {
	// 使用临时 FlagSet 和 NoOp 全局参数，避免重复解析那些 Set 会累加值的 Flag 实现。
	// as some Set implementations accumulate values from multiple flag invocations.
	fs := newFakeFlagSet(newFlagSetWithGlobals())
	// register throwaway KubeletFlags
	options.NewKubeletFlags().AddFlags(fs)
	// register new KubeletConfiguration
	options.AddKubeletConfigFlags(fs, kc)
	// 保存文件中原有的 Feature Gate；重新解析命令行后再补回没有被 Flag 指定的项。
	original := kc.FeatureGates
	// avoid duplicate printing the flag deprecation warnings during re-parsing
	fs.SetOutput(io.Discard)
	// re-parse flags
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
	// compute absolute path based on current working dir
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
	// EvictionHard can have OS-specific fields, which is why there's no default value for it.
	// See: https://github.com/kubernetes/kubernetes/pull/110263
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
	// compute absolute path based on current working dir
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

// UnsecuredDependencies returns a Dependencies suitable for being run, or an error if the server setup
// is not valid.  It will not start any background processes, and does not include authentication/authorization
// UnsecuredDependencies 构造不启动后台进程、也不包含认证/授权控制器的基础依赖集合。
func UnsecuredDependencies(ctx context.Context, s *options.KubeletServer, featureGate featuregate.FeatureGate) (*kubelet.Dependencies, error) {
	// Initialize the TLS Options
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
		Auth:                nil, // default does not enforce auth[nz]
		CAdvisorInterface:   nil, // cadvisor.New launches background processes (bg http.ListenAndServe, and some bg cleaners), not set here
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
// The kubeDeps argument may be nil - if so, it is initialized from the settings on KubeletServer.
// Otherwise, the caller is assumed to have set up the Dependencies object and a default one will
// not be generated.
func Run(ctx context.Context, s *options.KubeletServer, kubeDeps *kubelet.Dependencies, featureGate featuregate.FeatureGate) error {
	logger := klog.FromContext(ctx)
	// To help debugging, immediately log version
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

// convertToVersionedKubeletConfig converts the internal KubeletConfiguration to the
// external kubelet.config.k8s.io/v1beta1 representation and stamps the GroupVersionKind.
// This is the same conversion that /configz performs, so callers that want to surface the
// effective configuration in the exact form served by /configz can share this single path.
// convertToVersionedKubeletConfig 将内部配置转为对外公开的 v1beta1 对象，供 /configz 和日志共享同一转换路径。
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

// marshalKubeletConfigForLog renders the effective KubeletConfiguration as a human-readable
// YAML string for startup logging. The output mirrors what /configz serves except the
// sensitive field StaticPodURLHeader is masked while /configz outputs it.
func marshalKubeletConfigForLog(kc *kubeletconfiginternal.KubeletConfiguration) (string, error) {
	// Make the config safe for logging without mutating the caller's copy.
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
	// NewHealthChecker returns an error indicating that the watchdog is configured but the configuration is incorrect,
	// the kubelet will not be started.
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
	// Propagate feature gate state to the metrics subsystem. This must be called
	// after feature gates are set and before any histogram metrics are registered.
	metricsfeatures.ApplyFeatureGates(featureGate)
	// validate the initial KubeletServer (we set feature gates first, because this validation depends on feature gates)
	if err := options.ValidateKubeletServer(s); err != nil {
		return err
	}

	// Warn about MemoryQoS compatibility issues
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

		// If the value of EventRecordQPS is 0, there is no limit enforced
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
		// The timeout is the minimum of the lease duration and status update frequency
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
		// RuntimeCgroups is optional, so ignore if it isn't specified
		cgroupRoots = append(cgroupRoots, s.RuntimeCgroups)
	}

	if s.SystemCgroups != "" {
		// SystemCgroups is optional, so ignore if it isn't specified
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

	// Setup event recorder if required.
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
			// at cmd option validation phase it is tested either --system-reserved-cgroup or --kube-reserved-cgroup is specified, so overwrite should be ok
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
		// If the user requested to ignore eviction thresholds, then do not set valid values for hardEvictionThresholds here.
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
				// This feature is not supported for cgroupv1 so we are failing early.
				return fmt.Errorf("swap feature is enabled and LimitedSwap but it is only supported with cgroupv2")
			}
			if !s.FailSwapOn && s.MemorySwap.SwapBehavior == "" {
				// This is just a log because we are using a default of NoSwap.
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

	// TODO(vmarmol): Do this through container config.
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

	// If systemd is used, notify it that we have started
	go daemon.SdNotify(false, "READY=1")

	select {
	case <-done:
		break
	case <-ctx.Done():
		break
	}

	return nil
}

// buildKubeletClientConfig constructs the appropriate client config for the kubelet depending on whether
// bootstrapping is enabled or client certificate rotation is enabled.
// buildKubeletClientConfig 处理 kubeconfig、BootstrapKubeconfig 和客户端证书轮换，返回 API 客户端配置及心跳失败后的连接清理回调。
func buildKubeletClientConfig(ctx context.Context, s *options.KubeletServer, tp oteltrace.TracerProvider, nodeName types.NodeName) (*restclient.Config, func(), error) {
	logger := klog.FromContext(ctx)
	// 开启客户端证书轮换后，先准备证书申请凭据和由证书管理器维护的客户端配置。
	if s.RotateCertificates {
		// Rules for client rotation and the handling of kube config files:
		//
		// 1. If the client provides only a kubeconfig file, we must use that as the initial client
		//    kubeadm needs the initial data in the kubeconfig to be placed into the cert store
		// 2. If the client provides only an initial bootstrap kubeconfig file, we must create a
		//    kubeconfig file at the target location that points to the cert store, but until
		//    the file is present the client config will have no certs
		// 3. If the client provides both and the kubeconfig is valid, we must ignore the bootstrap
		//    kubeconfig.
		// 4. If the client provides both and the kubeconfig is expired or otherwise invalid, we must
		//    replace the kubeconfig with a new file that points to the cert dir
		//
		// The desired configuration for bootstrapping is to use a bootstrap kubeconfig and to have
		// the kubeconfig file be managed by this process. For backwards compatibility with kubeadm,
		// which provides a high powered kubeconfig on the master with cert/key data, we must
		// bootstrap the cert manager with the contents of the initial client config.

		logger.Info("Client rotation is on, will bootstrap in background")
		// certConfig 用于申请/轮换证书，clientConfig 最终指向证书存储；二者可能使用不同凭据。
		certConfig, clientConfig, err := bootstrap.LoadClientConfig(logger, s.KubeConfig, s.BootstrapKubeconfig, s.CertDirectory)
		if err != nil {
			return nil, nil, err
		}

		// use the correct content type for cert rotation, but don't set QPS
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

		// we set exitAfter to five minutes because we use this client configuration to request new certs - if we are unable
		// to request new certs, we will be unable to continue normal operation. Exiting the process allows a wrapper
		// or the bootstrapping credentials to potentially lay down new initial config.
		// UpdateTransport 安装动态证书 Transport；连续五分钟无法申请新证书时退出，让外部引导机制有机会修复凭据。
		closeAllConns, err := kubeletcertificate.UpdateTransport(logger, wait.NeverStop, transportConfig, clientCertificateManager, 5*time.Minute)
		if err != nil {
			return nil, nil, err
		}
		var onHeartbeatFailure func()
		// Kubelet needs to be able to recover from stale http connections.
		// HTTP2 has a mechanism to detect broken connections by sending periodical pings.
		// HTTP1 only can have one persistent connection, and it will close all Idle connections
		// once the Kubelet heartbeat fails. However, since there are many edge cases that we can't
		// control, users can still opt-in to the previous behavior for closing the connections by
		// setting the environment variable DISABLE_HTTP2.
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
	// Kubelet needs to be able to recover from stale http connections.
	// HTTP2 has a mechanism to detect broken connections by sending periodical pings.
	// HTTP1 only can have one persistent connection, and it will close all Idle connections
	// once the Kubelet heartbeat fails. However, since there are many edge cases that we can't
	// control, users can still opt-in to the previous behavior for closing the connections by
	// setting the environment variable DISABLE_HTTP2.
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

// updateDialer instruments a restconfig with a dial. the returned function allows forcefully closing all active connections.
// updateDialer 为 REST Client 安装可轮换连接的 Dialer；返回的回调可在心跳失败时强制关闭旧连接。
func updateDialer(clientConfig *restclient.Config) (func(), error) {
	if clientConfig.Transport != nil || clientConfig.Dial != nil {
		return nil, fmt.Errorf("there is already a transport or dialer configured")
	}
	d := connrotation.NewDialer((&net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}).DialContext)
	clientConfig.Dial = d.DialContext
	return d.CloseAll, nil
}

// buildClientCertificateManager creates a certificate manager that will use certConfig to request a client certificate
// if no certificate is available, or the most recent clientConfig (which is assumed to point to the cert that the manager will
// write out).
// buildClientCertificateManager 创建客户端证书生命周期管理器：首次申请使用引导凭据，已有证书时使用正式客户端配置。
func buildClientCertificateManager(certConfig, clientConfig *restclient.Config, certDir string, nodeName types.NodeName) (certificate.Manager, error) {
	// 申请首张证书时使用 bootstrap 凭据；证书签发后切换为正式 kubeconfig 中的身份。
	newClientsetFn := func(current *tls.Certificate) (clientset.Interface, error) {
		// If we have a valid certificate, use that to fetch CSRs. Otherwise use the bootstrap
		// credentials. In the future it would be desirable to change the behavior of bootstrap
		// to always fall back to the external bootstrap credentials when such credentials are
		// provided by a fundamental trust system like cloud VM identity or an HSM module.
		config := certConfig
		if current != nil {
			config = clientConfig
		}
		return clientset.NewForConfig(config)
	}

	return kubeletcertificate.NewKubeletClientCertificateManager(
		certDir,
		nodeName,

		// this preserves backwards compatibility with kubeadm which passes
		// a high powered certificate to the kubelet as --kubeconfig and expects
		// it to be rotated out immediately
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
	// Override kubeconfig qps/burst settings from flags
	clientConfig.QPS = float32(s.KubeAPIQPS)
	clientConfig.Burst = int(s.KubeAPIBurst)
}

// InitializeTLS 检查 HTTPS 服务端证书配置；未指定且未启用证书引导时生成并保存自签名证书。
// certificate and key file are generated. Returns a configured server.TLSOptions object.
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

// setContentTypeForClient sets the appropriate content type into the rest config
// and handles defaulting AcceptContentTypes based on that input.
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
		// otherwise let the rest client perform defaulting
	}
}

// RunKubelet is responsible for setting up and running a kubelet.  It is used in three different applications:
//
//	1 Integration tests
//	2 Kubelet binary
//	3 Standalone 'kubernetes' binary
//
// Eventually, #2 will be replaced with instances of #3
// RunKubelet 根据节点身份和本地配置创建 MainKubelet，检查 Pod 配置源后启动工作循环与端点。
func RunKubelet(ctx context.Context, kubeServer *options.KubeletServer, kubeDeps *kubelet.Dependencies) error {
	logger := klog.FromContext(ctx)
	hostname, err := nodeutil.GetHostname(kubeServer.HostnameOverride)
	if err != nil {
		return err
	}
	nodeName := types.NodeName(hostname)
	// Setup event recorder if required.
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
	// when the builder was run. This is just a precaution.
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

	// start the kubelet server
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
	// TODO: block until all sources have delivered at least one update to the channel, or break the sync loop
	// up into "per source" synchronizations

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

// parseResourceList parses the given configuration map into an API
// ResourceList or returns an error.
// parseResourceList 将资源字符串转换为 API ResourceList，并拒绝未知资源和负数。
func parseResourceList(m map[string]string) (v1.ResourceList, error) {
	if len(m) == 0 {
		return nil, nil
	}
	rl := make(v1.ResourceList)
	for k, v := range m {
		switch v1.ResourceName(k) {
		// CPU, memory, local storage, and PID resources are supported.
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
	// Retry a couple of times, hoping that any errors are transient.
	// Fail quickly on known, non transient errors.
	for range 3 {
		runtimeConfig, err = kubeDeps.RemoteRuntimeService.RuntimeConfig(ctx)
		if err != nil {
			s, ok := status.FromError(err)
			if !ok || s.Code() != codes.Unimplemented {
				// We could introduce a backoff delay or jitter, but this is largely catching cases
				// where the runtime is still starting up and we request too early.
				// Give it a little more time.
				time.Sleep(time.Second * 2)
				continue
			}
			// CRI implementation doesn't support RuntimeConfig, fallback
			legacyregistry.MustRegister(kubeletmetrics.CRILosingSupport)
			kubeletmetrics.CRILosingSupport.WithLabelValues("1.38.0").Inc()
			logger.Info("CRI implementation should be updated to support RuntimeConfig. Falling back to using cgroupDriver from kubelet config.")
			return nil
		}
	}
	if err != nil {
		return err
	}

	// Calling GetLinux().GetCgroupDriver() won't segfault, but it will always default to systemd
	// which is not intended by the fields not being populated
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
