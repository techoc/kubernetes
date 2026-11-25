/*
Copyright 2022 The Kubernetes Authors.

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

// Package app 完成配置和启动 Kubernetes app 进程所需的全部工作。
package app

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/spf13/cobra"
	"k8s.io/component-base/metrics"

	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	cliflag "k8s.io/component-base/cli/flag"
	"k8s.io/component-base/featuregate"
	"k8s.io/component-base/logs"
	logsapi "k8s.io/component-base/logs/api/v1"
	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/component-base/term"
	metadataapi "k8s.io/dynamic-resource-allocation/api/metadata/v1beta1" // always use the latest here
	"k8s.io/dynamic-resource-allocation/kubeletplugin"
	"k8s.io/dynamic-resource-allocation/resourceslice"
	"k8s.io/klog/v2"
)

// NewCommand 创建一个带默认参数的 *cobra.Command。
func NewCommand() *cobra.Command {
	o := logsapi.NewLoggingConfiguration()
	var clientset kubernetes.Interface
	var config *rest.Config
	logger := klog.Background()

	cmd := &cobra.Command{
		Use:  "cdi-test-driver",
		Long: "cdi-test-driver implements a resource driver controller and kubelet plugin.",
	}
	sharedFlagSets := cliflag.NamedFlagSets{}
	fs := sharedFlagSets.FlagSet("logging")
	logsapi.AddFlags(o, fs)
	logs.AddFlags(fs, logs.SkipLoggingConfigurationFlags())

	fs = sharedFlagSets.FlagSet("Kubernetes client")
	kubeconfig := fs.String("kubeconfig", "", "Absolute path to the kube.config file. Either this or KUBECONFIG need to be set if the driver is being run out of cluster.")
	kubeAPIQPS := fs.Float32("kube-api-qps", 50, "QPS to use while communicating with the kubernetes apiserver.")
	kubeAPIBurst := fs.Int("kube-api-burst", 100, "Burst to use while communicating with the kubernetes apiserver.")

	fs = sharedFlagSets.FlagSet("http server")
	httpEndpoint := fs.String("http-endpoint", "",
		"The TCP network address where the HTTP server for diagnostics, including pprof, metrics and (if applicable) leader election health check, will listen (example: `:8080`). The default is the empty string, which means the server is disabled.")
	metricsPath := fs.String("metrics-path", "/metrics", "The HTTP path where Prometheus metrics will be exposed, disabled if empty.")
	profilePath := fs.String("pprof-path", "", "The HTTP path where pprof profiling will be available, disabled if empty.")

	fs = sharedFlagSets.FlagSet("CDI")
	driverNameFlagName := "drivername"
	driverName := fs.String(driverNameFlagName, "test-driver.cdi.k8s.io", "Resource driver name.")

	fs = sharedFlagSets.FlagSet("other")
	featureGate := featuregate.NewFeatureGate()
	utilruntime.Must(logsapi.AddFeatureGates(featureGate))
	featureGate.AddFlag(fs)

	fs = cmd.PersistentFlags()
	for _, f := range sharedFlagSets.FlagSets {
		fs.AddFlagSet(f)
	}

	mux := http.NewServeMux()

	cmd.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
		// 尽早启用日志，之后再用最终日志配置展示各 flag。

		if err := logsapi.ValidateAndApply(o, featureGate); err != nil {
			return err
		}

		// 如果设置了环境变量 KUBECONFIG 就使用它（便于本地/调试集群）
		kubeconfigEnv := os.Getenv("KUBECONFIG")

		if kubeconfigEnv != "" {
			logger.Info("Found KUBECONFIG environment variable set, using that..")
			*kubeconfig = kubeconfigEnv
		}

		var err error
		if *kubeconfig == "" {
			config, err = rest.InClusterConfig()
			if err != nil {
				return fmt.Errorf("create in-cluster client configuration: %w", err)
			}
		} else {
			config, err = clientcmd.BuildConfigFromFlags("", *kubeconfig)
			if err != nil {
				return fmt.Errorf("create out-of-cluster client configuration: %w", err)
			}
		}
		config.QPS = *kubeAPIQPS
		config.Burst = *kubeAPIBurst

		clientset, err = kubernetes.NewForConfig(config)
		if err != nil {
			return fmt.Errorf("create client: %w", err)
		}

		if *httpEndpoint != "" {
			if *metricsPath != "" {
				// workqueue 和 leader election 的指标通过下面的匿名导入注册：
				// https://github.com/kubernetes/kubernetes/blob/master/staging/src/k8s.io/component-base/metrics/prometheus/workqueue/metrics.go
				// https://github.com/kubernetes/kubernetes/blob/master/staging/src/k8s.io/component-base/metrics/prometheus/clientgo/leaderelection/metrics.go
				//
				// 其中也顺带包含了 Go runtime 和进程指标：
				// https://github.com/kubernetes/kubernetes/blob/9780d88cb6a4b5b067256ecb4abf56892093ee87/staging/src/k8s.io/component-base/metrics/legacyregistry/registry.go#L46-L49
				gatherer := legacyregistry.DefaultGatherer
				actualPath := path.Join("/", *metricsPath)
				logger.Info("Starting metrics", "path", actualPath)
				mux.Handle(actualPath,
					metrics.HandlerFor(gatherer, metrics.HandlerOpts{}))
			}

			if *profilePath != "" {
				actualPath := path.Join("/", *profilePath)
				logger.Info("Starting profiling", "path", actualPath)
				mux.HandleFunc(path.Join("/", *profilePath), pprof.Index)
				mux.HandleFunc(path.Join("/", *profilePath, "cmdline"), pprof.Cmdline)
				mux.HandleFunc(path.Join("/", *profilePath, "profile"), pprof.Profile)
				mux.HandleFunc(path.Join("/", *profilePath, "symbol"), pprof.Symbol)
				mux.HandleFunc(path.Join("/", *profilePath, "trace"), pprof.Trace)
			}

			listener, err := net.Listen("tcp", *httpEndpoint)
			if err != nil {
				return fmt.Errorf("listen on HTTP endpoint: %w", err)
			}

			go func() {
				logger.Info("Starting HTTP server", "endpoint", *httpEndpoint)
				err := http.Serve(listener, mux)
				if err != nil {
					logger.Error(err, "HTTP server failed")
					klog.FlushAndExit(klog.ExitFlushTimeout, 1)
				}
			}()
		}

		return nil
	}

	kubeletPlugin := &cobra.Command{
		Use:   "kubelet-plugin",
		Short: "run as kubelet plugin",
		Long:  "cdi-test-driver kubelet-plugin runs as a device plugin for kubelet that supports dynamic resource allocation.",
		Args:  cobra.ExactArgs(0),
	}
	kubeletPluginFlagSets := cliflag.NamedFlagSets{}
	fs = kubeletPluginFlagSets.FlagSet("kubelet")
	kubeletRegistryDir := fs.String("plugin-registration-path", kubeletplugin.KubeletRegistryDir, "The directory where kubelet looks for plugin registration sockets.")
	kubeletPluginsDir := fs.String("datadir", kubeletplugin.KubeletPluginsDir, "The per-driver directory where the DRA Unix domain socket will be created.")
	fs = kubeletPluginFlagSets.FlagSet("CDI")
	cdiDir := fs.String("cdi-dir", "/var/run/cdi", "directory for dynamically created CDI JSON files")
	nodeName := fs.String("node-name", "", "name of the node that the kubelet plugin is responsible for")
	numDevices := fs.Int("num-devices", 4, "number of devices to simulate per node")
	enableDeviceMetadata := fs.Bool("enable-device-metadata", false,
		"Enable the device metadata feature.")
	fs = kubeletPlugin.Flags()
	for _, f := range kubeletPluginFlagSets.FlagSets {
		fs.AddFlagSet(f)
	}
	kubeletPlugin.RunE = func(cmd *cobra.Command, args []string) error {
		// 确保目录存在，必要时创建。这样能尽早发现会导致创建失败的
		// 环境问题。
		if err := os.MkdirAll(*cdiDir, os.FileMode(0750)); err != nil {
			return fmt.Errorf("create CDI directory: %w", err)
		}
		datadir := path.Join(*kubeletPluginsDir, *driverName)
		if err := os.MkdirAll(filepath.Dir(datadir), 0750); err != nil {
			return fmt.Errorf("create socket directory: %w", err)
		}

		if *nodeName == "" {
			return errors.New("--node-name not set")
		}

		devices := make([]resourceapi.Device, *numDevices)
		for i := 0; i < *numDevices; i++ {
			devices[i] = resourceapi.Device{
				Name: fmt.Sprintf("device-%02d", i),
			}
		}
		driverResources := resourceslice.DriverResources{
			Pools: map[string]resourceslice.Pool{
				*nodeName: {
					Slices: []resourceslice.Slice{{
						Devices: devices,
					}},
				},
			},
		}

		pluginOpts := []any{
			Options{EnableHealthService: true},
			kubeletplugin.PluginDataDirectoryPath(datadir),
			kubeletplugin.RegistrarDirectoryPath(*kubeletRegistryDir),
			kubeletplugin.EnableDeviceMetadata(*enableDeviceMetadata, []schema.GroupVersion{metadataapi.SchemeGroupVersion /* always use the latest */}),
		}
		plugin, err := StartPlugin(cmd.Context(), *cdiDir, *driverName, clientset, *nodeName, FileOperations{DriverResources: &driverResources},
			pluginOpts...,
		)
		if err != nil {
			return fmt.Errorf("start example plugin: %w", err)
		}

		// 处理优雅退出：需要删除 Unix domain socket。
		sigc := make(chan os.Signal, 1)
		signal.Notify(sigc, os.Interrupt, syscall.SIGTERM)
		logger.Info("Waiting for signal.")
		sig := <-sigc
		logger.Info("Received signal, shutting down.", "signal", sig)
		plugin.Stop()
		return nil
	}
	cmd.AddCommand(kubeletPlugin)

	// SetUsageAndHelpFunc 负责 flag 分组，但不支持列出子命令，
	// 所以这里手动把它们加到 cmd.Use。
	cols, _, _ := term.TerminalSize(cmd.OutOrStdout())
	cliflag.SetUsageAndHelpFunc(cmd, sharedFlagSets, cols)
	var children []string
	for _, child := range cmd.Commands() {
		children = append(children, child.Use)
	}
	cmd.Use += " [shared flags] " + strings.Join(children, "|")
	cliflag.SetUsageAndHelpFunc(kubeletPlugin, kubeletPluginFlagSets, cols)

	return cmd
}
