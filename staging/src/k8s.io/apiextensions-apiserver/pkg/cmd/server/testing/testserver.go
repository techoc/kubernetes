/*
Copyright 2018 The Kubernetes Authors.

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

package testing

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/spf13/pflag"

	extensionsapiserver "k8s.io/apiextensions-apiserver/pkg/apiserver"
	"k8s.io/apiextensions-apiserver/pkg/cmd/server/options"
	generatedopenapi "k8s.io/apiextensions-apiserver/pkg/generated/openapi"
	"k8s.io/apimachinery/pkg/util/wait"
	openapinamer "k8s.io/apiserver/pkg/endpoints/openapi"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/storage/storagebackend"
	"k8s.io/apiserver/pkg/util/compatibility"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/apiserver/pkg/util/openapi"
	"k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	basecompatibility "k8s.io/component-base/compatibility"
	"k8s.io/component-base/featuregate"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	logsapi "k8s.io/component-base/logs/api/v1"
	"k8s.io/klog/v2"
)

func init() {
	// 同一测试进程内可能多次实例化测试服务器，或与其他服务器并存；它们都会试图修改
	// 全局日志状态，测试期间必须忽略这类重复应用。
	logsapi.ReapplyHandling = logsapi.ReapplyHandlingIgnoreUnchanged
}

// TearDownFunc 用于关闭测试服务器。
type TearDownFunc func()

// TestServerInstanceOptions 是启动测试服务器时的实例选项。
type TestServerInstanceOptions struct {
}

// TestServer 是 kube-test-ApiServer 提供给调用方的启动结果。
type TestServer struct {
	ClientConfig    *restclient.Config                              // REST 客户端配置
	ServerOpts      *options.CustomResourceDefinitionsServerOptions // 服务器选项
	TearDownFn      TearDownFunc                                    // 关闭函数
	TmpDir          string                                          // apiserver 使用的临时目录
	CompletedConfig extensionsapiserver.CompletedConfig
}

// Logger 允许把 t.Testing 和 b.Testing 传给 StartTestServer 和 StartTestServerOrDie。
type Logger interface {
	Errorf(format string, args ...interface{})
	Fatalf(format string, args ...interface{})
	Logf(format string, args ...interface{})
}

// NewDefaultTestServerOptions 返回 TestServer 实例的默认选项。
func NewDefaultTestServerOptions() *TestServerInstanceOptions {
	return &TestServerInstanceOptions{}
}

// StartTestServer 启动一个 apiextensions-apiserver，返回 REST 客户端配置、
// 关闭函数以及临时目录的位置。
//
// 注意：返回关闭函数而非停止通道，因为 Golang testing 的 os.Exit 不会给停止通道的
// goroutine 留出足够时间清理临时文件，直接返回通道会导致临时文件泄漏。
func StartTestServer(t Logger, _ *TestServerInstanceOptions, customFlags []string, storageConfig *storagebackend.Config) (result TestServer, err error) {
	// TODO: 如果 test/utils/ktesting 未来进入 staging 仓库，这里可以改用它。
	ctx, cancel := context.WithCancelCause(context.Background())
	var errCh chan error
	tearDown := func() {
		// cancel 会停止 apiextensions apiserver 及其委托对象，
		// 后者自身会完成清理，包括关闭存储层。
		cancel(errors.New("tearing down"))

		// 如果 apiextensions apiserver 已经启动，等待它清晰退出。
		if errCh != nil {
			err, ok := <-errCh
			if ok && err != nil {
				klog.Errorf("Failed to shutdown test server clearly: %v", err)
			}
		}

		if len(result.TmpDir) != 0 {
			os.RemoveAll(result.TmpDir)
		}
	}
	defer func() {
		if result.TearDownFn == nil {
			tearDown()
		}
	}()

	result.TmpDir, err = os.MkdirTemp("", "apiextensions-apiserver")
	if err != nil {
		return result, fmt.Errorf("failed to create temp dir: %v", err)
	}

	fs := pflag.NewFlagSet("test", pflag.PanicOnError)

	s := options.NewCustomResourceDefinitionsServerOptions(os.Stdout, os.Stderr)

	// 为并行测试新建独立的 ComponentGlobalsRegistry，避免与默认实例争用。
	featureGate := utilfeature.DefaultMutableFeatureGate.DeepCopy()
	effectiveVersion := compatibility.DefaultKubeEffectiveVersionForTest()
	effectiveVersion.SetEmulationVersion(featureGate.EmulationVersion())
	componentGlobalsRegistry := basecompatibility.NewComponentGlobalsRegistry()
	if err := componentGlobalsRegistry.Register(basecompatibility.DefaultKubeComponent, effectiveVersion, featureGate); err != nil {
		return result, err
	}
	s.ServerRunOptions.ComponentGlobalsRegistry = componentGlobalsRegistry
	s.AddFlags(fs)

	s.RecommendedOptions.SecureServing.Listener, s.RecommendedOptions.SecureServing.BindPort, err = createLocalhostListenerOnFreePort()
	if err != nil {
		return result, fmt.Errorf("failed to create listener: %v", err)
	}
	s.RecommendedOptions.SecureServing.ServerCert.CertDirectory = result.TmpDir
	s.RecommendedOptions.SecureServing.ExternalAddress = s.RecommendedOptions.SecureServing.Listener.Addr().(*net.TCPAddr).IP // use listener addr although it is a loopback device

	pkgPath, err := pkgPath(t)
	if err != nil {
		return result, err
	}
	s.RecommendedOptions.SecureServing.ServerCert.FixtureDirectory = filepath.Join(pkgPath, "testdata")

	if storageConfig != nil {
		s.RecommendedOptions.Etcd.StorageConfig = *storageConfig
	}
	s.APIEnablement.RuntimeConfig.Set("api/all=true")

	fs.Parse(customFlags)

	if err := componentGlobalsRegistry.Set(); err != nil {
		return result, fmt.Errorf("%w\nIf you are using SetFeatureGate*DuringTest, try using --emulated-version and --feature-gates flags instead", err)
	}
	// 若本地 ComponentGlobalsRegistry 被命令行标志改动，需要把新的特性值回写到
	// DefaultFeatureGate，因为多数特性检查仍然使用默认的 DefaultFeatureGate。
	if !featureGate.EmulationVersion().EqualTo(utilfeature.DefaultMutableFeatureGate.EmulationVersion()) || !featureGate.MinCompatibilityVersion().EqualTo(utilfeature.DefaultMutableFeatureGate.MinCompatibilityVersion()) {
		featuregatetesting.SetFeatureGateVersionsDuringTest(t.(featuregatetesting.TB), utilfeature.DefaultMutableFeatureGate, effectiveVersion.EmulationVersion(), effectiveVersion.MinCompatibilityVersion())
	}
	featureOverrides := map[featuregate.Feature]bool{}
	// 找出本地 registry 与默认 gate 之间的差异，逐一通过测试钩子同步，
	// 保证测试期间全局特性状态与启动配置一致。
	for f := range utilfeature.DefaultMutableFeatureGate.GetAll() {
		if featureGate.Enabled(f) != utilfeature.DefaultFeatureGate.Enabled(f) {
			featureOverrides[f] = featureGate.Enabled(f)
		}
	}
	if len(featureOverrides) > 0 {
		featuregatetesting.SetFeatureGatesDuringTest(t.(featuregatetesting.TB), utilfeature.DefaultFeatureGate, featureOverrides)
	}

	if err := s.Complete(); err != nil {
		return result, fmt.Errorf("failed to set default options: %v", err)
	}
	if err := s.Validate(); err != nil {
		return result, fmt.Errorf("failed to validate options: %v", err)
	}

	t.Logf("runtime-config=%v", s.APIEnablement.RuntimeConfig)
	t.Logf("Starting apiextensions-apiserver on port %d...", s.RecommendedOptions.SecureServing.BindPort)

	config, err := s.Config()
	if err != nil {
		return result, fmt.Errorf("failed to create config from options: %v", err)
	}

	getOpenAPIDefinitions := openapi.GetOpenAPIDefinitionsWithoutDisabledFeatures(generatedopenapi.GetOpenAPIDefinitions)
	namer := openapinamer.NewDefinitionNamer(extensionsapiserver.Scheme)
	config.GenericConfig.OpenAPIConfig = genericapiserver.DefaultOpenAPIConfig(getOpenAPIDefinitions, namer)

	completedConfig := config.Complete()
	server, err := completedConfig.New(genericapiserver.NewEmptyDelegate())
	if err != nil {
		return result, fmt.Errorf("failed to create server: %v", err)
	}

	errCh = make(chan error)
	// 在独立 goroutine 中运行服务器，把启动或运行错误回传给等待方。
	go func() {
		defer close(errCh)

		if err := server.GenericAPIServer.PrepareRun().RunWithContext(ctx); err != nil {
			errCh <- err
		}
	}()

	t.Logf("Waiting for /healthz to be ok...")

	// 用回环客户端等待健康检查通过，确保服务器已具备服务能力后再返回。
	client, err := kubernetes.NewForConfig(server.GenericAPIServer.LoopbackClientConfig)
	if err != nil {
		return result, fmt.Errorf("failed to create a client: %v", err)
	}
	err = wait.Poll(100*time.Millisecond, time.Minute, func() (bool, error) {
		select {
		case err := <-errCh:
			return false, err
		default:
		}

		result := client.CoreV1().RESTClient().Get().AbsPath("/healthz").Do(context.TODO())
		status := 0
		result.StatusCode(&status)
		if status == 200 {
			return true, nil
		}
		return false, nil
	})
	if err != nil {
		return result, fmt.Errorf("failed to wait for /healthz to return ok: %v", err)
	}

	// 从这里开始，调用方必须调用 tearDown。
	result.ClientConfig = server.GenericAPIServer.LoopbackClientConfig
	result.ServerOpts = s
	result.TearDownFn = tearDown
	result.CompletedConfig = completedConfig

	return result, nil
}

// StartTestServerOrDie 调用 StartTestServer，失败时通过 t.Fatal 终止测试。
func StartTestServerOrDie(t Logger, instanceOptions *TestServerInstanceOptions, flags []string, storageConfig *storagebackend.Config) *TestServer {
	result, err := StartTestServer(t, instanceOptions, flags, storageConfig)
	if err == nil {
		return &result
	}

	t.Fatalf("failed to launch server: %v", err)
	return nil
}

// createLocalhostListenerOnFreePort 在 127.0.0.1 上监听一个随机空闲端口。
func createLocalhostListenerOnFreePort() (net.Listener, int, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, 0, err
	}

	// 取实际监听端口
	tcpAddr, ok := ln.Addr().(*net.TCPAddr)
	if !ok {
		ln.Close()
		return nil, 0, fmt.Errorf("invalid listen address: %q", ln.Addr().String())
	}

	return ln, tcpAddr.Port, nil
}

// pkgPath 返回本包目录的绝对路径。使用 go test 时可以直接查看运行时调用栈；
// 但 bazel 以 -trimpath 编译 Go 二进制，简单方式会失效，因此需要借助环境变量推导路径。
//
// 这里的做法在 go test 和 bazel 下都成立，前提是：仅当传入 trimpath 时才处于 bazel 环境。
func pkgPath(t Logger) (string, error) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		return "", fmt.Errorf("failed to get current file")
	}

	pkgPath := filepath.Dir(thisFile)

	// 若存在 bazel 环境变量，说明传入了 -trimpath，需要从环境变量构造路径。
	if testSrcdir, testWorkspace := os.Getenv("TEST_SRCDIR"), os.Getenv("TEST_WORKSPACE"); testSrcdir != "" && testWorkspace != "" {
		t.Logf("Detected bazel env varaiables: TEST_SRCDIR=%q TEST_WORKSPACE=%q", testSrcdir, testWorkspace)
		pkgPath = filepath.Join(testSrcdir, testWorkspace, pkgPath)
	}

	// 路径仍非绝对路径，说明并非 bazel 以 -trimpath 编译的情况。
	if !filepath.IsAbs(pkgPath) {
		return "", fmt.Errorf("can't construct an absolute path from %q", pkgPath)
	}

	t.Logf("Resolved testserver package path to: %q", pkgPath)

	return pkgPath, nil
}
