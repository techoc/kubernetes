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
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/spf13/pflag"

	"k8s.io/apimachinery/pkg/util/wait"
	utilcompatibility "k8s.io/apiserver/pkg/util/compatibility"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/kubernetes"
	"k8s.io/component-base/compatibility"
	"k8s.io/component-base/configz"
	"k8s.io/component-base/featuregate"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	logsapi "k8s.io/component-base/logs/api/v1"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/cmd/kube-controller-manager/app"
	kubecontrollerconfig "k8s.io/kubernetes/cmd/kube-controller-manager/app/config"
	"k8s.io/kubernetes/cmd/kube-controller-manager/app/options"
)

func init() {
	// 多个测试服务器会争用全局日志状态，测试中忽略未变化的重复配置。
	logsapi.ReapplyHandling = logsapi.ReapplyHandlingIgnoreUnchanged
}

// TearDownFunc 用于关闭并清理测试服务器。
type TearDownFunc func()

// TestServer 保存启动测试服务器所需的选项、配置和清理信息。
type TestServer struct {
	// Options 保存本实例实际使用的启动选项。
	Options *options.KubeControllerManagerOptions
	// Config 保存运行配置，可用于继续创建测试所需的客户端或 Informer。
	Config     *kubecontrollerconfig.Config
	TearDownFn TearDownFunc // 清理函数
	TmpDir     string       // API 服务器使用的临时目录
}

// StartTestServer 启动测试用控制器管理器，返回选项、REST 配置、清理函数和临时目录。
// 返回清理函数而非停止通道，因为 Go 测试调用 os.Exit 时，停止通道协程可能来不及删除临时文件。
func StartTestServer(t *testing.T, ctx context.Context, customFlags []string) (result TestServer, err error) {
	logger := klog.FromContext(ctx)
	ctx, cancel := context.WithCancel(ctx)
	var errCh chan error
	tearDown := func() {
		cancel()

		// 若控制器管理器已启动，等待其完成优雅关闭。
		if errCh != nil {
			err, ok := <-errCh
			if ok && err != nil {
				logger.Error(err, "Failed to shutdown test server cleanly")
			}
		}
		configz.Delete(app.ConfigzName)
	}
	defer func() {
		if result.TearDownFn == nil {
			tearDown()
		}
	}()

	result.TmpDir = t.TempDir()

	fs := pflag.NewFlagSet("test", pflag.PanicOnError)

	s, err := options.NewKubeControllerManagerOptions()
	if err != nil {
		return TestServer{}, err
	}
	// 创建独立 ComponentGlobalsRegistry，避免并行测试争用默认注册表。
	featureGate := utilfeature.DefaultMutableFeatureGate.DeepCopy()
	effectiveVersion := utilcompatibility.DefaultKubeEffectiveVersionForTest()
	effectiveVersion.SetEmulationVersion(featureGate.EmulationVersion())
	effectiveVersion.SetMinCompatibilityVersion(featureGate.MinCompatibilityVersion())
	componentGlobalsRegistry := compatibility.NewComponentGlobalsRegistry()
	if err := componentGlobalsRegistry.Register(compatibility.DefaultKubeComponent, effectiveVersion, featureGate); err != nil {
		return result, err
	}
	s.ComponentGlobalsRegistry = componentGlobalsRegistry

	all, disabled, aliases := app.KnownControllers(), app.ControllersDisabledByDefault(), app.ControllerAliases()
	namedFlagSets := s.Flags(all, disabled, aliases)
	for _, f := range namedFlagSets.FlagSets {
		fs.AddFlagSet(f)
	}
	fs.Parse(customFlags)
	s.ParsedFlags = &namedFlagSets

	if err := s.ComponentGlobalsRegistry.Set(); err != nil {
		return result, err
	}
	// 参数修改本地注册表后，还需同步 DefaultFeatureGate，因为多数特性检查仍读取它。
	if !featureGate.EmulationVersion().EqualTo(utilfeature.DefaultMutableFeatureGate.EmulationVersion()) || !featureGate.MinCompatibilityVersion().EqualTo(utilfeature.DefaultMutableFeatureGate.MinCompatibilityVersion()) {
		featuregatetesting.SetFeatureGateVersionsDuringTest(t, utilfeature.DefaultMutableFeatureGate, effectiveVersion.EmulationVersion(), effectiveVersion.MinCompatibilityVersion())
	}
	featureOverrides := map[featuregate.Feature]bool{}
	for f := range utilfeature.DefaultMutableFeatureGate.GetAll() {
		if featureGate.Enabled(f) != utilfeature.DefaultFeatureGate.Enabled(f) {
			featureOverrides[f] = featureGate.Enabled(f)
		}
	}
	if len(featureOverrides) > 0 {
		featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featureOverrides)
	}

	if s.SecureServing.BindPort != 0 {
		s.SecureServing.Listener, s.SecureServing.BindPort, err = createListenerOnFreePort()
		if err != nil {
			return result, fmt.Errorf("failed to create listener: %v", err)
		}
		s.SecureServing.ServerCert.CertDirectory = result.TmpDir

		logger.Info("kube-controller-manager will listen securely", "port", s.SecureServing.BindPort)
	}

	config, err := s.Config(ctx, all, disabled, aliases)
	if err != nil {
		return result, fmt.Errorf("failed to create config from options: %v", err)
	}

	errCh = make(chan error)
	go func(ctx context.Context) {
		defer close(errCh)

		if err := app.Run(ctx, config.Complete()); err != nil {
			errCh <- err
		}
	}(ctx)

	logger.Info("Waiting for /healthz to be ok...")
	client, err := kubernetes.NewForConfig(config.Kubeconfig)
	if err != nil {
		return result, fmt.Errorf("failed to create a client: %v", err)
	}
	err = wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 30*time.Second, false, func(ctx context.Context) (bool, error) {
		select {
		case <-ctx.Done():
			return false, ctx.Err()
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

	// 返回成功后，调用方必须执行清理函数。
	result.Options = s
	result.Config = config
	result.TearDownFn = tearDown

	return result, nil
}

// StartTestServerOrDie 启动测试服务器；失败时调用 t.Fatal 终止测试。
func StartTestServerOrDie(t *testing.T, ctx context.Context, flags []string) *TestServer {
	result, err := StartTestServer(t, ctx, flags)
	if err != nil {
		t.Fatalf("failed to launch server: %v", err)
	}
	return &result
}

func createListenerOnFreePort() (net.Listener, int, error) {
	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		return nil, 0, err
	}

	// 读取监听端口
	tcpAddr, ok := ln.Addr().(*net.TCPAddr)
	if !ok {
		ln.Close()
		return nil, 0, fmt.Errorf("invalid listen address: %q", ln.Addr().String())
	}

	return ln, tcpAddr.Port, nil
}
