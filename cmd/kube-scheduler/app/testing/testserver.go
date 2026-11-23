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
	"k8s.io/kubernetes/cmd/kube-scheduler/app"
	kubeschedulerconfig "k8s.io/kubernetes/cmd/kube-scheduler/app/config"
	"k8s.io/kubernetes/cmd/kube-scheduler/app/options"
)

func init() {
	// If instantiated more than once or together with other servers, the
	// servers would try to modify the global logging state. This must get
	// ignored during testing.
	logsapi.ReapplyHandling = logsapi.ReapplyHandlingIgnoreUnchanged
}

// TearDownFunc is to be called to tear down a test server.
type TearDownFunc func()

// TestServer return values supplied by kube-test-ApiServer
type TestServer struct {
	Options    *options.Options
	Config     *kubeschedulerconfig.Config
	TearDownFn TearDownFunc // TearDown function
	TmpDir     string       // Temp Dir used, by the apiserver
}

// StartTestServer 在测试中启动真实的 kube-scheduler 实例，并返回运行配置、临时目录和显式清理函数。
// 启动成功以 /healthz 返回 200 为准，而不是仅以 Run goroutine 已创建为准。
//
// and location of the tmpdir are returned.
//
// Note: we return a tear-down func instead of a stop channel because the later will leak temporary
//
//	files that because Golang testing's call to os.Exit will not give a stop channel go routine
//	enough time to remove temporary files.
func StartTestServer(t *testing.T, ctx context.Context, customFlags []string) (result TestServer, err error) {
	logger := klog.FromContext(ctx)
	// 使用独立子 Context 管理测试实例；TearDown 取消它以通知服务和后台协程退出。
	ctx, cancel := context.WithCancel(ctx)

	var errCh chan error
	tearDown := func() {
		// 先取消运行上下文，再等待 Run goroutine 退出，避免测试结束后遗留后台任务或占用端口。
		cancel()

		// If the scheduler was started, let's wait for it to
		// shutdown clearly.
		if errCh != nil {
			err, ok := <-errCh
			if ok && err != nil {
				logger.Error(err, "Failed to shutdown test server clearly")
			}
		}
		// configz 使用进程级注册表；删除本测试注册项，防止后续测试遇到重复名称。
		configz.Delete("componentconfig")
	}
	defer func() {
		if result.TearDownFn == nil {
			tearDown()
		}
	}()

	result.TmpDir = t.TempDir()

	fs := pflag.NewFlagSet("test", pflag.PanicOnError)

	opts := options.NewOptions()
	// 每个测试使用独立注册表和 FeatureGate 副本，避免并行测试修改共享版本/开关时互相污染。
	featureGate := utilfeature.DefaultMutableFeatureGate.DeepCopy()
	effectiveVersion := utilcompatibility.DefaultKubeEffectiveVersionForTest()
	effectiveVersion.SetEmulationVersion(featureGate.EmulationVersion())
	effectiveVersion.SetMinCompatibilityVersion(featureGate.MinCompatibilityVersion())
	componentGlobalsRegistry := compatibility.NewComponentGlobalsRegistry()
	if err := componentGlobalsRegistry.Register(compatibility.DefaultKubeComponent, effectiveVersion, featureGate); err != nil {
		return result, err
	}
	opts.ComponentGlobalsRegistry = componentGlobalsRegistry

	nfs := opts.Flags
	for _, f := range nfs.FlagSets {
		fs.AddFlagSet(f)
	}
	fs.Parse(customFlags)
	if err := opts.ComponentGlobalsRegistry.Set(); err != nil {
		return result, err
	}
	// If the local ComponentGlobalsRegistry is changed by the flags,
	// we need to copy the new feature values back to the DefaultFeatureGate because most feature checks still use the DefaultFeatureGate.
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

	// 让内核分配空闲端口，避免测试依赖固定端口或与本机服务冲突；证书写入本测试临时目录。
	if opts.SecureServing.BindPort != 0 {
		opts.SecureServing.Listener, opts.SecureServing.BindPort, err = createListenerOnFreePort()
		if err != nil {
			return result, fmt.Errorf("failed to create listener: %v", err)
		}
		opts.SecureServing.ServerCert.CertDirectory = result.TmpDir

		logger.Info("kube-scheduler will listen securely", "port", opts.SecureServing.BindPort)
	}

	// 测试通过匿名 /healthz 轮询确认服务启动，因此将该路径加入授权白名单。
	opts.Authorization.AlwaysAllowPaths = append(opts.Authorization.AlwaysAllowPaths, "/healthz")

	cc, sched, err := app.Setup(ctx, opts)
	if err != nil {
		return result, fmt.Errorf("failed to create config from options: %v", err)
	}

	// app.Run 是阻塞调用；单独放入 goroutine，errCh 用于把提前退出或运行错误通知给启动轮询与清理逻辑。
	errCh = make(chan error)
	go func(ctx context.Context) {
		defer close(errCh)
		if err := app.Run(ctx, cc, sched); err != nil {
			errCh <- err
		}
	}(ctx)

	logger.Info("Waiting for /healthz to be ok...")

	// cc.KubeConfig 用于访问 Kubernetes API Server，并不是当前测试 Scheduler 的监听地址。
	// 因此使用 Scheduler 自己的服务端证书创建本地 HTTPS 客户端。
	// Cannot use cc.Kubeconfig since it is overwritten to point to kube-apiserver,
	// build our own client instead.
	serverCert, _ := cc.SecureServing.Cert.CurrentCertKeyContent()
	clientConfig, err := cc.SecureServing.NewClientConfig(serverCert)
	if err != nil {
		return result, fmt.Errorf("getting secure serving rest config %w", err)
	}

	// The server cert only includes IPv4 localhost, not IPv6.
	clientConfig.TLSClientConfig.ServerName = "127.0.0.1"
	client, err := kubernetes.NewForConfig(clientConfig)
	if err != nil {
		return result, fmt.Errorf("creating client: %w", err)
	}

	// 每 100ms 检查一次 /healthz，最多等待 30 秒；轮询期间也检查 Run goroutine 是否提前退出。
	err = wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 30*time.Second, false, func(ctx context.Context) (bool, error) {
		select {
		case err := <-errCh:
			return false, err
		default:
		}

		status := 0
		client.CoreV1().RESTClient().Get().AbsPath("/healthz").Do(ctx).StatusCode(&status)
		if status == 200 {
			return true, nil
		}

		return false, nil
	})
	if err != nil {
		return result, fmt.Errorf("failed to wait for /healthz to return ok: %v", err)
	}

	// /healthz 已返回成功，从这里开始清理责任交给调用方，必须调用 TearDownFn。
	result.Options = opts
	result.Config = cc.Config
	result.TearDownFn = tearDown

	return result, nil
}

// StartTestServerOrDie calls StartTestServer panic if it does not succeed.
func StartTestServerOrDie(t *testing.T, ctx context.Context, flags []string) *TestServer {
	result, err := StartTestServer(t, ctx, flags)
	if err != nil {
		t.Fatalf("failed to launch server: %v", err)
	}
	return &result
}

// createListenerOnFreePort 让内核选择可用 TCP 端口，并返回仍保持打开的 Listener，避免端口探测与绑定之间的抢占竞态。
func createListenerOnFreePort() (net.Listener, int, error) {
	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		return nil, 0, err
	}

	// get port
	tcpAddr, ok := ln.Addr().(*net.TCPAddr)
	if !ok {
		ln.Close()
		return nil, 0, fmt.Errorf("invalid listen address: %q", ln.Addr().String())
	}

	return ln, tcpAddr.Port, nil
}
