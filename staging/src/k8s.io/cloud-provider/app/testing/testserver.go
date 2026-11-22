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

// Package testing 提供在测试中启动真实 cloud-controller-manager 的辅助函数，
// 便于端到端验证命令行解析、控制器装配与 webhook 监听等组合行为。
package testing

import (
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	cloudprovider "k8s.io/cloud-provider"
	"k8s.io/cloud-provider/app"
	"k8s.io/cloud-provider/app/config"
	"k8s.io/cloud-provider/names"
	"k8s.io/cloud-provider/options"
	cliflag "k8s.io/component-base/cli/flag"
	"k8s.io/component-base/configz"
	logsapi "k8s.io/component-base/logs/api/v1"
	"k8s.io/klog/v2"
)

func init() {
	// If instantiated more than once or together with other servers, the
	// servers would try to modify the global logging state. This must get
	// ignored during testing.
	// 测试中可能同时启动多个服务端，忽略未变化的日志重配置可避免全局状态冲突。
	logsapi.ReapplyHandling = logsapi.ReapplyHandlingIgnoreUnchanged

	// Because the test server gets started after other goroutines are
	// running already, we also have to initialize logging here when
	// those goroutines are not running yet. This works because the
	// test server uses the default config.
	// 启动服务端时已有 goroutine 在运行，无法再安全地重新初始化日志，
	// 因此在 init 阶段用默认配置完成一次初始化。
	config := logsapi.NewLoggingConfiguration()
	if err := logsapi.ValidateAndApply(config, nil); err != nil {
		panic(err)
	}
}

// TearDownFunc is to be called to tear down a test server.
// 调用方必须在用完 TestServer 后执行该函数，否则监听端口与临时文件会泄漏。
type TearDownFunc func()

// TestServer return values supplied by kube-test-ApiServer
// TestServer 汇总测试用 cloud-controller-manager 的连接信息与清理入口。
type TestServer struct {
	LoopbackClientConfig *restclient.Config                     // Rest client config using the magic token; 使用特权回环 token，测试可直接调用服务端内部 API。
	Options              *options.CloudControllerManagerOptions // 实际生效的选项，供测试断言 flag 解析结果。
	Config               *config.CompletedConfig                // 捕获到的已完成配置，可用于检查组件配置与客户端装配。
	TearDownFn           TearDownFunc                           // TearDown function; 用例结束必须调用，否则端口与临时文件会泄漏。
	TmpDir               string                                 // Temp Dir used, by the apiserver
}

// StartTestServer starts a cloud-controller-manager. A rest client config and a tear-down func,
// and location of the tmpdir are returned.
//
// Note: we return a tear-down func instead of a stop channel because the later will leak temporary
// files that because Golang testing's call to os.Exit will not give a stop channel go routine
// enough time to remove temporary files.
// StartTestServer 使用默认控制器集合与别名启动测试服务端。
func StartTestServer(t *testing.T, ctx context.Context, customFlags []string) (result TestServer, err error) {
	return StartTestServerWithOptions(t, ctx, customFlags, app.DefaultInitFuncConstructors, names.CCMControllerAliases())
}

// StartTestServerWithOptions starts a cloud-controller-manager with extra controller loop initializer constructors
// and controller loop aliaes. Within the returned TestServer, a rest client config, a tear-down func,
// and location of the tmpdir are returned; or an error if one occurred.
// 允许调用方注入自定义控制器构造器与别名，用于测试扩展控制器的装配路径。
func StartTestServerWithOptions(t *testing.T,
	ctx context.Context,
	customFlags []string,
	constructors map[string]app.ControllerInitFuncConstructor,
	aliases map[string]string) (result TestServer, err error) {
	logger := klog.FromContext(ctx)
	// stopCh 用于通知命令退出；errCh 传递命令执行错误；configDoneCh 标记
	// 云初始化函数已经收到完成后的配置，供主 goroutine 与初始化回调同步。
	stopCh := make(chan struct{})
	var errCh chan error
	configDoneCh := make(chan struct{})
	var capturedConfig config.CompletedConfig
	tearDown := func() {
		close(stopCh)

		// If cloud-controller-manager was started, let's wait for
		// it to shutdown clearly.
		// 等待命令 goroutine 返回，确保优雅关闭完成后再清理 configz。
		if errCh != nil {
			err, ok := <-errCh
			if ok && err != nil {
				klog.Errorf("Failed to shutdown test server clearly: %v", err)
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

	// Do not run leader election.
	// 测试中只运行一个实例，禁用选主可避免等待 lease 获取而拖慢启动。
	s, err := options.NewCloudControllerManagerOptions()
	if err != nil {
		return TestServer{}, err
	}
	s.Generic.LeaderElection.LeaderElect = false

	cloudInitializer := func(config *config.CompletedConfig) cloudprovider.Interface {
		// 捕获命令实际使用的配置，并通知启动流程可以继续。
		capturedConfig = *config
		// send signal to indicate the capturedConfig has been properly set
		close(configDoneCh)
		cloudConfig := config.ComponentConfig.KubeCloudShared.CloudProvider
		cloud, err := cloudprovider.InitCloudProvider(cloudConfig.Name, cloudConfig.CloudConfigFile)
		if err != nil {
			panic(fmt.Errorf("Cloud provider could not be initialized: %v", err))
		}
		s.SecureServing.ServerCert.CertDirectory = result.TmpDir
		if cloud == nil {
			panic("Cloud provider is nil")
		}
		return cloud
	}
	fss := cliflag.NamedFlagSets{}
	// 用真实的命令构造逻辑启动，以覆盖 flag、配置校验与控制器装配等完整路径。
	command := app.NewCloudControllerManagerCommand(s, cloudInitializer, constructors, aliases, fss, stopCh)

	commandArgs := []string{}
	listeners := []net.Listener{}
	disableSecure := false
	webhookServing := false
	for _, arg := range customFlags {
		// This block collects all custom flags other than secure serving flags,
		// which are added after creating a listener.
		// 安全监听端口需要先占用空闲端口再回填 flag，因此这里把相关 flag 单独筛出；
		// --secure-port=0 表示显式禁用安全端口，直接透传。
		if strings.HasPrefix(arg, "--secure-port=") || strings.HasPrefix(arg, "--cert-dir=") {
			if arg == "--secure-port=0" {
				commandArgs = append(commandArgs, arg)
				disableSecure = true
			}
		} else if strings.HasPrefix(arg, "--webhook-secure-port=") || strings.HasPrefix(arg, "--webhook-cert-dir=") {
			if arg == "--webhook-secure-port=0" {
				commandArgs = append(commandArgs, arg)
				webhookServing = false
			} else {
				webhookServing = true
			}
		} else {
			commandArgs = append(commandArgs, arg)
		}
	}

	if !disableSecure {
		// 先监听 :0 获取空闲端口再关闭，避免测试并行运行时端口冲突。
		listener, bindPort, err := createListenerOnFreePort()
		if err != nil {
			return result, fmt.Errorf("failed to create listener: %v", err)
		}
		listeners = append(listeners, listener)
		commandArgs = append(commandArgs, fmt.Sprintf("--secure-port=%d", bindPort))
		commandArgs = append(commandArgs, fmt.Sprintf("--cert-dir=%s", result.TmpDir))

		logger.Info("cloud-controller-manager will listen securely", "port", bindPort)
	}

	if webhookServing {
		// webhook 使用独立端口，同样先探测空闲端口。
		listener, bindPort, err := createListenerOnFreePort()
		if err != nil {
			return result, fmt.Errorf("failed to create listener: %v", err)
		}
		listeners = append(listeners, listener)
		commandArgs = append(commandArgs, fmt.Sprintf("--webhook-secure-port=%d", bindPort))
		commandArgs = append(commandArgs, fmt.Sprintf("--webhook-cert-dir=%s", result.TmpDir))

		logger.Info("cloud-controller-manager (webhook endpoint) will listen securely", "port", bindPort)
	}

	// 探测用的监听器必须在命令启动前关闭，否则命令无法绑定同一端口。
	for _, listener := range listeners {
		listener.Close()
	}

	// 命令在后台 goroutine 中运行，错误通过 errCh 回传。
	errCh = make(chan error)
	go func() {
		defer close(errCh)

		command.SetArgs(commandArgs)
		if err := command.Execute(); err != nil {
			errCh <- err
		}
	}()

	select {
	case <-configDoneCh:
		// 配置已捕获，继续等待 /healthz 就绪。

	case err := <-errCh:
		return result, err
	}

	logger.Info("Waiting for /healthz to be ok...")
	// 使用回环客户端轮询 /healthz，确认服务端、认证与 informer 均已就绪。
	client, err := kubernetes.NewForConfig(capturedConfig.LoopbackClientConfig)
	if err != nil {
		return result, fmt.Errorf("failed to create a client: %v", err)
	}
	err = wait.Poll(100*time.Millisecond, 30*time.Second, func() (bool, error) {
		// 轮询期间命令若提前退出，应立即返回其错误而不是等到超时。
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

	// from here the caller must call tearDown
	result.LoopbackClientConfig = capturedConfig.LoopbackClientConfig
	result.Options = s
	result.Config = &capturedConfig
	result.TearDownFn = tearDown

	return result, nil
}

// StartTestServerOrDie calls StartTestServer panic if it does not succeed.
// StartTestServerOrDie 在启动失败时直接终止当前测试，适合不需要断言错误的场景。
func StartTestServerOrDie(t *testing.T, ctx context.Context, flags []string) *TestServer {
	result, err := StartTestServer(t, ctx, flags)
	if err != nil {
		t.Fatalf("failed to launch server: %v", err)
	}
	return &result
}

// createListenerOnFreePort 通过监听 :0 让内核分配空闲端口，并连同监听器一起返回；
// 调用方需负责关闭监听器，通常仅用于探测端口号。
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
