/*
Copyright 2017 The Kubernetes Authors.

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
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/spf13/pflag"
	"go.etcd.io/etcd/client/pkg/v3/transport"
	clientv3 "go.etcd.io/etcd/client/v3"
	"k8s.io/kubernetes/pkg/controlplane/apiserver/samples/generic/server"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/server/flagz"
	"k8s.io/apiserver/pkg/storage/storagebackend"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	cliflag "k8s.io/component-base/cli/flag"
	logsapi "k8s.io/component-base/logs/api/v1"
	zpagesfeatures "k8s.io/component-base/zpages/features"
	"k8s.io/klog/v2"
	controlplaneapiserver "k8s.io/kubernetes/pkg/controlplane/apiserver/options"
	"k8s.io/kubernetes/test/utils/ktesting"
)

// init 调整日志配置的重复应用策略，避免多个测试服务器实例冲突。
func init() {
	// 多个服务器实例会修改全局日志状态，测试中需忽略重复应用。
	logsapi.ReapplyHandling = logsapi.ReapplyHandlingIgnoreUnchanged
}

// 该密钥仅用于测试，不安全。
const ecdsaPrivateKey = `-----BEGIN EC PRIVATE KEY-----
MHcCAQEEIEZmTmUhuanLjPA2CLquXivuwBDHTt5XYwgIr/kA1LtRoAoGCCqGSM49
AwEHoUQDQgAEH6cuzP8XuD5wal6wf9M6xDljTOPLX2i8uIp/C/ASqiIGUeeKQtX0
/IR3qCXyThP/dbCiHrF3v1cuhBOHY8CLVg==
-----END EC PRIVATE KEY-----`

// TearDownFunc 用于清理测试服务器。
type TearDownFunc func()

// TestServerInstanceOptions 定义测试服务器实例选项。
type TestServerInstanceOptions struct {
	// SkipHealthzCheck 为 true 时不等待服务器健康检查通过，适用于预期 /healthz 无法完成的场景。
	SkipHealthzCheck bool
}

// TestServer 表示一个运行中的测试服务器，包含访问它及其后端 etcd 所需的一切。
type TestServer struct {
	ClientConfig      *restclient.Config             // Rest client config
	ServerOpts        *controlplaneapiserver.Options // ServerOpts
	TearDownFn        TearDownFunc                   // TearDown function
	TmpDir            string                         // Temp Dir used, by the apiserver
	EtcdClient        *clientv3.Client               // used by tests that need to check data migrated from APIs that are no longer served
	EtcdStoragePrefix string                         // storage prefix in etcd
}

// NewDefaultTestServerOptions 返回测试服务器实例的默认选项。
func NewDefaultTestServerOptions() *TestServerInstanceOptions {
	return &TestServerInstanceOptions{}
}

// StartTestServer 启动 etcd 与 sample-generic-controlplane，返回包含清理函数和访问客户端的 TestServer。
func StartTestServer(t ktesting.TB, instanceOptions *TestServerInstanceOptions, customFlags []string, storageConfig *storagebackend.Config) (result TestServer, err error) {
	tCtx := ktesting.Init(t)

	if instanceOptions == nil {
		instanceOptions = NewDefaultTestServerOptions()
	}

	result.TmpDir, err = os.MkdirTemp("", "sample-generic-controlplane")
	if err != nil {
		return result, fmt.Errorf("failed to create temp dir: %w", err)
	}

	var errCh chan error
	tearDown := func() {
		// 取消上下文会停止 apiserver 并清理其存储层。
		tCtx.Cancel("tearing down")

		// 若 apiserver 已启动，等待其干净关闭。
		if errCh != nil {
			err, ok := <-errCh
			if ok && err != nil {
				klog.Errorf("Failed to shutdown test server clearly: %v", err)
			}
		}
		os.RemoveAll(result.TmpDir) //nolint:errcheck // best effort
	}
	defer func() {
		if result.TearDownFn == nil {
			tearDown()
		}
	}()

	o := server.NewOptions()
	var fss cliflag.NamedFlagSets
	o.AddFlags(&fss)
	if utilfeature.DefaultFeatureGate.Enabled(zpagesfeatures.ComponentFlagz) {
		o.Flagz = flagz.NamedFlagSetsReader{FlagSets: fss}
	}

	fs := pflag.NewFlagSet("test", pflag.PanicOnError)
	for _, f := range fss.FlagSets {
		fs.AddFlagSet(f)
	}

	o.SecureServing.Listener, o.SecureServing.BindPort, err = createLocalhostListenerOnFreePort()
	if err != nil {
		return result, fmt.Errorf("failed to create listener: %w", err)
	}
	o.SecureServing.ServerCert.CertDirectory = result.TmpDir
	o.SecureServing.ExternalAddress = o.SecureServing.Listener.Addr().(*net.TCPAddr).IP // use listener addr although it is a loopback device

	pkgPath, err := pkgPath(t)
	if err != nil {
		return result, err
	}
	o.SecureServing.ServerCert.FixtureDirectory = filepath.Join(pkgPath, "testdata")

	o.Etcd.StorageConfig = *storageConfig
	utilruntime.Must(o.APIEnablement.RuntimeConfig.Set("api/all=true"))

	if err := fs.Parse(customFlags); err != nil {
		return result, err
	}

	saSigningKeyFile, err := os.CreateTemp("/tmp", "insecure_test_key")
	if err != nil {
		t.Fatalf("create temp file failed: %v", err)
	}
	defer os.RemoveAll(saSigningKeyFile.Name()) //nolint:errcheck // best effort
	if err = os.WriteFile(saSigningKeyFile.Name(), []byte(ecdsaPrivateKey), 0666); err != nil {
		t.Fatalf("write file %s failed: %v", saSigningKeyFile.Name(), err)
	}
	o.ServiceAccountSigningKeyFile = saSigningKeyFile.Name()
	o.Authentication.ServiceAccounts.Issuers = []string{"https://foo.bar.example.com"}
	o.Authentication.ServiceAccounts.KeyFiles = []string{saSigningKeyFile.Name()}

	completedOptions, err := o.Complete(tCtx, nil, nil)
	if err != nil {
		return result, fmt.Errorf("failed to set default ServerRunOptions: %w", err)
	}

	if errs := completedOptions.Validate(); len(errs) != 0 {
		return result, fmt.Errorf("failed to validate ServerRunOptions: %w", utilerrors.NewAggregate(errs))
	}

	t.Logf("runtime-config=%v", completedOptions.APIEnablement.RuntimeConfig)
	t.Logf("Starting sample-generic-controlplane on port %d...", o.SecureServing.BindPort)

	config, err := server.NewConfig(completedOptions)
	if err != nil {
		return result, err
	}
	completed, err := config.Complete()
	if err != nil {
		return result, err
	}
	s, err := server.CreateServerChain(completed)
	if err != nil {
		return result, fmt.Errorf("failed to create server chain: %w", err)
	}

	errCh = make(chan error)
	go func() {
		defer close(errCh)
		prepared, err := s.PrepareRun()
		if err != nil {
			errCh <- err
		} else if err := prepared.Run(tCtx); err != nil {
			errCh <- err
		}
	}()

	client, err := kubernetes.NewForConfig(s.GenericAPIServer.LoopbackClientConfig)
	if err != nil {
		return result, fmt.Errorf("failed to create a client: %w", err)
	}

	if !instanceOptions.SkipHealthzCheck {
		t.Logf("Waiting for /healthz to be ok...")

		// 轮询等待 /healthz 返回 ok。
		err = wait.PollUntilContextTimeout(tCtx, 100*time.Millisecond, wait.ForeverTestTimeout, true, func(ctx context.Context) (bool, error) {
			select {
			case err := <-errCh:
				return false, err
			default:
			}

			req := client.CoreV1().RESTClient().Get().AbsPath("/healthz")
			result := req.Do(ctx)
			status := 0
			result.StatusCode(&status)
			if status == 200 {
				return true, nil
			}
			return false, nil
		})
		if err != nil {
			return result, fmt.Errorf("failed to wait for /healthz to return ok: %w", err)
		}
	}

	// 等待 default namespace 被创建完成。
	err = wait.PollUntilContextTimeout(tCtx, 100*time.Millisecond, wait.ForeverTestTimeout, true, func(ctx context.Context) (bool, error) {
		select {
		case err := <-errCh:
			return false, err
		default:
		}

		if _, err := client.CoreV1().Namespaces().Get(ctx, "default", metav1.GetOptions{}); err != nil {
			if !errors.IsNotFound(err) {
				t.Logf("Unable to get default namespace: %v", err)
			}
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		return result, fmt.Errorf("failed to wait for default namespace to be created: %w", err)
	}

	tlsInfo := transport.TLSInfo{
		CertFile:      storageConfig.Transport.CertFile,
		KeyFile:       storageConfig.Transport.KeyFile,
		TrustedCAFile: storageConfig.Transport.TrustedCAFile,
	}
	tlsConfig, err := tlsInfo.ClientConfig()
	if err != nil {
		return result, err
	}
	etcdConfig := clientv3.Config{
		Endpoints:   storageConfig.Transport.ServerList,
		DialTimeout: 20 * time.Second,
		TLS:         tlsConfig,
	}
	etcdClient, err := clientv3.New(etcdConfig)
	if err != nil {
		return result, err
	}

	// 从这里开始调用方必须负责调用 tearDown 清理资源。
	result.ClientConfig = restclient.CopyConfig(s.GenericAPIServer.LoopbackClientConfig)
	result.ClientConfig.QPS = 1000
	result.ClientConfig.Burst = 10000
	result.ServerOpts = o
	result.TearDownFn = func() {
		tearDown()
		etcdClient.Close() //nolint:errcheck // best effort
	}
	result.EtcdClient = etcdClient
	result.EtcdStoragePrefix = storageConfig.Prefix

	return result, nil
}

// StartTestServerOrDie 调用 StartTestServer，失败时直接 t.Fatal。
func StartTestServerOrDie(t ktesting.TB, instanceOptions *TestServerInstanceOptions, flags []string, storageConfig *storagebackend.Config) *TestServer {
	result, err := StartTestServer(t, instanceOptions, flags, storageConfig)
	if err == nil {
		return &result
	}

	t.Fatalf("failed to launch server: %v", err)
	return nil
}

// createLocalhostListenerOnFreePort 在本地随机空闲端口上创建监听器。
func createLocalhostListenerOnFreePort() (net.Listener, int, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, 0, err
	}

	// 从监听器地址中取出实际分配的端口。
	tcpAddr, ok := ln.Addr().(*net.TCPAddr)
	if !ok {
		ln.Close() //nolint:errcheck // best effort
		return nil, 0, fmt.Errorf("invalid listen address: %q", ln.Addr().String())
	}

	return ln, tcpAddr.Port, nil
}

// pkgPath 返回本包目录的绝对路径。go test 下可直接看调用栈；bazel 使用 -trimpath 编译，需借助环境变量推导路径。
// 该方法对两者均适用，前提是仅 bazel 会传入 -trimpath。
func pkgPath(t ktesting.TB) (string, error) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		return "", fmt.Errorf("failed to get current file")
	}

	pkgPath := filepath.Dir(thisFile)

	// 发现 bazel 环境变量说明使用了 -trimpath，需从环境变量重建路径。
	if testSrcdir, testWorkspace := os.Getenv("TEST_SRCDIR"), os.Getenv("TEST_WORKSPACE"); testSrcdir != "" && testWorkspace != "" {
		t.Logf("Detected bazel env varaiables: TEST_SRCDIR=%q TEST_WORKSPACE=%q", testSrcdir, testWorkspace)
		pkgPath = filepath.Join(testSrcdir, testWorkspace, pkgPath)
	}

	// 路径仍非绝对说明有非 bazel 的编译过程传入了 -trimpath。
	if !filepath.IsAbs(pkgPath) {
		return "", fmt.Errorf("can't construct an absolute path from %q", pkgPath)
	}

	t.Logf("Resolved testserver package path to: %q", pkgPath)

	return pkgPath, nil
}
