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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/spf13/pflag"
	"go.etcd.io/etcd/client/pkg/v3/transport"
	clientv3 "go.etcd.io/etcd/client/v3"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/wait"
	genericfeatures "k8s.io/apiserver/pkg/features"
	"k8s.io/apiserver/pkg/server/flagz"
	serveroptions "k8s.io/apiserver/pkg/server/options"
	"k8s.io/apiserver/pkg/storage/storagebackend"
	"k8s.io/apiserver/pkg/storageversion"
	"k8s.io/apiserver/pkg/util/compatibility"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/util/cert"
	"k8s.io/client-go/util/keyutil"
	basecompatibility "k8s.io/component-base/compatibility"
	"k8s.io/component-base/featuregate"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	logsapi "k8s.io/component-base/logs/api/v1"
	zpagesfeatures "k8s.io/component-base/zpages/features"
	"k8s.io/klog/v2"
	"k8s.io/kube-aggregator/pkg/apiserver"
	"k8s.io/kubernetes/cmd/kube-apiserver/app"
	"k8s.io/kubernetes/cmd/kube-apiserver/app/options"
	"k8s.io/kubernetes/test/e2e/invariants/metrics"
	testutil "k8s.io/kubernetes/test/utils"
	"k8s.io/kubernetes/test/utils/ktesting"
)

func init() {
	// 多个测试服务器会重复修改全局日志状态，测试中应忽略未变化的配置。
	logsapi.ReapplyHandling = logsapi.ReapplyHandlingIgnoreUnchanged
}

// 该私钥仅用于测试，不能视为安全凭据。
const ecdsaPrivateKey = `-----BEGIN EC PRIVATE KEY-----
MHcCAQEEIEZmTmUhuanLjPA2CLquXivuwBDHTt5XYwgIr/kA1LtRoAoGCCqGSM49
AwEHoUQDQgAEH6cuzP8XuD5wal6wf9M6xDljTOPLX2i8uIp/C/ASqiIGUeeKQtX0
/IR3qCXyThP/dbCiHrF3v1cuhBOHY8CLVg==
-----END EC PRIVATE KEY-----`

// TearDownFunc 用于停止测试服务器并释放其资源，必须由调用方在测试结束时调用。
type TearDownFunc func()

// TestServerInstanceOptions 控制单个测试服务器的启动行为。
type TestServerInstanceOptions struct {
	// SkipHealthzCheck 跳过健康等待，适用于配置本身就阻止 /healthz 完成的测试。
	SkipHealthzCheck bool
	// DisableInvariantChecks 跳过测试结束时的指标不变量检查。
	DisableInvariantChecks bool
	// EnableCertAuth 为 kube-apiserver 启用证书认证。
	EnableCertAuth bool
	// StorageVersionWrapFunc 包装通用服务器的存储版本接口。
	StorageVersionWrapFunc func(storageversion.Manager) storageversion.Manager
	// ProxyCA 提供请求头认证使用的 CA，供以下连接共享：
	// 1. 本地 API 服务器因版本差异无法处理请求时，kube-apiserver 与对等节点的通信；
	// 2. kube-apiserver 与聚合 API 服务器的通信。
	// 多个服务器共用同一 ProxyCA 可模拟版本差异下的一致性客户端验证。
	ProxyCA *ProxyCA
	// BinaryVersion 设置服务器有效版本中的二进制版本；为空时使用 DefaultKubeEffectiveVersion。
	BinaryVersion string
	// RequestTimeout 覆盖服务器默认的请求超时时间。
	RequestTimeout time.Duration
}

// TestServer 保存启动测试 API 服务器后返回的客户端、配置和清理信息。
type TestServer struct {
	ClientConfig      *restclient.Config        // REST 客户端配置
	ServerOpts        *options.ServerRunOptions // 服务器启动参数
	TearDownFn        TearDownFunc              // 清理函数
	TmpDir            string                    // API 服务器使用的临时目录
	EtcdClient        *clientv3.Client          // 供测试检查已不再提供的 API 所迁移的数据
	EtcdStoragePrefix string                    // etcd 存储前缀
}

// Logger 是 StartTestServer 和 StartTestServerOrDie 所需的最小测试日志接口。
type Logger interface {
	Helper()
	Errorf(format string, args ...interface{})
	Fatalf(format string, args ...interface{})
	Logf(format string, args ...interface{})
	Cleanup(func())
}

// ProxyCA 保存用于验证 kube-apiserver 客户端连接的 CA 证书和私钥。
// 客户端包括聚合 API 服务器和对等 kube-apiserver。
type ProxyCA struct {
	// ProxySigningCert 是签发代理客户端证书的 CA 证书。
	ProxySigningCert *x509.Certificate
	// ProxySigningKey 是与 ProxySigningCert 配对的 CA 私钥。
	ProxySigningKey *rsa.PrivateKey
}

// NewDefaultTestServerOptions 创建测试服务器的默认实例选项。
func NewDefaultTestServerOptions() *TestServerInstanceOptions {
	return &TestServerInstanceOptions{
		EnableCertAuth: true,
	}
}

// StartTestServer 启动 etcd 和 kube-apiserver，并返回 REST 客户端配置、清理函数和临时目录位置。
// 返回清理函数而不是停止通道，是因为 Go 测试调用 os.Exit 时不会给停止通道的 goroutine
// 足够时间删除临时文件。
func StartTestServer(t ktesting.TB, instanceOptions *TestServerInstanceOptions, customFlags []string, storageConfig *storagebackend.Config) (result TestServer, err error) {
	// 部分调用方可能已初始化 ktesting。
	tCtx, ok := t.(ktesting.TContext)
	if ok {
		// 派生可取消的子上下文，避免下方的 Cancel 影响共享该 TContext 的其他代码。
		tCtx = tCtx.WithCancel()
	} else {
		tCtx = ktesting.Init(t)
	}

	if instanceOptions == nil {
		instanceOptions = NewDefaultTestServerOptions()
	}

	result.TmpDir, err = os.MkdirTemp("", "kubernetes-kube-apiserver")
	if err != nil {
		return result, fmt.Errorf("failed to create temp dir: %v", err)
	}

	var errCh chan error
	tearDown := func() {
		// 取消上下文以停止 API 服务器及其存储层。
		tCtx.Cancel("tearing down")

		// 若服务器已启动，等待其完全退出。
		if errCh != nil {
			err, ok := <-errCh
			if ok && err != nil {
				klog.Errorf("Failed to shutdown test server clearly: %v", err)
			}
		}
		os.RemoveAll(result.TmpDir)
	}
	defer func() {
		if result.TearDownFn == nil {
			tearDown()
		}
	}()

	fs := pflag.NewFlagSet("test", pflag.PanicOnError)

	featureGate := utilfeature.DefaultMutableFeatureGate.DeepCopy()
	effectiveVersion := compatibility.DefaultKubeEffectiveVersionForTest()
	if instanceOptions.BinaryVersion != "" {
		effectiveVersion = basecompatibility.NewEffectiveVersionFromString(instanceOptions.BinaryVersion, "", "")
	}
	effectiveVersion.SetEmulationVersion(featureGate.EmulationVersion())
	effectiveVersion.SetMinCompatibilityVersion(featureGate.MinCompatibilityVersion())
	componentGlobalsRegistry := basecompatibility.NewComponentGlobalsRegistry()
	if err := componentGlobalsRegistry.Register(basecompatibility.DefaultKubeComponent, effectiveVersion, featureGate); err != nil {
		return result, err
	}

	s := options.NewServerRunOptions()
	if !effectiveVersion.BinaryVersion().EqualTo(effectiveVersion.EmulationVersion()) {
		// 显式启用的特性可能依赖版本模拟时默认关闭的 API，因此允许前向兼容的新 API。
		s.GenericServerRunOptions.RuntimeConfigEmulationForwardCompatible = true
	}

	// 使用独立注册表，避免并行测试争用 DefaultComponentGlobalsRegistry。
	s.Options.GenericServerRunOptions.ComponentGlobalsRegistry = componentGlobalsRegistry
	if instanceOptions.RequestTimeout > 0 {
		s.GenericServerRunOptions.RequestTimeout = instanceOptions.RequestTimeout
	}

	namedFlagSets := s.Flags()
	for _, f := range namedFlagSets.FlagSets {
		fs.AddFlagSet(f)
	}

	s.SecureServing.Listener, s.SecureServing.BindPort, err = createLocalhostListenerOnFreePort()
	if err != nil {
		return result, fmt.Errorf("failed to create listener: %v", err)
	}
	s.SecureServing.ServerCert.CertDirectory = result.TmpDir

	reqHeaderFromFlags := s.Authentication.RequestHeader
	if instanceOptions.EnableCertAuth {
		// 为请求头认证设置默认配置。
		reqHeaders := serveroptions.NewDelegatingAuthenticationOptions()
		s.Authentication.RequestHeader = &reqHeaders.RequestHeader

		var proxySigningKey *rsa.PrivateKey
		var proxySigningCert *x509.Certificate

		if instanceOptions.ProxyCA != nil {
			// 复用调用方提供的 ProxyCA。
			proxySigningKey = instanceOptions.ProxyCA.ProxySigningKey
			proxySigningCert = instanceOptions.ProxyCA.ProxySigningCert

		} else {
			// 创建供聚合和客户端证书认证使用的 CA。
			proxySigningKey, err = testutil.NewPrivateKey()
			if err != nil {
				return result, err
			}
			proxySigningCert, err = cert.NewSelfSignedCACert(cert.Config{CommonName: "front-proxy-ca"}, proxySigningKey)
			if err != nil {
				return result, err
			}
		}
		proxyCACertFile := filepath.Join(s.SecureServing.ServerCert.CertDirectory, "proxy-ca.crt")
		if err := os.WriteFile(proxyCACertFile, testutil.EncodeCertPEM(proxySigningCert), 0644); err != nil {
			return result, err
		}
		s.Authentication.RequestHeader.ClientCAFile = proxyCACertFile

		// 为 API 服务器设置请求头认证身份，使聚合服务器能够识别调用用户。
		s.Authentication.RequestHeader.AllowedNames = []string{"ash", "misty", "brock"}

		signer, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			return result, err
		}

		// 客户端证书的 CommonName 必须匹配上面允许的名称。
		serial, err := rand.Int(rand.Reader, new(big.Int).SetInt64(math.MaxInt64-1))
		if err != nil {
			return result, err
		}
		serial = new(big.Int).Add(serial, big.NewInt(1))
		tenThousandHoursLater := time.Now().Add(10_000 * time.Hour)
		certTmpl := x509.Certificate{
			Subject: pkix.Name{
				CommonName: "misty",
			},
			SerialNumber: serial,
			NotBefore:    proxySigningCert.NotBefore,
			NotAfter:     tenThousandHoursLater,
			KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
			ExtKeyUsage: []x509.ExtKeyUsage{
				x509.ExtKeyUsageClientAuth,
			},
			BasicConstraintsValid: true,
		}
		certDERBytes, err := x509.CreateCertificate(rand.Reader, &certTmpl, proxySigningCert, signer.Public(), proxySigningKey)
		if err != nil {
			return result, err
		}
		clientCrtOfAPIServer, err := x509.ParseCertificate(certDERBytes)
		if err != nil {
			return result, err
		}

		certificatePath := filepath.Join(s.SecureServing.ServerCert.CertDirectory, "misty-crt.crt")
		certBlock := pem.Block{
			Type:  "CERTIFICATE",
			Bytes: clientCrtOfAPIServer.Raw,
		}
		certBytes := pem.EncodeToMemory(&certBlock)
		if err := cert.WriteCert(certificatePath, certBytes); err != nil {
			return result, err
		}

		privateKeyPath := filepath.Join(s.SecureServing.ServerCert.CertDirectory, "misty-crt.key")
		encodedPrivateKey, err := keyutil.MarshalPrivateKeyToPEM(signer)
		if err != nil {
			return result, err
		}
		if err := keyutil.WriteKey(privateKeyPath, encodedPrivateKey); err != nil {
			return result, err
		}

		s.ProxyClientKeyFile = filepath.Join(s.SecureServing.ServerCert.CertDirectory, "misty-crt.key")
		s.ProxyClientCertFile = filepath.Join(s.SecureServing.ServerCert.CertDirectory, "misty-crt.crt")

		clientSigningKey, err := testutil.NewPrivateKey()
		if err != nil {
			return result, err
		}
		clientSigningCert, err := cert.NewSelfSignedCACert(cert.Config{CommonName: "client-ca"}, clientSigningKey)
		if err != nil {
			return result, err
		}
		clientCACertFile := filepath.Join(s.SecureServing.ServerCert.CertDirectory, "client-ca.crt")
		if err := os.WriteFile(clientCACertFile, testutil.EncodeCertPEM(clientSigningCert), 0644); err != nil {
			return result, err
		}
		s.Authentication.ClientCert.ClientCA = clientCACertFile
	}

	s.SecureServing.ExternalAddress = s.SecureServing.Listener.Addr().(*net.TCPAddr).IP // 虽为回环设备，仍使用监听器地址。

	pkgPath, err := pkgPath(t)
	if err != nil {
		return result, err
	}
	s.SecureServing.ServerCert.FixtureDirectory = filepath.Join(pkgPath, "testdata")

	s.ServiceClusterIPRanges = "10.0.0.0/16"
	s.Etcd.StorageConfig = *storageConfig

	if err := fs.Parse(customFlags); err != nil {
		return result, err
	}

	// 回环“外部”地址无法被集群访问；除非用户显式覆盖协调器或公布地址，否则关闭端点协调。
	if !fs.Changed("advertise-address") && !fs.Changed("endpoint-reconciler-type") {
		s.EndpointReconcilerType = "none"
	}
	if utilfeature.DefaultFeatureGate.Enabled(zpagesfeatures.ComponentFlagz) {
		s.Flagz = flagz.NamedFlagSetsReader{FlagSets: namedFlagSets}
	}

	// EnableCertAuth 会替换 RequestHeader 指针，使参数绑定到已不在 ServerOptions 中的结构体。
	// 为保留命令行配置能力，仍需把请求头字段复制到新的认证配置。
	if usernameHeaders := reqHeaderFromFlags.UsernameHeaders; len(usernameHeaders) > 0 {
		s.Authentication.RequestHeader.UsernameHeaders = usernameHeaders
	}
	if uidHeaders := reqHeaderFromFlags.UIDHeaders; len(uidHeaders) > 0 {
		s.Authentication.RequestHeader.UIDHeaders = uidHeaders
	}
	if groupHeaders := reqHeaderFromFlags.GroupHeaders; len(groupHeaders) > 0 {
		s.Authentication.RequestHeader.GroupHeaders = groupHeaders
	}
	if extraHeaders := reqHeaderFromFlags.ExtraHeaderPrefixes; len(extraHeaders) > 0 {
		s.Authentication.RequestHeader.ExtraHeaderPrefixes = extraHeaders
	}

	if err := componentGlobalsRegistry.Set(); err != nil {
		return result, fmt.Errorf("%w\nIf you are using SetFeatureGate*DuringTest, try using --emulated-version and --feature-gates flags instead", err)
	}
	// 多数特性检查仍读取 DefaultFeatureGate，因此需把参数对本地注册表的修改同步回去。
	// 不能直接向注册表注入 DefaultFeatureGate，因为 Set() 的修改不会在测试结束时自动撤销。
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
	utilfeature.DefaultMutableFeatureGate.AddMetrics()

	if instanceOptions.EnableCertAuth {
		if featureGate.Enabled(genericfeatures.UnknownVersionInteroperabilityProxy) {
			s.PeerCAFile = filepath.Join(s.SecureServing.ServerCert.CertDirectory, s.SecureServing.ServerCert.PairName+".crt")
		}
	}

	saSigningKeyFile, err := os.CreateTemp("/tmp", "insecure_test_key")
	if err != nil {
		t.Fatalf("create temp file failed: %v", err)
	}
	defer os.RemoveAll(saSigningKeyFile.Name())
	if err = os.WriteFile(saSigningKeyFile.Name(), []byte(ecdsaPrivateKey), 0666); err != nil {
		t.Fatalf("write file %s failed: %v", saSigningKeyFile.Name(), err)
	}
	s.ServiceAccountSigningKeyFile = saSigningKeyFile.Name()
	s.Authentication.ServiceAccounts.Issuers = []string{"https://foo.bar.example.com"}
	s.Authentication.ServiceAccounts.KeyFiles = []string{saSigningKeyFile.Name()}

	completedOptions, err := s.Complete(tCtx)
	if err != nil {
		return result, fmt.Errorf("failed to set default ServerRunOptions: %v", err)
	}

	if errs := completedOptions.Validate(); len(errs) != 0 {
		return result, fmt.Errorf("failed to validate ServerRunOptions: %v", utilerrors.NewAggregate(errs))
	}

	t.Logf("runtime-config=%v", completedOptions.APIEnablement.RuntimeConfig)
	t.Logf("Starting kube-apiserver on port %d...", s.SecureServing.BindPort)

	config, err := app.NewConfig(completedOptions)
	if err != nil {
		return result, err
	}
	completed, err := config.Complete()
	if err != nil {
		return result, err
	}
	server, err := app.CreateServerChain(completed)
	if err != nil {
		return result, fmt.Errorf("failed to create server chain: %v", err)
	}
	if instanceOptions.StorageVersionWrapFunc != nil {
		server.GenericAPIServer.StorageVersionManager = instanceOptions.StorageVersionWrapFunc(server.GenericAPIServer.StorageVersionManager)
	}

	errCh = make(chan error)
	go func() {
		defer close(errCh)
		prepared, err := server.PrepareRun()
		if err != nil {
			errCh <- err
		} else if err := prepared.Run(tCtx); err != nil {
			errCh <- err
		}
	}()

	client, err := kubernetes.NewForConfig(server.GenericAPIServer.LoopbackClientConfig)
	if err != nil {
		return result, fmt.Errorf("failed to create a client: %v", err)
	}

	if !instanceOptions.SkipHealthzCheck {
		t.Logf("Waiting for /healthz to be ok...")

		// 等待健康端点返回成功。
		err = wait.Poll(100*time.Millisecond, time.Minute, func() (bool, error) {
			select {
			case err := <-errCh:
				return false, err
			default:
			}

			req := client.CoreV1().RESTClient().Get().AbsPath("/healthz")
			// 存储版本引导测试会包装 post-start hook，导致该 hook 在服务器引导期间保持不健康。
			if instanceOptions.StorageVersionWrapFunc != nil {
				// 直接硬编码排除项，避免为测试专用行为增加公共配置字段。
				storageVersionCheck := fmt.Sprintf("poststarthook/%s", apiserver.StorageVersionPostStartHookName)
				req.Param("exclude", storageVersionCheck)
			}
			result := req.Do(context.TODO())
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
	}

	// 等待默认命名空间创建完成。
	err = wait.Poll(100*time.Millisecond, 30*time.Second, func() (bool, error) {
		select {
		case err := <-errCh:
			return false, err
		default:
		}

		if _, err := client.CoreV1().Namespaces().Get(context.TODO(), "default", metav1.GetOptions{}); err != nil {
			if !errors.IsNotFound(err) {
				t.Logf("Unable to get default namespace: %v", err)
			}
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		return result, fmt.Errorf("failed to wait for default namespace to be created: %v", err)
	}

	etcdClient, _, err := GetEtcdClients(storageConfig.Transport)
	if err != nil {
		return result, fmt.Errorf("create etcd client: %w", err)
	}

	// 至此启动成功，调用方必须调用 TearDownFn 清理资源。
	result.ClientConfig = restclient.CopyConfig(server.GenericAPIServer.LoopbackClientConfig)
	result.ClientConfig.QPS = 1000
	result.ClientConfig.Burst = 10000
	result.ServerOpts = s
	result.TearDownFn = func() {
		defer func() {
			if err := etcdClient.Close(); err != nil {
				tCtx.Errorf("Failed to close etcd client: %v", err)
			}
		}()
		defer tearDown()
		if instanceOptions.DisableInvariantChecks {
			return
		}
		if tCtx.Err() != nil {
			tCtx.Logf("Skipping metrics scrape because context is already canceled: %v", tCtx.Err())
			return
		}
		if err := metrics.CheckMetricInvariants(tCtx, client, false); err != nil {
			tCtx.Errorf("Invariant check failed (if the test intentionally breaks metrics/auth, consider setting DisableInvariantChecks: true in TestServerInstanceOptions): %v", err)
		}
	}
	result.EtcdClient = etcdClient
	result.EtcdStoragePrefix = storageConfig.Prefix

	return result, nil
}

// GetEtcdClients 返回已初始化的 etcd 客户端和 KV 接口。
func GetEtcdClients(config storagebackend.TransportConfig) (*clientv3.Client, clientv3.KV, error) {
	// clientv3.New 对 http:// 忽略无效 TLS 配置，但对 unix:// 会报错
	//（https://github.com/etcd-io/etcd/blob/5a8fba466087686fc15815f5bc041fb7eb1f23ea/client/v3/internal/endpoint/endpoint.go#L61-L66）。
	// 因此只有确实启用传输安全时才设置 Config.TLS，以兼容 unix://。
	var tlsConfig *tls.Config
	if config.CertFile != "" ||
		config.KeyFile != "" ||
		config.TrustedCAFile != "" {
		tlsInfo := transport.TLSInfo{
			CertFile:      config.CertFile,
			KeyFile:       config.KeyFile,
			TrustedCAFile: config.TrustedCAFile,
		}

		var err error
		tlsConfig, err = tlsInfo.ClientConfig()
		if err != nil {
			return nil, nil, err
		}
	}

	cfg := clientv3.Config{
		Endpoints:   config.ServerList,
		DialTimeout: 20 * time.Second,
		TLS:         tlsConfig,
	}

	c, err := clientv3.New(cfg)
	if err != nil {
		return nil, nil, err
	}

	return c, clientv3.NewKV(c), nil
}

// StartTestServerOrDie 启动测试服务器；启动失败时通过 t.Fatal 终止测试。
func StartTestServerOrDie(t testing.TB, instanceOptions *TestServerInstanceOptions, flags []string, storageConfig *storagebackend.Config) *TestServer {
	result, err := StartTestServer(t, instanceOptions, flags, storageConfig)
	if err == nil {
		return &result
	}

	t.Fatalf("failed to launch server: %v", err)
	return nil
}

func createLocalhostListenerOnFreePort() (net.Listener, int, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, 0, err
	}

	// 读取内核分配的端口。
	tcpAddr, ok := ln.Addr().(*net.TCPAddr)
	if !ok {
		ln.Close()
		return nil, 0, fmt.Errorf("invalid listen address: %q", ln.Addr().String())
	}

	return ln, tcpAddr.Port, nil
}

// pkgPath 返回当前包目录的绝对路径。go test 可直接从运行时调用栈获取源码路径；
// Bazel 编译时使用 -trimpath，该路径失效，因此改为从环境变量推导。
// 这里假设只有 Bazel 构建会传入 -trimpath，从而同时兼容 go test 和 Bazel。
func pkgPath(t Logger) (string, error) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		return "", fmt.Errorf("failed to get current file")
	}

	pkgPath := filepath.Dir(thisFile)

	// 发现 Bazel 环境变量说明构建使用了 -trimpath，需基于环境变量重建路径。
	if testSrcdir, testWorkspace := os.Getenv("TEST_SRCDIR"), os.Getenv("TEST_WORKSPACE"); testSrcdir != "" && testWorkspace != "" {
		t.Logf("Detected bazel env varaiables: TEST_SRCDIR=%q TEST_WORKSPACE=%q", testSrcdir, testWorkspace)
		pkgPath = filepath.Join(testSrcdir, testWorkspace, pkgPath)
	}

	// 若路径仍不是绝对路径，则构建方并非带 -trimpath 的 Bazel。
	if !filepath.IsAbs(pkgPath) {
		return "", fmt.Errorf("can't construct an absolute path from %q", pkgPath)
	}

	t.Logf("Resolved testserver package path to: %q", pkgPath)

	return pkgPath, nil
}
