/*
Copyright 2014 The Kubernetes Authors.

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
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	cadvisorapi "github.com/google/cadvisor/lib/model"
	gwebsocket "github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/component-base/configz"
	"k8s.io/kubelet/config/v1beta1"
	"k8s.io/utils/ptr"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/version"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	remotecommand "k8s.io/client-go/tools/remotecommand"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	statsapi "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/test/utils/ktesting"
	"k8s.io/streaming/pkg/httpstream"
	"k8s.io/streaming/pkg/httpstream/spdy"

	// Do some initialization to decode the query parameters correctly.
	"k8s.io/apiserver/pkg/server/dynamiccertificates"
	"k8s.io/apiserver/pkg/server/flagz"
	"k8s.io/apiserver/pkg/server/healthz"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	metricsfeatures "k8s.io/component-base/metrics/features"
	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/component-base/metrics/testutil"
	zpagesfeatures "k8s.io/component-base/zpages/features"
	"k8s.io/cri-streaming/pkg/streaming"
	"k8s.io/cri-streaming/pkg/streaming/portforward"
	remotecommandserver "k8s.io/cri-streaming/pkg/streaming/remotecommand"
	_ "k8s.io/kubernetes/pkg/apis/core/install"
	"k8s.io/kubernetes/pkg/features"
	kubeletconfiginternal "k8s.io/kubernetes/pkg/kubelet/apis/config"
	"k8s.io/kubernetes/pkg/kubelet/cm"
	kubeletmetrics "k8s.io/kubernetes/pkg/kubelet/metrics"
	servermetrics "k8s.io/kubernetes/pkg/kubelet/server/metrics"
	"k8s.io/kubernetes/pkg/kubelet/server/stats"
	"k8s.io/kubernetes/pkg/volume"
)

const (
	// 流式请求测试使用的固定 Pod UID、容器 ID 与 sandbox ID。
	testUID          = "9b01b80f-8fb4-11e4-95ab-4200af06647"
	testContainerID  = "container789"
	testPodSandboxID = "pod0987"
)

// fakeKubelet 通过可替换的函数字段模拟 kubelet 的全部依赖行为，
// 测试按需注入期望的返回值与断言逻辑。
type fakeKubelet struct {
	podByNameFunc          func(namespace, name string) (*v1.Pod, bool)
	machineInfoFunc        func() (*cadvisorapi.MachineInfo, error)
	podsFunc               func() []*v1.Pod
	runningPodsFunc        func(ctx context.Context) ([]*v1.Pod, error)
	logFunc                func(w http.ResponseWriter, req *http.Request)
	runFunc                func(podFullName string, uid types.UID, containerName string, cmd []string) ([]byte, error)
	getExecCheck           func(string, types.UID, string, []string, remotecommandserver.Options)
	getAttachCheck         func(string, types.UID, string, remotecommandserver.Options)
	getPortForwardCheck    func(string, string, types.UID, portforward.V4Options)
	allocatedPodsFunc      func() []*v1.Pod
	allocatedPodByNameFunc func(namespace, name string) *v1.Pod

	containerLogsFunc func(ctx context.Context, podFullName, containerName string, logOptions *v1.PodLogOptions, stdout, stderr io.Writer) error
	resyncInterval    time.Duration
	loopEntryTime     time.Time
	plegHealth        bool
	streamingRuntime  streaming.Server
}

// 返回 fake kubelet 记录的最近一次 syncLoop 进入时间。
func (fk *fakeKubelet) LatestLoopEntryTime() time.Time {
	return fk.loopEntryTime
}

// 按 namespace/name 查询 Pod，实际逻辑委托给注入的 podByNameFunc。
func (fk *fakeKubelet) GetPodByName(namespace, name string) (*v1.Pod, bool) {
	return fk.podByNameFunc(namespace, name)
}

// 测试中不需要真实 cAdvisor 数据，返回空 map。
func (fk *fakeKubelet) GetRequestedContainersInfo(containerName string, options cadvisorapi.RequestOptions) (map[string]*cadvisorapi.ContainerInfo, error) {
	return map[string]*cadvisorapi.ContainerInfo{}, nil
}

// 返回注入的 machineInfoFunc 结果。
func (fk *fakeKubelet) GetCachedMachineInfo() (*cadvisorapi.MachineInfo, error) {
	return fk.machineInfoFunc()
}

// 测试中返回空的版本信息。
func (*fakeKubelet) GetVersionInfo() (*cadvisorapi.VersionInfo, error) {
	return &cadvisorapi.VersionInfo{}, nil
}

// 返回注入的 podsFunc 结果。
func (fk *fakeKubelet) GetPods() []*v1.Pod {
	return fk.podsFunc()
}

// 返回注入的 runningPodsFunc 结果。
func (fk *fakeKubelet) GetRunningPods(ctx context.Context) ([]*v1.Pod, error) {
	return fk.runningPodsFunc(ctx)
}

// 返回注入的 allocatedPodsFunc 结果；未注入时返回 nil。
func (fk *fakeKubelet) GetAllocatedPods() ([]*v1.Pod, error) {
	if fk.allocatedPodsFunc != nil {
		return fk.allocatedPodsFunc(), nil
	}
	return nil, nil
}

// 按 namespace/name 返回已分配 Pod；未注入时返回 nil。
func (fk *fakeKubelet) GetAllocatedPodByName(namespace, name string) (*v1.Pod, error) {
	if fk.allocatedPodByNameFunc != nil {
		return fk.allocatedPodByNameFunc(namespace, name), nil
	}
	return nil, nil
}

// 把 /logs 请求转交给注入的 logFunc。
func (fk *fakeKubelet) ServeLogs(w http.ResponseWriter, req *http.Request) {
	fk.logFunc(w, req)
}

// 把容器日志请求转交给注入的 containerLogsFunc。
func (fk *fakeKubelet) GetKubeletContainerLogs(ctx context.Context, podFullName, containerName string, logOptions *v1.PodLogOptions, stdout, stderr io.Writer) error {
	return fk.containerLogsFunc(ctx, podFullName, containerName, logOptions, stdout, stderr)
}

// 把 /run 请求转交给注入的 runFunc。
func (fk *fakeKubelet) RunInContainer(_ context.Context, podFullName string, uid types.UID, containerName string, cmd []string) ([]byte, error) {
	return fk.runFunc(podFullName, uid, containerName, cmd)
}

// 容器名为 checkpointingFailure 时返回错误，用于覆盖 checkpoint 失败路径。
func (fk *fakeKubelet) CheckpointContainer(_ context.Context, podUID types.UID, podFullName, containerName string, options *runtimeapi.CheckpointContainerRequest) error {
	if containerName == "checkpointingFailure" {
		return fmt.Errorf("Returning error for test")
	}
	return nil
}

// 测试中不返回任何指标描述。
func (fk *fakeKubelet) ListMetricDescriptors(ctx context.Context) ([]*runtimeapi.MetricDescriptor, error) {
	return nil, nil
}

// 测试中不返回任何 Pod sandbox 指标。
func (fk *fakeKubelet) ListPodSandboxMetrics(ctx context.Context) ([]*runtimeapi.PodSandboxMetrics, error) {
	return nil, nil
}

// 模拟 syncLoop 健康检查：以 resyncInterval 的两倍与 5 分钟取较大者为超时，
// 最近一次进入循环时间早于该阈值则判定不健康。
func (fk *fakeKubelet) SyncLoopHealthCheck(req *http.Request) error {
	duration := fk.resyncInterval * 2
	minDuration := time.Minute * 5
	if duration < minDuration {
		duration = minDuration
	}
	enterLoopTime := fk.LatestLoopEntryTime()
	if !enterLoopTime.IsZero() && time.Now().After(enterLoopTime.Add(duration)) {
		return fmt.Errorf("sync Loop took longer than expected")
	}
	return nil
}

// fakeRuntime 是流式服务端使用的运行时桩，把 exec/attach/portforward 转发给测试注入的函数。
type fakeRuntime struct {
	execFunc        func(string, []string, io.Reader, io.WriteCloser, io.WriteCloser, bool, <-chan remotecommandserver.TerminalSize) error
	attachFunc      func(string, io.Reader, io.WriteCloser, io.WriteCloser, bool, <-chan remotecommandserver.TerminalSize) error
	portForwardFunc func(string, int32, io.ReadWriteCloser) error
}

// 把 exec 请求转交给注入的 execFunc。
func (f *fakeRuntime) Exec(_ context.Context, containerID string, cmd []string, stdin io.Reader, stdout, stderr io.WriteCloser, tty bool, resize <-chan remotecommandserver.TerminalSize) error {
	return f.execFunc(containerID, cmd, stdin, stdout, stderr, tty, resize)
}

// 把 attach 请求转交给注入的 attachFunc。
func (f *fakeRuntime) Attach(_ context.Context, containerID string, stdin io.Reader, stdout, stderr io.WriteCloser, tty bool, resize <-chan remotecommandserver.TerminalSize) error {
	return f.attachFunc(containerID, stdin, stdout, stderr, tty, resize)
}

// 把 port forward 请求转交给注入的 portForwardFunc。
func (f *fakeRuntime) PortForward(_ context.Context, podSandboxID string, port int32, stream io.ReadWriteCloser) error {
	return f.portForwardFunc(podSandboxID, port, stream)
}

// testStreamingServer 包装一个真实的 streaming.Server，并通过 httptest.Server 暴露 HTTP 入口。
type testStreamingServer struct {
	streaming.Server
	fakeRuntime    *fakeRuntime
	testHTTPServer *httptest.Server
}

// 启动一个监听本地端口的流式服务端，并把请求转发给内部 streaming.Server，
// 用于在测试中直接拨号 exec/attach/portforward。
func newTestStreamingServer(streamIdleTimeout time.Duration) (s *testStreamingServer, err error) {
	s = &testStreamingServer{}
	s.testHTTPServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.ServeHTTP(w, r)
	}))
	defer func() {
		if err != nil {
			s.testHTTPServer.Close()
		}
	}()

	testURL, err := url.Parse(s.testHTTPServer.URL)
	if err != nil {
		return nil, err
	}

	s.fakeRuntime = &fakeRuntime{}
	config := streaming.DefaultConfig
	config.BaseURL = testURL
	if streamIdleTimeout != 0 {
		config.StreamIdleTimeout = streamIdleTimeout
	}
	s.Server, err = streaming.NewServer(config, s.fakeRuntime)
	if err != nil {
		return nil, err
	}
	return s, nil
}

// 先调用注入的 getExecCheck 做参数断言，再向流式服务端发起 exec，始终使用 testContainerID。
func (fk *fakeKubelet) GetExec(_ context.Context, podFullName string, podUID types.UID, containerName string, cmd []string, streamOpts remotecommandserver.Options) (*url.URL, error) {
	if fk.getExecCheck != nil {
		fk.getExecCheck(podFullName, podUID, containerName, cmd, streamOpts)
	}
	// Always use testContainerID
	resp, err := fk.streamingRuntime.GetExec(&runtimeapi.ExecRequest{
		ContainerId: testContainerID,
		Cmd:         cmd,
		Tty:         streamOpts.TTY,
		Stdin:       streamOpts.Stdin,
		Stdout:      streamOpts.Stdout,
		Stderr:      streamOpts.Stderr,
	})
	if err != nil {
		return nil, err
	}
	return url.Parse(resp.GetUrl())
}

// 先调用注入的 getAttachCheck 做参数断言，再向流式服务端发起 attach，始终使用 testContainerID。
func (fk *fakeKubelet) GetAttach(_ context.Context, podFullName string, podUID types.UID, containerName string, streamOpts remotecommandserver.Options) (*url.URL, error) {
	if fk.getAttachCheck != nil {
		fk.getAttachCheck(podFullName, podUID, containerName, streamOpts)
	}
	// Always use testContainerID
	resp, err := fk.streamingRuntime.GetAttach(&runtimeapi.AttachRequest{
		ContainerId: testContainerID,
		Tty:         streamOpts.TTY,
		Stdin:       streamOpts.Stdin,
		Stdout:      streamOpts.Stdout,
		Stderr:      streamOpts.Stderr,
	})
	if err != nil {
		return nil, err
	}
	return url.Parse(resp.GetUrl())
}

// 先调用注入的 getPortForwardCheck 做参数断言，再向流式服务端发起 port forward，始终使用 testPodSandboxID。
func (fk *fakeKubelet) GetPortForward(ctx context.Context, podName, podNamespace string, podUID types.UID, portForwardOpts portforward.V4Options) (*url.URL, error) {
	if fk.getPortForwardCheck != nil {
		fk.getPortForwardCheck(podName, podNamespace, podUID, portForwardOpts)
	}
	// Always use testPodSandboxID
	resp, err := fk.streamingRuntime.GetPortForward(&runtimeapi.PortForwardRequest{
		PodSandboxId: testPodSandboxID,
		Port:         portForwardOpts.Ports,
	})
	if err != nil {
		return nil, err
	}
	return url.Parse(resp.GetUrl())
}

// 以下方法仅为满足接口而存在，测试中不会被调用，因此统一返回零值。
// GetNode 返回空节点，测试不使用。
func (*fakeKubelet) GetNode(context.Context) (*v1.Node, error) { return nil, nil }

// GetNodeConfig 返回空的节点配置。
func (*fakeKubelet) GetNodeConfig() cm.NodeConfig { return cm.NodeConfig{} }

// GetPodCgroupRoot 返回空的 cgroup 根路径。
func (*fakeKubelet) GetPodCgroupRoot() string { return "" }

// GetPodByCgroupfs 始终返回未找到。
func (*fakeKubelet) GetPodByCgroupfs(cgroupfs string) (*v1.Pod, bool) { return nil, false }

// ListVolumesForPod 返回空卷列表但标记为已找到。
func (fk *fakeKubelet) ListVolumesForPod(podUID types.UID) (map[string]volume.Volume, bool) {
	return map[string]volume.Volume{}, true
}

// ListBlockVolumesForPod 返回空块卷列表但标记为已找到。
func (*fakeKubelet) ListBlockVolumesForPod(podUID types.UID) (map[string]volume.BlockVolume, bool) {
	return map[string]volume.BlockVolume{}, true
}

// RootFsStats 不提供根文件系统统计。
func (*fakeKubelet) RootFsStats() (*statsapi.FsStats, error) { return nil, nil }

// ListPodStats 不提供 Pod 统计。
func (*fakeKubelet) ListPodStats(context.Context) ([]statsapi.PodStats, error) { return nil, nil }

// ListPodStatsAndUpdateCPUNanoCoreUsage 不提供统计也不更新 CPU 用量。
func (*fakeKubelet) ListPodStatsAndUpdateCPUNanoCoreUsage(context.Context) ([]statsapi.PodStats, error) {
	return nil, nil
}

// ListPodCPUAndMemoryStats 不提供 Pod 的 CPU/内存统计。
func (*fakeKubelet) ListPodCPUAndMemoryStats(context.Context) ([]statsapi.PodStats, error) {
	return nil, nil
}

// ImageFsStats 不提供镜像文件系统统计。
func (*fakeKubelet) ImageFsStats(context.Context) (*statsapi.FsStats, *statsapi.FsStats, error) {
	return nil, nil, nil
}

// RlimitStats 不提供 rlimit 统计。
func (*fakeKubelet) RlimitStats() (*statsapi.RlimitStats, error) { return nil, nil }

// GetCgroupStats 不提供 cgroup 统计。
func (*fakeKubelet) GetCgroupStats(context.Context, string, bool) (*statsapi.ContainerStats, *statsapi.NetworkStats, error) {
	return nil, nil, nil
}

// GetCgroupCPUAndMemoryStats 不提供 cgroup 的 CPU/内存统计。
func (*fakeKubelet) GetCgroupCPUAndMemoryStats(cgroupName string, updateStats bool) (*statsapi.ContainerStats, error) {
	return nil, nil
}

// fakeAuth 同时实现认证、属性提取与授权接口，行为由测试注入的函数决定。
type fakeAuth struct {
	authenticateFunc       func(*http.Request) (*authenticator.Response, bool, error)
	attributesFunc         func(user.Info, *http.Request) []authorizer.Attributes
	authorizeFunc          func(authorizer.Attributes) (authorized authorizer.Decision, reason string, err error)
	currentCABundleContent func() []byte
	name                   string
	verifyOptions          func() (x509.VerifyOptions, bool)
}

// 委托给注入的 authenticateFunc，测试可据此控制认证结果。
func (f *fakeAuth) AuthenticateRequest(req *http.Request) (*authenticator.Response, bool, error) {
	return f.authenticateFunc(req)
}

// 委托给注入的 attributesFunc，测试可据此检查授权属性。
func (f *fakeAuth) GetRequestAttributes(ctx context.Context, u user.Info, req *http.Request) []authorizer.Attributes {
	return f.attributesFunc(u, req)
}

// 委托给注入的 authorizeFunc，测试可据此控制授权决策。
func (f *fakeAuth) Authorize(ctx context.Context, a authorizer.Attributes) (authorized authorizer.Decision, reason string, err error) {
	return f.authorizeFunc(a)
}

// fakeAuth 不维护监听器，空实现即可。
func (f *fakeAuth) AddListener(listener dynamiccertificates.Listener) {}

// 返回注入的 CA bundle 内容。
func (f *fakeAuth) CurrentCABundleContent() []byte {
	return f.currentCABundleContent()
}

// 返回注入的认证器名称。
func (f *fakeAuth) Name() string {
	return f.name
}

// 返回注入的 TLS 校验选项。
func (f *fakeAuth) VerifyOptions() (x509.VerifyOptions, bool) {
	return f.verifyOptions()
}

// serverTestFramework 汇总被测 Server、fake kubelet、fake 认证器与测试 HTTP 服务器。
type serverTestFramework struct {
	serverUnderTest *Server
	fakeKubelet     *fakeKubelet
	fakeAuth        *fakeAuth
	testHTTPServer  *httptest.Server
}

// 创建开启全部调试端点的测试服务框架，不注入流式服务端。
func newServerTest(ctx context.Context) *serverTestFramework {
	return newServerTestWithDebug(ctx, true, nil)
}

// 按开关构造调试相关的 kubelet 配置，再创建测试服务框架。
func newServerTestWithDebug(ctx context.Context, enableDebugging bool, streamingServer streaming.Server) *serverTestFramework {
	kubeCfg := &kubeletconfiginternal.KubeletConfiguration{
		EnableDebuggingHandlers: enableDebugging,
		EnableSystemLogHandler:  enableDebugging,
		EnableProfilingHandler:  enableDebugging,
		EnableDebugFlagsHandler: enableDebugging,
	}
	return newServerTestWithDebuggingHandlers(ctx, kubeCfg, streamingServer)
}

// 用给定 kubelet 配置装配 fake kubelet、fake 认证器与 HTTP 测试服务器，
// 默认认证通过、授权放行。
func newServerTestWithDebuggingHandlers(ctx context.Context, kubeCfg *kubeletconfiginternal.KubeletConfiguration, streamingServer streaming.Server) *serverTestFramework {

	fw := &serverTestFramework{}
	fw.fakeKubelet = &fakeKubelet{
		podByNameFunc: func(namespace, name string) (*v1.Pod, bool) {
			return &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: namespace,
					Name:      name,
					UID:       testUID,
				},
			}, true
		},
		plegHealth:       true,
		streamingRuntime: streamingServer,
	}
	fw.fakeAuth = &fakeAuth{
		authenticateFunc: func(req *http.Request) (*authenticator.Response, bool, error) {
			return &authenticator.Response{User: &user.DefaultInfo{Name: "test"}}, true, nil
		},
		attributesFunc: func(u user.Info, req *http.Request) []authorizer.Attributes {
			return []authorizer.Attributes{&authorizer.AttributesRecord{User: u}}
		},
		authorizeFunc: func(a authorizer.Attributes) (decision authorizer.Decision, reason string, err error) {
			return authorizer.DecisionAllow, "", nil
		},
	}
	server := NewServer(
		ctx,
		fw.fakeKubelet,
		stats.NewResourceAnalyzer(ctx, fw.fakeKubelet, time.Minute, &record.FakeRecorder{}),
		[]healthz.HealthChecker{},
		flagz.NamedFlagSetsReader{},
		fw.fakeAuth,
		kubeCfg,
	)
	fw.serverUnderTest = &server
	fw.testHTTPServer = httptest.NewServer(fw.serverUnderTest)
	return fw
}

// 按 kubelet 约定拼接 Pod 全名；namespace 为空时使用 default。
func getPodName(name, namespace string) string {
	if namespace == "" {
		namespace = metav1.NamespaceDefault
	}
	return name + "_" + namespace
}

// 验证 /logs/ 端点会把 kubelet 的日志目录列表原样透传。
func TestServeLogs(t *testing.T) {
	tCtx := ktesting.Init(t)
	fw := newServerTest(tCtx)
	defer fw.testHTTPServer.Close()

	content := string(`<pre><a href="kubelet.log">kubelet.log</a><a href="google.log">google.log</a></pre>`)
	fw.fakeKubelet.logFunc = func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Header().Add("Content-Type", "text/html")
		w.Write([]byte(content))
	}
	resp, err := http.Get(fw.testHTTPServer.URL + "/logs/")
	if err != nil {
		t.Fatalf("Got error GETing: %v", err)
	}
	defer resp.Body.Close()

	body, err := httputil.DumpResponse(resp, true)
	if err != nil {
		// copying the response body did not work
		t.Errorf("Cannot copy resp: %#v", err)
	}
	result := string(body)
	if !strings.Contains(result, "kubelet.log") || !strings.Contains(result, "google.log") {
		t.Errorf("Received wrong data: %s", result)
	}

}

// 验证仅支持 GET 的端点收到 POST 时返回 405，并带有 Allow: GET 头。
func TestGETOnlyEndpointsRejectPostWithAllowHeader(t *testing.T) {
	tCtx := ktesting.Init(t)
	fw := newServerTest(tCtx)
	defer fw.testHTTPServer.Close()

	tests := []struct {
		name string
		path string
	}{
		{name: "pods", path: "/pods/"},
		{name: "containerLogs", path: "/containerLogs/default/mypod/mycontainer"},
		{name: "runningpods", path: "/runningpods/"},
		{name: "logs", path: "/logs/"},
		{name: "pprof", path: "/debug/pprof/profile?seconds=1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodPost, fw.testHTTPServer.URL+tt.path, nil)
			require.NoError(t, err)

			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close() //nolint:errcheck

			assert.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)
			assert.Equal(t, http.MethodGet, resp.Header.Get("Allow"))
		})
	}
}

// 验证 /run 端点正确解析 Pod、容器与命令，并原样返回执行输出。
func TestServeRunInContainer(t *testing.T) {
	tCtx := ktesting.Init(t)
	fw := newServerTest(tCtx)
	defer fw.testHTTPServer.Close()
	output := "foo bar"
	podNamespace := "other"
	podName := "foo"
	expectedPodName := getPodName(podName, podNamespace)
	expectedContainerName := "baz"
	expectedCommand := "ls -a"
	fw.fakeKubelet.runFunc = func(podFullName string, uid types.UID, containerName string, cmd []string) ([]byte, error) {
		if podFullName != expectedPodName {
			t.Errorf("expected %s, got %s", expectedPodName, podFullName)
		}
		if containerName != expectedContainerName {
			t.Errorf("expected %s, got %s", expectedContainerName, containerName)
		}
		if strings.Join(cmd, " ") != expectedCommand {
			t.Errorf("expected: %s, got %v", expectedCommand, cmd)
		}

		return []byte(output), nil
	}

	resp, err := http.Post(fw.testHTTPServer.URL+"/run/"+podNamespace+"/"+podName+"/"+expectedContainerName+"?cmd=ls%20-a", "", nil)

	if err != nil {
		t.Fatalf("Got error POSTing: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		// copying the response body did not work
		t.Errorf("Cannot copy resp: %#v", err)
	}
	result := string(body)
	if result != output {
		t.Errorf("expected %s, got %s", output, result)
	}
}

// 在 /run 路径中带上 Pod UID，验证 UID 被正确传给 kubelet。
func TestServeRunInContainerWithUID(t *testing.T) {
	tCtx := ktesting.Init(t)
	fw := newServerTest(tCtx)
	defer fw.testHTTPServer.Close()
	output := "foo bar"
	podNamespace := "other"
	podName := "foo"
	expectedPodName := getPodName(podName, podNamespace)
	expectedContainerName := "baz"
	expectedCommand := "ls -a"
	fw.fakeKubelet.runFunc = func(podFullName string, uid types.UID, containerName string, cmd []string) ([]byte, error) {
		if podFullName != expectedPodName {
			t.Errorf("expected %s, got %s", expectedPodName, podFullName)
		}
		if string(uid) != testUID {
			t.Errorf("expected %s, got %s", testUID, uid)
		}
		if containerName != expectedContainerName {
			t.Errorf("expected %s, got %s", expectedContainerName, containerName)
		}
		if strings.Join(cmd, " ") != expectedCommand {
			t.Errorf("expected: %s, got %v", expectedCommand, cmd)
		}

		return []byte(output), nil
	}

	resp, err := http.Post(fw.testHTTPServer.URL+"/run/"+podNamespace+"/"+podName+"/"+testUID+"/"+expectedContainerName+"?cmd=ls%20-a", "", nil)
	if err != nil {
		t.Fatalf("Got error POSTing: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		// copying the response body did not work
		t.Errorf("Cannot copy resp: %#v", err)
	}
	result := string(body)
	if result != output {
		t.Errorf("expected %s, got %s", output, result)
	}
}

// 验证 /healthz 在正常状态下返回 200 且响应体包含 ok。
func TestHealthCheck(t *testing.T) {
	tCtx := ktesting.Init(t)
	fw := newServerTest(tCtx)
	defer fw.testHTTPServer.Close()

	assertHealthIsOk(t, fw.testHTTPServer.URL+"/healthz")
}

// 请求给定 URL，断言返回期望的错误状态码。
func assertHealthFails(t *testing.T, httpURL string, expectedErrorCode int) {
	resp, err := http.Get(httpURL)
	if err != nil {
		t.Fatalf("Got error GETing: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != expectedErrorCode {
		t.Errorf("expected status code %d, got %d", expectedErrorCode, resp.StatusCode)
	}
}

// 在 ComponentStatusz 特性门控开启时验证 /statusz 列出全部已注册路径。
func TestStatusz(t *testing.T) {
	tCtx := ktesting.Init(t)
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, zpagesfeatures.ComponentStatusz, true)

	fw := newServerTest(tCtx)
	defer fw.testHTTPServer.Close()

	req, err := http.NewRequest(http.MethodGet, fw.testHTTPServer.URL+"/statusz", nil)
	if err != nil {
		t.Fatalf("Got error creating request: %v", err)
	}
	req.Header.Set("Accept", "text/plain")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Got error GETing: %v", err)
	}
	defer func(resp *http.Response) {
		closeErr := resp.Body.Close()
		if closeErr != nil {
			t.Errorf("Got error closing response body: %v", err)
		}
	}(resp)
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status code %d, got %d", http.StatusOK, resp.StatusCode)
	}
	resBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Got error reading response body: %v", err)
	}

	reg := regexp.MustCompile(`Paths([:=\s]+)/configz /debug /flagz /healthz /metrics\n$`)
	if reg.FindStringSubmatch(string(resBody)) == nil {
		t.Errorf("statusz paths missing: %s\n\nExpected: %q", string(resBody), "Paths<delimter> /configz /debug /flagz /healthz /metrics")
	}
}

// 验证 /configz 返回注册的 kubeletconfig，且能反序列化为公开配置类型。
func TestConfigz(t *testing.T) {
	tCtx := ktesting.Init(t)

	configz.Delete("kubeletconfig")
	cz, err := configz.New("kubeletconfig")
	if err != nil {
		t.Fatalf("unable to register configz: %v", err)
	}
	defer configz.Delete("kubeletconfig")
	kc := &v1beta1.KubeletConfiguration{}
	kc.TypeMeta.APIVersion = "kubelet.config.k8s.io/v1beta1"
	kc.TypeMeta.Kind = "KubeletConfiguration"
	if err := cz.Set(kc); err != nil {
		t.Fatalf("unable to set configz: %v", err)
	}

	fw := newServerTest(tCtx)
	defer fw.testHTTPServer.Close()

	req, err := http.NewRequest(http.MethodGet, fw.testHTTPServer.URL+"/configz", nil)
	if err != nil {
		t.Fatalf("Got error creating request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Got error GETing: %v", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status code %d, got %d", http.StatusOK, resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Got error reading response body: %v", err)
	}

	var configz map[string]unstructured.Unstructured
	if err := json.Unmarshal(body, &configz); err != nil {
		t.Fatalf("failed to unmarshal configz: %v", err)
	}
	cfg, ok := configz["kubeletconfig"]
	if !ok {
		t.Fatalf("configz missing 'kubeletconfig' key")
	}
	if cfg.GetAPIVersion() != "kubelet.config.k8s.io/v1beta1" {
		t.Errorf("unexpected APIVersion: %s", cfg.GetAPIVersion())
	}
	if cfg.GetKind() != "KubeletConfiguration" {
		t.Errorf("unexpected Kind: %s", cfg.GetKind())
	}

	// confirm that they expose public config type
	var kubeletConfig v1beta1.KubeletConfiguration
	err = json.Unmarshal(body, &struct {
		ComponentConfig *v1beta1.KubeletConfiguration `json:"kubeletconfig"`
	}{ComponentConfig: &kubeletConfig})
	if err != nil {
		t.Errorf("failed to deserialize into public config type: %v", err)
	}
}

// 确保所有注册的 handler 与 web service 路由都有对应的授权测试用例，
// 避免新增端点时遗漏权限测试。
func TestAuthzCoverage(t *testing.T) {
	tCtx := ktesting.Init(t)
	fw := newServerTest(tCtx)
	defer fw.testHTTPServer.Close()

	for _, fineGrained := range []bool{false, true} {
		if !fineGrained {
			featuregatetesting.SetFeatureGateEmulationVersionDuringTest(t, utilfeature.DefaultFeatureGate, version.MustParse("1.35"))
		}
		t.Run(fmt.Sprintf("fineGrained=%v", fineGrained), func(t *testing.T) {
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.KubeletFineGrainedAuthz, fineGrained)
			// method:path -> 是否已有对应用例；先全部置 false，再由 AuthzTestCases 置 true。
			// method:path -> has coverage
			expectedCases := map[string]bool{}

			// Test all the non-web-service handlers
			for _, path := range fw.serverUnderTest.restfulCont.RegisteredHandlePaths() {
				expectedCases["GET:"+path] = false
				expectedCases["POST:"+path] = false
			}

			// Test all the generated web-service paths
			for _, ws := range fw.serverUnderTest.restfulCont.RegisteredWebServices() {
				for _, r := range ws.Routes() {
					expectedCases[r.Method+":"+r.Path] = false
				}
			}

			// 兜底检查 Handle->HandleWithFilter() 的委托仍然生效；
			// 理想情况下这些端点会迁到注册的 web service，届时该列表可以缩短。
			// This is a sanity check that the Handle->HandleWithFilter() delegation is working
			// Ideally, these would move to registered web services and this list would get shorter
			expectedPaths := []string{"/healthz", "/metrics", "/metrics/cadvisor"}
			for _, expectedPath := range expectedPaths {
				if _, expected := expectedCases["GET:"+expectedPath]; !expected {
					t.Errorf("Expected registered handle path %s was missing", expectedPath)
				}
			}

			for _, tc := range AuthzTestCases(fineGrained) {
				expectedCases[tc.Method+":"+tc.Path] = true
			}

			for tc, found := range expectedCases {
				if !found {
					t.Errorf("Missing authz test case for %s", tc)
				}
			}
		})
	}
}

// 验证免认证 handler 白名单未被扩大：任何新端点都必须要求认证，
// 并确认调试端点被显式禁用后返回 405。
func TestInstallAuthNotRequiredHandlers(t *testing.T) {
	tCtx := ktesting.Init(t)
	fw := newServerTestWithDebug(tCtx, false, nil)
	defer fw.testHTTPServer.Close()

	// 该白名单不应再扩大：新增端点必须要求认证。
	// No new handlers should be added to this list.
	allowedAuthNotRequiredHandlers := sets.NewString(
		"/healthz",
		"/healthz/log",
		"/healthz/ping",
		"/healthz/syncloop",
		"/metrics",
		"/metrics/slis",
		"/metrics/cadvisor",
		"/metrics/probes",
		"/metrics/resource",
		"/pods/",
		"/stats/",
		"/stats/summary",
	)

	// 这些调试端点在本测试配置中被显式关闭。
	// These handlers are explicitly disabled.
	debuggingDisabledHandlers := sets.NewString(
		"/run/",
		"/exec/",
		"/attach/",
		"/portForward/",
		"/containerLogs/",
		"/runningpods/",
		"/debug/pprof/",
		"/logs/",
	)
	allowedAuthNotRequiredHandlers.Insert(debuggingDisabledHandlers.UnsortedList()...)

	// Test all the non-web-service handlers
	for _, path := range fw.serverUnderTest.restfulCont.RegisteredHandlePaths() {
		if !allowedAuthNotRequiredHandlers.Has(path) {
			t.Errorf("New handler %q must require auth", path)
		}
	}

	// Test all the generated web-service paths
	for _, ws := range fw.serverUnderTest.restfulCont.RegisteredWebServices() {
		for _, r := range ws.Routes() {
			if !allowedAuthNotRequiredHandlers.Has(r.Path) {
				t.Errorf("New handler %q must require auth", r.Path)
			}
		}
	}

	// 确认被关闭的端点对所有 HTTP 方法都返回 405。
	// Ensure the disabled handlers are in fact disabled.
	for path := range debuggingDisabledHandlers {
		for _, method := range []string{"GET", "POST", "PUT", "PATCH", "DELETE"} {
			t.Run(method+":"+path, func(t *testing.T) {
				req, err := http.NewRequest(method, fw.testHTTPServer.URL+path, nil)
				require.NoError(t, err)

				resp, err := http.DefaultClient.Do(req)
				require.NoError(t, err)
				defer resp.Body.Close() //nolint:errcheck

				assert.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)
			})
		}
	}
}

// 对每个授权测试用例验证认证、属性提取、授权三步都会被调用，
// 且授权拒绝时返回 403。
func TestAuthFilters(t *testing.T) {
	tCtx := ktesting.Init(t)
	// 打开 checkpoint 与 zpages 相关特性，确保被测端点全部注册。
	featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
		features.ContainerCheckpoint:    true,
		zpagesfeatures.ComponentStatusz: true,
		zpagesfeatures.ComponentFlagz:   true,
	})

	fw := newServerTest(tCtx)
	defer fw.testHTTPServer.Close()

	attributesGetter := NewNodeAuthorizerAttributesGetter(authzTestNodeName)

	for _, fineGraned := range []bool{false, true} {
		if !fineGraned {
			featuregatetesting.SetFeatureGateEmulationVersionDuringTest(t, utilfeature.DefaultFeatureGate, version.MustParse("1.35"))
		}
		featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.KubeletFineGrainedAuthz, fineGraned)
		for _, tc := range AuthzTestCases(fineGraned) {
			t.Run(fmt.Sprintf("method=%v:path=%v:fineGrained=%v", tc.Method, tc.Path, fineGraned), func(t *testing.T) {
				var (
					expectedUser = AuthzTestUser()

					calledAuthenticate = false
					calledAuthorize    = false
					calledAttributes   = false
				)

				fw.fakeAuth.authenticateFunc = func(req *http.Request) (*authenticator.Response, bool, error) {
					calledAuthenticate = true
					return &authenticator.Response{User: expectedUser}, true, nil
				}
				fw.fakeAuth.attributesFunc = func(u user.Info, req *http.Request) []authorizer.Attributes {
					calledAttributes = true
					require.Equal(t, expectedUser, u)
					attrs := attributesGetter.GetRequestAttributes(tCtx, u, req)
					tc.AssertAttributes(t, attrs)
					return attrs
				}
				fw.fakeAuth.authorizeFunc = func(a authorizer.Attributes) (decision authorizer.Decision, reason string, err error) {
					calledAuthorize = true
					return authorizer.DecisionNoOpinion, "", nil
				}

				req, err := http.NewRequest(tc.Method, fw.testHTTPServer.URL+tc.Path, nil)
				require.NoError(t, err)

				resp, err := http.DefaultClient.Do(req)
				require.NoError(t, err)
				defer resp.Body.Close() //nolint:errcheck

				assert.Equal(t, http.StatusForbidden, resp.StatusCode)
				assert.True(t, calledAuthenticate, "Authenticate was not called")
				assert.True(t, calledAttributes, "Attributes were not called")
				assert.True(t, calledAuthorize, "Authorize was not called")
			})
		}
	}
}

// 验证认证后授权函数返回错误时请求失败，且三步鉴权流程都执行了。
func TestAuthenticationError(t *testing.T) {
	tCtx := ktesting.Init(t)
	var (
		expectedUser       = &user.DefaultInfo{Name: "test"}
		expectedAttributes = []authorizer.Attributes{&authorizer.AttributesRecord{User: expectedUser}}

		calledAuthenticate = false
		calledAuthorize    = false
		calledAttributes   = false
	)

	fw := newServerTest(tCtx)
	defer fw.testHTTPServer.Close()
	fw.fakeAuth.authenticateFunc = func(req *http.Request) (*authenticator.Response, bool, error) {
		calledAuthenticate = true
		return &authenticator.Response{User: expectedUser}, true, nil
	}
	fw.fakeAuth.attributesFunc = func(u user.Info, req *http.Request) []authorizer.Attributes {
		calledAttributes = true
		return expectedAttributes
	}
	fw.fakeAuth.authorizeFunc = func(a authorizer.Attributes) (decision authorizer.Decision, reason string, err error) {
		calledAuthorize = true
		return authorizer.DecisionNoOpinion, "", errors.New("Failed")
	}

	assertHealthFails(t, fw.testHTTPServer.URL+"/healthz", http.StatusInternalServerError)

	if !calledAuthenticate {
		t.Fatalf("Authenticate was not called")
	}
	if !calledAttributes {
		t.Fatalf("Attributes was not called")
	}
	if !calledAuthorize {
		t.Fatalf("Authorize was not called")
	}
}

// 验证认证失败（未通过）时直接返回 401，不再继续提取属性或授权。
func TestAuthenticationFailure(t *testing.T) {
	tCtx := ktesting.Init(t)
	var (
		expectedUser       = &user.DefaultInfo{Name: "test"}
		expectedAttributes = []authorizer.Attributes{&authorizer.AttributesRecord{User: expectedUser}}

		calledAuthenticate = false
		calledAuthorize    = false
		calledAttributes   = false
	)

	fw := newServerTest(tCtx)
	defer fw.testHTTPServer.Close()
	fw.fakeAuth.authenticateFunc = func(req *http.Request) (*authenticator.Response, bool, error) {
		calledAuthenticate = true
		return nil, false, nil
	}
	fw.fakeAuth.attributesFunc = func(u user.Info, req *http.Request) []authorizer.Attributes {
		calledAttributes = true
		return expectedAttributes
	}
	fw.fakeAuth.authorizeFunc = func(a authorizer.Attributes) (decision authorizer.Decision, reason string, err error) {
		calledAuthorize = true
		return authorizer.DecisionNoOpinion, "", nil
	}

	assertHealthFails(t, fw.testHTTPServer.URL+"/healthz", http.StatusUnauthorized)

	if !calledAuthenticate {
		t.Fatalf("Authenticate was not called")
	}
	if calledAttributes {
		t.Fatalf("Attributes was called unexpectedly")
	}
	if calledAuthorize {
		t.Fatalf("Authorize was called unexpectedly")
	}
}

// 验证认证、属性提取、授权全部成功时 /healthz 返回 200。
func TestAuthorizationSuccess(t *testing.T) {
	tCtx := ktesting.Init(t)
	var (
		expectedUser       = &user.DefaultInfo{Name: "test"}
		expectedAttributes = []authorizer.Attributes{&authorizer.AttributesRecord{User: expectedUser}}

		calledAuthenticate = false
		calledAuthorize    = false
		calledAttributes   = false
	)

	fw := newServerTest(tCtx)
	defer fw.testHTTPServer.Close()
	fw.fakeAuth.authenticateFunc = func(req *http.Request) (*authenticator.Response, bool, error) {
		calledAuthenticate = true
		return &authenticator.Response{User: expectedUser}, true, nil
	}
	fw.fakeAuth.attributesFunc = func(u user.Info, req *http.Request) []authorizer.Attributes {
		calledAttributes = true
		return expectedAttributes
	}
	fw.fakeAuth.authorizeFunc = func(a authorizer.Attributes) (decision authorizer.Decision, reason string, err error) {
		calledAuthorize = true
		return authorizer.DecisionAllow, "", nil
	}

	assertHealthIsOk(t, fw.testHTTPServer.URL+"/healthz")

	if !calledAuthenticate {
		t.Fatalf("Authenticate was not called")
	}
	if !calledAttributes {
		t.Fatalf("Attributes were not called")
	}
	if !calledAuthorize {
		t.Fatalf("Authorize was not called")
	}
}

// 验证 syncLoop 长时间未进入时 /healthz 返回 500。
func TestSyncLoopCheck(t *testing.T) {
	tCtx := ktesting.Init(t)
	fw := newServerTest(tCtx)
	defer fw.testHTTPServer.Close()

	fw.fakeKubelet.resyncInterval = time.Minute
	fw.fakeKubelet.loopEntryTime = time.Now()

	// Test with correct hostname, Docker version
	assertHealthIsOk(t, fw.testHTTPServer.URL+"/healthz")

	fw.fakeKubelet.loopEntryTime = time.Now().Add(time.Minute * -10)
	assertHealthFails(t, fw.testHTTPServer.URL+"/healthz", http.StatusInternalServerError)
}

// 请求给定 URL，断言返回 200 且响应体包含 ok。
func assertHealthIsOk(t *testing.T, httpURL string) {
	resp, err := http.Get(httpURL)
	if err != nil {
		t.Fatalf("Got error GETing: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status code %d, got %d", http.StatusOK, resp.StatusCode)
	}
	body, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		// copying the response body did not work
		t.Fatalf("Cannot copy resp: %#v", readErr)
	}
	result := string(body)
	if !strings.Contains(result, "ok") {
		t.Errorf("expected body contains ok, got %s", result)
	}
}

// 让 fake kubelet 对任意查询都返回包含指定容器的 Pod。
func setPodByNameFunc(fw *serverTestFramework, namespace, pod, container string) {
	fw.fakeKubelet.podByNameFunc = func(namespace, name string) (*v1.Pod, bool) {
		return &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace,
				Name:      pod,
			},
			Spec: v1.PodSpec{
				Containers: []v1.Container{
					{
						Name: container,
					},
				},
			},
		}, true
	}
}

// 替换容器日志函数，校验传入参数并写入固定输出。
func setGetContainerLogsFunc(fw *serverTestFramework, t *testing.T, expectedPodName, expectedContainerName string, expectedLogOptions *v1.PodLogOptions, output string) {
	fw.fakeKubelet.containerLogsFunc = func(_ context.Context, podFullName, containerName string, logOptions *v1.PodLogOptions, stdout, stderr io.Writer) error {
		if podFullName != expectedPodName {
			t.Errorf("expected %s, got %s", expectedPodName, podFullName)
		}
		if containerName != expectedContainerName {
			t.Errorf("expected %s, got %s", expectedContainerName, containerName)
		}
		if !reflect.DeepEqual(expectedLogOptions, logOptions) {
			t.Errorf("expected %#v, got %#v", expectedLogOptions, logOptions)
		}

		io.WriteString(stdout, output)
		return nil
	}
}

// 验证 /containerLogs 对 tail、follow 等查询参数的处理，
// 并在 PodLogsQuerySplitStreams 开关两种取值下保持原有行为。
func TestContainerLogs(t *testing.T) {
	tCtx := ktesting.Init(t)
	fw := newServerTest(tCtx)
	defer fw.testHTTPServer.Close()

	tests := map[string]struct {
		query        string
		podLogOption *v1.PodLogOptions
	}{
		"without tail":     {"", &v1.PodLogOptions{}},
		"with tail":        {"?tailLines=5", &v1.PodLogOptions{TailLines: ptr.To[int64](5)}},
		"with legacy tail": {"?tail=5", &v1.PodLogOptions{TailLines: ptr.To[int64](5)}},
		"with tail all":    {"?tail=all", &v1.PodLogOptions{}},
		"with follow":      {"?follow=1", &v1.PodLogOptions{Follow: true}},
	}

	for desc, test := range tests {
		// To make sure the original behavior doesn't change no matter the feature PodLogsQuerySplitStreams is enabled or not.
		for _, enablePodLogsQuerySplitStreams := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s (enablePodLogsQuerySplitStreams=%v)", desc, enablePodLogsQuerySplitStreams), func(t *testing.T) {
				featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.PodLogsQuerySplitStreams, enablePodLogsQuerySplitStreams)
				expectedLogOptions := test.podLogOption.DeepCopy()
				if enablePodLogsQuerySplitStreams && expectedLogOptions.Stream == nil {
					// 开启分流后，HTTP handler 会在内部把 stream 默认为 All。
					// The HTTP handler will internally set the default stream value.
					expectedLogOptions.Stream = ptr.To(v1.LogStreamAll)
				}

				output := "foo bar"
				podNamespace := "other"
				podName := "foo"
				expectedPodName := getPodName(podName, podNamespace)
				expectedContainerName := "baz"
				setPodByNameFunc(fw, podNamespace, podName, expectedContainerName)
				setGetContainerLogsFunc(fw, t, expectedPodName, expectedContainerName, expectedLogOptions, output)
				resp, err := http.Get(fw.testHTTPServer.URL + "/containerLogs/" + podNamespace + "/" + podName + "/" + expectedContainerName + test.query)
				if err != nil {
					t.Errorf("Got error GETing: %v", err)
				}
				defer func() {
					_ = resp.Body.Close()
				}()

				body, err := io.ReadAll(resp.Body)
				if err != nil {
					t.Errorf("Error reading container logs: %v", err)
				}
				result := string(body)
				if result != output {
					t.Errorf("Expected: '%v', got: '%v'", output, result)
				}
			})
		}
	}
}

// 验证非法 tail 参数（-1）返回 422。
func TestContainerLogsWithInvalidTail(t *testing.T) {
	tCtx := ktesting.Init(t)
	fw := newServerTest(tCtx)
	defer fw.testHTTPServer.Close()
	output := "foo bar"
	podNamespace := "other"
	podName := "foo"
	expectedPodName := getPodName(podName, podNamespace)
	expectedContainerName := "baz"
	setPodByNameFunc(fw, podNamespace, podName, expectedContainerName)
	setGetContainerLogsFunc(fw, t, expectedPodName, expectedContainerName, &v1.PodLogOptions{}, output)
	resp, err := http.Get(fw.testHTTPServer.URL + "/containerLogs/" + podNamespace + "/" + podName + "/" + expectedContainerName + "?tail=-1")
	if err != nil {
		t.Errorf("Got error GETing: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Errorf("Unexpected non-error reading container logs: %#v", resp)
	}
}

// 在 PodLogsQuerySplitStreams 开启时验证 stream 参数能正确筛选 stdout/stderr，
// 并覆盖空查询、limitBytes、tailLines 及非法 stream 等情形。
func TestContainerLogsWithSeparateStream(t *testing.T) {
	tCtx := ktesting.Init(t)
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.PodLogsQuerySplitStreams, true)

	type logEntry struct {
		stream string
		msg    string
	}

	fw := newServerTest(tCtx)
	defer fw.testHTTPServer.Close()

	var (
		streamStdout = v1.LogStreamStdout
		streamStderr = v1.LogStreamStderr
		streamAll    = v1.LogStreamAll
	)

	testCases := []struct {
		name               string
		query              string
		logs               []logEntry
		expectedOutput     string
		expectedLogOptions *v1.PodLogOptions
	}{
		{
			// 查询串为空时默认值填充不会生效，因此这里显式期望 stream=All。
			// See also https://github.com/kubernetes/kubernetes/issues/128589
			// Defaulters don't work if the query is empty.
			name: "empty query should return all logs",
			logs: []logEntry{
				{stream: v1.LogStreamStdout, msg: "foo\n"},
				{stream: v1.LogStreamStderr, msg: "bar\n"},
			},
			query: "",
			expectedLogOptions: &v1.PodLogOptions{
				Stream: &streamAll,
			},
			expectedOutput: "foo\nbar\n",
		},
		{
			name: "missing stream param should return all logs",
			logs: []logEntry{
				{stream: v1.LogStreamStdout, msg: "foo\n"},
				{stream: v1.LogStreamStderr, msg: "bar\n"},
			},
			query: "?limitBytes=100",
			expectedLogOptions: &v1.PodLogOptions{
				Stream:     &streamAll,
				LimitBytes: ptr.To[int64](100),
			},
			expectedOutput: "foo\nbar\n",
		},
		{
			name: "only stdout logs",
			logs: []logEntry{
				{stream: v1.LogStreamStdout, msg: "out1\n"},
				{stream: v1.LogStreamStderr, msg: "err1\n"},
				{stream: v1.LogStreamStdout, msg: "out2\n"},
			},
			query: "?stream=Stdout",
			expectedLogOptions: &v1.PodLogOptions{
				Stream: &streamStdout,
			},
			expectedOutput: "out1\nout2\n",
		},
		{
			name: "only stderr logs",
			logs: []logEntry{
				{stream: v1.LogStreamStderr, msg: "err1\n"},
				{stream: v1.LogStreamStderr, msg: "err2\n"},
				{stream: v1.LogStreamStdout, msg: "out1\n"},
			},
			query: "?stream=Stderr",
			expectedLogOptions: &v1.PodLogOptions{
				Stream: &streamStderr,
			},
			expectedOutput: "err1\nerr2\n",
		},
		{
			name: "return all logs",
			logs: []logEntry{
				{stream: v1.LogStreamStdout, msg: "out1\n"},
				{stream: v1.LogStreamStderr, msg: "err1\n"},
				{stream: v1.LogStreamStdout, msg: "out2\n"},
			},
			query: "?stream=All",
			expectedLogOptions: &v1.PodLogOptions{
				Stream: &streamAll,
			},
			expectedOutput: "out1\nerr1\nout2\n",
		},
		{
			name: "stdout logs with legacy tail",
			logs: []logEntry{
				{stream: v1.LogStreamStdout, msg: "out1\n"},
				{stream: v1.LogStreamStderr, msg: "err1\n"},
				{stream: v1.LogStreamStdout, msg: "out2\n"},
			},
			query: "?stream=All&tail=1",
			expectedLogOptions: &v1.PodLogOptions{
				Stream:    &streamAll,
				TailLines: ptr.To[int64](1),
			},
			expectedOutput: "out2\n",
		},
		{
			name: "return the last 2 lines of logs",
			logs: []logEntry{
				{stream: v1.LogStreamStdout, msg: "out1\n"},
				{stream: v1.LogStreamStderr, msg: "err1\n"},
				{stream: v1.LogStreamStdout, msg: "out2\n"},
			},
			query: "?stream=All&tailLines=2",
			expectedLogOptions: &v1.PodLogOptions{
				Stream:    &streamAll,
				TailLines: ptr.To[int64](2),
			},
			expectedOutput: "err1\nout2\n",
		},
		{
			name: "return the first 6 bytes of the stdout log stream",
			logs: []logEntry{
				{stream: v1.LogStreamStderr, msg: "err1\n"},
				{stream: v1.LogStreamStdout, msg: "out1\n"},
				{stream: v1.LogStreamStderr, msg: "err2\n"},
				{stream: v1.LogStreamStdout, msg: "out2\n"},
			},
			query: "?stream=Stdout&limitBytes=6",
			expectedLogOptions: &v1.PodLogOptions{
				Stream:     &streamStdout,
				LimitBytes: ptr.To[int64](6),
			},
			expectedOutput: "out1\no",
		},
		{
			name: "invalid stream",
			logs: []logEntry{
				{stream: v1.LogStreamStderr, msg: "err1\n"},
				{stream: v1.LogStreamStdout, msg: "out1\n"},
			},
			query:              "?stream=invalid",
			expectedLogOptions: nil,
			expectedOutput:     `{"message": "Invalid request."}`,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			podNamespace := "other"
			podName := "foo"
			expectedContainerName := "baz"
			setPodByNameFunc(fw, podNamespace, podName, expectedContainerName)
			fw.fakeKubelet.containerLogsFunc = func(_ context.Context, podFullName, containerName string, logOptions *v1.PodLogOptions, stdout, stderr io.Writer) error {
				if !reflect.DeepEqual(tc.expectedLogOptions, logOptions) {
					t.Errorf("expected %#v, got %#v", tc.expectedLogOptions, logOptions)
				}

				var dst io.Writer
				tailLines := len(tc.logs)
				if logOptions.TailLines != nil {
					tailLines = int(*logOptions.TailLines)
				}

				remain := 0
				if logOptions.LimitBytes != nil {
					remain = int(*logOptions.LimitBytes)
				} else {
					for _, log := range tc.logs {
						remain += len(log.msg)
					}
				}

				logs := tc.logs[len(tc.logs)-tailLines:]
				for _, log := range logs {
					switch log.stream {
					case v1.LogStreamStdout:
						dst = stdout
					case v1.LogStreamStderr:
						dst = stderr
					}
					// Skip if the stream is not requested
					if dst == nil {
						continue
					}
					line := log.msg
					if len(line) > remain {
						line = line[:remain]
					}
					_, _ = io.WriteString(dst, line)
					remain -= len(line)
					if remain <= 0 {
						return nil
					}
				}
				return nil
			}
			resp, err := http.Get(fw.testHTTPServer.URL + "/containerLogs/" + podNamespace + "/" + podName + "/" + expectedContainerName + tc.query)
			if err != nil {
				t.Errorf("Got error GETing: %v", err)
			}
			defer func() {
				_ = resp.Body.Close()
			}()

			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Errorf("Error reading container logs: %v", err)
			}
			result := string(body)
			if result != tc.expectedOutput {
				t.Errorf("Expected: %q, got: %q", tc.expectedOutput, result)
			}
		})
	}
}

// 验证 /checkpoint 在 Pod 不存在、容器名错误、执行失败、成功以及特性关闭时
// 分别返回预期的状态码与错误信息。
func TestCheckpointContainer(t *testing.T) {
	tCtx := ktesting.Init(t)
	podNamespace := "other"
	podName := "foo"
	expectedContainerName := "baz"

	setupTest := func(featureGate bool) *serverTestFramework {
		// Enable features.ContainerCheckpoint during test
		featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.ContainerCheckpoint, featureGate)

		fw := newServerTest(tCtx)
		// 初始状态下 Pod 查询一律失败，用于覆盖 404 分支。
		// GetPodByName() should always fail
		fw.fakeKubelet.podByNameFunc = func(namespace, name string) (*v1.Pod, bool) {
			return nil, false
		}
		return fw
	}
	fw := setupTest(true)
	defer fw.testHTTPServer.Close()

	t.Run("wrong pod namespace", func(t *testing.T) {
		resp, err := http.Post(fw.testHTTPServer.URL+"/checkpoint/"+podNamespace+"/"+podName+"/"+expectedContainerName, "", nil)
		if err != nil {
			t.Errorf("Got error POSTing: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("Unexpected non-error checkpointing container: %#v", resp)
		}
	})
	// 让 Pod 查询成功，但请求的容器名不在该 Pod 中。
	// let GetPodByName() return a result, but our container "wrongContainerName" is not part of the Pod
	setPodByNameFunc(fw, podNamespace, podName, expectedContainerName)
	t.Run("wrong container name", func(t *testing.T) {
		resp, err := http.Post(fw.testHTTPServer.URL+"/checkpoint/"+podNamespace+"/"+podName+"/wrongContainerName", "", nil)
		if err != nil {
			t.Errorf("Got error POSTing: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("Unexpected non-error checkpointing container: %#v", resp)
		}
	})
	// 让容器名触发 fake kubelet 的 checkpoint 失败分支。
	// Now the checkpointing of the container fails
	fw.fakeKubelet.podByNameFunc = func(namespace, name string) (*v1.Pod, bool) {
		return &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: podNamespace,
				Name:      podName,
			},
			Spec: v1.PodSpec{
				Containers: []v1.Container{
					{
						Name: "checkpointingFailure",
					},
				},
			},
		}, true
	}
	t.Run("checkpointing fails", func(t *testing.T) {
		resp, err := http.Post(fw.testHTTPServer.URL+"/checkpoint/"+podNamespace+"/"+podName+"/checkpointingFailure", "", nil)
		if err != nil {
			t.Errorf("Got error POSTing: %v", err)
		}
		defer resp.Body.Close()
		assert.Equal(t, 500, resp.StatusCode)
		body, _ := io.ReadAll(resp.Body)
		assert.Equal(t, "checkpointing of other/foo/checkpointingFailure failed (Returning error for test)", string(body))
	})
	// 正常容器应返回 200。
	// Now test a successful checkpoint succeeds
	setPodByNameFunc(fw, podNamespace, podName, expectedContainerName)
	t.Run("checkpointing succeeds", func(t *testing.T) {
		resp, err := http.Post(fw.testHTTPServer.URL+"/checkpoint/"+podNamespace+"/"+podName+"/"+expectedContainerName, "", nil)
		if err != nil {
			t.Errorf("Got error POSTing: %v", err)
		}
		assert.Equal(t, 200, resp.StatusCode)
	})

	// 特性门控关闭时，checkpoint 端点应返回 404。
	// Now test for 404 if checkpointing support is explicitly disabled.
	fw.testHTTPServer.Close()
	fw = setupTest(false)
	defer fw.testHTTPServer.Close()
	setPodByNameFunc(fw, podNamespace, podName, expectedContainerName)
	t.Run("checkpointing fails because disabled", func(t *testing.T) {
		resp, err := http.Post(fw.testHTTPServer.URL+"/checkpoint/"+podNamespace+"/"+podName+"/"+expectedContainerName, "", nil)
		if err != nil {
			t.Errorf("Got error POSTing: %v", err)
		}
		assert.Equal(t, 404, resp.StatusCode)
	})
}

// 构造带指定流式协议版本头的 HTTP 请求。
func makeReq(t *testing.T, method, url, clientProtocol string) *http.Request {
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		t.Fatalf("error creating request: %v", err)
	}
	req.Header.Set("Content-Type", "")
	req.Header.Add("X-Stream-Protocol-Version", clientProtocol)
	return req
}

// 验证 exec 连接在空闲超时后会被服务端主动关闭。
func TestServeExecInContainerIdleTimeout(t *testing.T) {
	tCtx := ktesting.Init(t)
	ss, err := newTestStreamingServer(100 * time.Millisecond)
	require.NoError(t, err)
	defer ss.testHTTPServer.Close()
	fw := newServerTestWithDebug(tCtx, true, ss)
	defer fw.testHTTPServer.Close()

	podNamespace := "other"
	podName := "foo"
	expectedContainerName := "baz"

	url := fw.testHTTPServer.URL + "/exec/" + podNamespace + "/" + podName + "/" + expectedContainerName + "?c=ls&c=-a&" + api.ExecStdinParam + "=1"

	upgradeRoundTripper, err := spdy.NewRoundTripper(&tls.Config{})
	if err != nil {
		t.Fatalf("Error creating SpdyRoundTripper: %v", err)
	}
	c := &http.Client{Transport: upgradeRoundTripper}

	resp, err := c.Do(makeReq(t, "POST", url, "v4.channel.k8s.io"))
	if err != nil {
		t.Fatalf("Got error POSTing: %v", err)
	}
	defer resp.Body.Close()

	upgradeRoundTripper.Dialer = &net.Dialer{
		Deadline: time.Now().Add(60 * time.Second),
		Timeout:  60 * time.Second,
	}
	conn, err := upgradeRoundTripper.NewConnection(resp)
	if err != nil {
		t.Fatalf("Unexpected error creating streaming connection: %s", err)
	}
	if conn == nil {
		t.Fatal("Unexpected nil connection")
	}

	<-conn.CloseChan()
}

// exec 与 attach 的共用测试：覆盖 stdin/stdout/stderr/tty 各组合，
// 校验请求参数、流式数据与最终调用的处理函数。
func testExecAttach(t *testing.T, verb string) {
	tCtx := ktesting.Init(t)
	tests := map[string]struct {
		stdin              bool
		stdout             bool
		stderr             bool
		tty                bool
		responseStatusCode int
		uid                bool
	}{
		"no input or output":           {responseStatusCode: http.StatusBadRequest},
		"stdin":                        {stdin: true, responseStatusCode: http.StatusSwitchingProtocols},
		"stdout":                       {stdout: true, responseStatusCode: http.StatusSwitchingProtocols},
		"stderr":                       {stderr: true, responseStatusCode: http.StatusSwitchingProtocols},
		"stdout and stderr":            {stdout: true, stderr: true, responseStatusCode: http.StatusSwitchingProtocols},
		"stdin stdout and stderr":      {stdin: true, stdout: true, stderr: true, responseStatusCode: http.StatusSwitchingProtocols},
		"stdin stdout stderr with uid": {stdin: true, stdout: true, stderr: true, responseStatusCode: http.StatusSwitchingProtocols, uid: true},
	}

	for desc := range tests {
		test := tests[desc]
		t.Run(desc, func(t *testing.T) {
			ss, err := newTestStreamingServer(0)
			require.NoError(t, err)
			defer ss.testHTTPServer.Close()
			fw := newServerTestWithDebug(tCtx, true, ss)
			defer fw.testHTTPServer.Close()
			fmt.Println(desc)

			podNamespace := "other"
			podName := "foo"
			expectedPodName := getPodName(podName, podNamespace)
			expectedContainerName := "baz"
			expectedCommand := "ls -a"
			expectedStdin := "stdin"
			expectedStdout := "stdout"
			expectedStderr := "stderr"
			done := make(chan struct{})
			clientStdoutReadDone := make(chan struct{})
			clientStderrReadDone := make(chan struct{})
			execInvoked := false
			attachInvoked := false

			checkStream := func(podFullName string, uid types.UID, containerName string, streamOpts remotecommandserver.Options) {
				assert.Equal(t, expectedPodName, podFullName, "podFullName")
				if test.uid {
					assert.Equal(t, testUID, string(uid), "uid")
				}
				assert.Equal(t, expectedContainerName, containerName, "containerName")
				assert.Equal(t, test.stdin, streamOpts.Stdin, "stdin")
				assert.Equal(t, test.stdout, streamOpts.Stdout, "stdout")
				assert.Equal(t, test.tty, streamOpts.TTY, "tty")
				assert.Equal(t, !test.tty && test.stderr, streamOpts.Stderr, "stderr")
			}

			fw.fakeKubelet.getExecCheck = func(podFullName string, uid types.UID, containerName string, cmd []string, streamOpts remotecommandserver.Options) {
				execInvoked = true
				assert.Equal(t, expectedCommand, strings.Join(cmd, " "), "cmd")
				checkStream(podFullName, uid, containerName, streamOpts)
			}

			fw.fakeKubelet.getAttachCheck = func(podFullName string, uid types.UID, containerName string, streamOpts remotecommandserver.Options) {
				attachInvoked = true
				checkStream(podFullName, uid, containerName, streamOpts)
			}

			testStream := func(containerID string, in io.Reader, out, stderr io.WriteCloser, tty bool, done chan struct{}) error {
				close(done)
				assert.Equal(t, testContainerID, containerID, "containerID")
				assert.Equal(t, test.tty, tty, "tty")
				require.Equal(t, test.stdin, in != nil, "in")
				require.Equal(t, test.stdout, out != nil, "out")
				require.Equal(t, !test.tty && test.stderr, stderr != nil, "err")

				if test.stdin {
					b := make([]byte, 10)
					n, err := in.Read(b)
					assert.NoError(t, err, "reading from stdin")
					assert.Equal(t, expectedStdin, string(b[0:n]), "content from stdin")
				}

				if test.stdout {
					_, err := out.Write([]byte(expectedStdout))
					assert.NoError(t, err, "writing to stdout")
					out.Close()
					<-clientStdoutReadDone
				}

				if !test.tty && test.stderr {
					_, err := stderr.Write([]byte(expectedStderr))
					assert.NoError(t, err, "writing to stderr")
					stderr.Close()
					<-clientStderrReadDone
				}
				return nil
			}

			ss.fakeRuntime.execFunc = func(containerID string, cmd []string, stdin io.Reader, stdout, stderr io.WriteCloser, tty bool, resize <-chan remotecommandserver.TerminalSize) error {
				assert.Equal(t, expectedCommand, strings.Join(cmd, " "), "cmd")
				return testStream(containerID, stdin, stdout, stderr, tty, done)
			}

			ss.fakeRuntime.attachFunc = func(containerID string, stdin io.Reader, stdout, stderr io.WriteCloser, tty bool, resize <-chan remotecommandserver.TerminalSize) error {
				return testStream(containerID, stdin, stdout, stderr, tty, done)
			}

			var url string
			if test.uid {
				url = fw.testHTTPServer.URL + "/" + verb + "/" + podNamespace + "/" + podName + "/" + testUID + "/" + expectedContainerName + "?ignore=1"
			} else {
				url = fw.testHTTPServer.URL + "/" + verb + "/" + podNamespace + "/" + podName + "/" + expectedContainerName + "?ignore=1"
			}
			if verb == "exec" {
				url += "&command=ls&command=-a"
			}
			if test.stdin {
				url += "&" + api.ExecStdinParam + "=1"
			}
			if test.stdout {
				url += "&" + api.ExecStdoutParam + "=1"
			}
			if test.stderr && !test.tty {
				url += "&" + api.ExecStderrParam + "=1"
			}
			if test.tty {
				url += "&" + api.ExecTTYParam + "=1"
			}

			var (
				resp                *http.Response
				upgradeRoundTripper httpstream.UpgradeRoundTripper
				c                   *http.Client
			)
			upgradeRoundTripper, err = spdy.NewRoundTripper(&tls.Config{})
			if err != nil {
				t.Fatalf("Error creating SpdyRoundTripper: %v", err)
			}
			c = &http.Client{Transport: upgradeRoundTripper}

			resp, err = c.Do(makeReq(t, "POST", url, "v4.channel.k8s.io"))
			require.NoError(t, err, "POSTing")
			defer resp.Body.Close()

			_, err = io.ReadAll(resp.Body)
			assert.NoError(t, err, "reading response body")

			require.Equal(t, test.responseStatusCode, resp.StatusCode, "response status")
			if test.responseStatusCode != http.StatusSwitchingProtocols {
				return
			}

			conn, err := upgradeRoundTripper.NewConnection(resp)
			require.NoError(t, err, "creating streaming connection")
			defer conn.Close()

			h := http.Header{}
			h.Set(api.StreamType, api.StreamTypeError)
			_, err = conn.CreateStream(h)
			require.NoError(t, err, "creating error stream")

			if test.stdin {
				h.Set(api.StreamType, api.StreamTypeStdin)
				stream, err := conn.CreateStream(h)
				require.NoError(t, err, "creating stdin stream")
				_, err = stream.Write([]byte(expectedStdin))
				require.NoError(t, err, "writing to stdin stream")
			}

			var stdoutStream httpstream.Stream
			if test.stdout {
				h.Set(api.StreamType, api.StreamTypeStdout)
				stdoutStream, err = conn.CreateStream(h)
				require.NoError(t, err, "creating stdout stream")
			}

			var stderrStream httpstream.Stream
			if test.stderr && !test.tty {
				h.Set(api.StreamType, api.StreamTypeStderr)
				stderrStream, err = conn.CreateStream(h)
				require.NoError(t, err, "creating stderr stream")
			}

			if test.stdout {
				output := make([]byte, 10)
				n, err := stdoutStream.Read(output)
				close(clientStdoutReadDone)
				assert.NoError(t, err, "reading from stdout stream")
				assert.Equal(t, expectedStdout, string(output[0:n]), "stdout")
			}

			if test.stderr && !test.tty {
				output := make([]byte, 10)
				n, err := stderrStream.Read(output)
				close(clientStderrReadDone)
				assert.NoError(t, err, "reading from stderr stream")
				assert.Equal(t, expectedStderr, string(output[0:n]), "stderr")
			}

			// 等待服务端处理完成，再确认调用的是 exec 还是 attach。
			// wait for the server to finish before checking if the attach/exec funcs were invoked
			<-done

			if verb == "exec" {
				assert.True(t, execInvoked, "exec should be invoked")
				assert.False(t, attachInvoked, "attach should not be invoked")
			} else {
				assert.True(t, attachInvoked, "attach should be invoked")
				assert.False(t, execInvoked, "exec should not be invoked")
			}
		})
	}
}

// 用 exec 动词运行 testExecAttach。
func TestServeExecInContainer(t *testing.T) {
	testExecAttach(t, "exec")
}

// 用 attach 动词运行 testExecAttach。
func TestServeAttachContainer(t *testing.T) {
	testExecAttach(t, "attach")
}

// 验证通过 WebSocket 客户端执行 attach 时数据能正确回传。
func TestWebsocketExecAttach(t *testing.T) {
	tCtx := ktesting.Init(t)
	ss, err := newTestStreamingServer(0)
	require.NoError(t, err)
	defer ss.testHTTPServer.Close()
	fw := newServerTestWithDebug(tCtx, true, ss)
	defer fw.testHTTPServer.Close()

	podNamespace := "other"
	podName := "foo"
	expectedContainerName := "baz"
	expectedStdin := "stdin"
	done := make(chan struct{})
	attachInvoked := false

	fw.fakeKubelet.getAttachCheck = func(podFullName string, uid types.UID, containerName string, streamOpts remotecommandserver.Options) {
		attachInvoked = true
	}

	ss.fakeRuntime.attachFunc = func(containerID string, stdin io.Reader, stdout, stderr io.WriteCloser, tty bool, resize <-chan remotecommandserver.TerminalSize) error {
		defer close(done)
		defer stdout.Close() //nolint:errcheck
		_, err := io.Copy(stdout, stdin)
		return err
	}

	requestURL := fw.testHTTPServer.URL + "/attach/" + podNamespace + "/" + podName + "/" + expectedContainerName + "?input=1&output=1"

	websocketLocation, err := url.Parse(fw.testHTTPServer.URL)
	require.NoError(t, err)

	exec, err := remotecommand.NewWebSocketExecutor(&rest.Config{Host: websocketLocation.Host}, "GET", requestURL)
	require.NoError(t, err)

	var stdout bytes.Buffer
	options := &remotecommand.StreamOptions{
		Stdin:  bytes.NewReader([]byte(expectedStdin)),
		Stdout: &stdout,
	}

	errorChan := make(chan error)
	go func() {
		errorChan <- exec.StreamWithContext(tCtx, *options)
	}()

	select {
	case <-time.After(wait.ForeverTestTimeout):
		t.Fatalf("expect stream to be closed after connection is closed.")
	case err := <-errorChan:
		require.NoError(t, err)
	}

	assert.Equal(t, expectedStdin, stdout.String())
	assert.True(t, attachInvoked, "attach should be invoked")
	<-done
}

// 验证 port forward 连接在空闲超时后会被服务端关闭。
func TestServePortForwardIdleTimeout(t *testing.T) {
	tCtx := ktesting.Init(t)
	ss, err := newTestStreamingServer(100 * time.Millisecond)
	require.NoError(t, err)
	defer ss.testHTTPServer.Close()
	fw := newServerTestWithDebug(tCtx, true, ss)
	defer fw.testHTTPServer.Close()

	podNamespace := "other"
	podName := "foo"

	url := fw.testHTTPServer.URL + "/portForward/" + podNamespace + "/" + podName

	upgradeRoundTripper, err := spdy.NewRoundTripper(&tls.Config{})
	if err != nil {
		t.Fatalf("Error creating SpdyRoundTripper: %v", err)
	}
	c := &http.Client{Transport: upgradeRoundTripper}

	req := makeReq(t, "POST", url, "portforward.k8s.io")
	resp, err := c.Do(req)
	if err != nil {
		t.Fatalf("Got error POSTing: %v", err)
	}
	defer resp.Body.Close()

	conn, err := upgradeRoundTripper.NewConnection(resp)
	if err != nil {
		t.Fatalf("Unexpected error creating streaming connection: %s", err)
	}
	if conn == nil {
		t.Fatal("Unexpected nil connection")
	}
	defer conn.Close()

	<-conn.CloseChan()
}

// 表驱动验证 /portForward 对端口范围的校验以及双向数据转发。
func TestServePortForward(t *testing.T) {
	tCtx := ktesting.Init(t)
	tests := map[string]struct {
		port          string
		uid           bool
		clientData    string
		containerData string
		shouldError   bool
	}{
		"no port":                       {port: "", shouldError: true},
		"none number port":              {port: "abc", shouldError: true},
		"negative port":                 {port: "-1", shouldError: true},
		"too large port":                {port: "65536", shouldError: true},
		"0 port":                        {port: "0", shouldError: true},
		"min port":                      {port: "1", shouldError: false},
		"normal port":                   {port: "8000", shouldError: false},
		"normal port with data forward": {port: "8000", clientData: "client data", containerData: "container data", shouldError: false},
		"max port":                      {port: "65535", shouldError: false},
		"normal port with uid":          {port: "8000", uid: true, shouldError: false},
	}

	podNamespace := "other"
	podName := "foo"

	for desc := range tests {
		test := tests[desc]
		t.Run(desc, func(t *testing.T) {
			ss, err := newTestStreamingServer(0)
			require.NoError(t, err)
			defer ss.testHTTPServer.Close()
			fw := newServerTestWithDebug(tCtx, true, ss)
			defer fw.testHTTPServer.Close()

			portForwardFuncDone := make(chan struct{})

			fw.fakeKubelet.getPortForwardCheck = func(name, namespace string, uid types.UID, opts portforward.V4Options) {
				assert.Equal(t, podName, name, "pod name")
				assert.Equal(t, podNamespace, namespace, "pod namespace")
				if test.uid {
					assert.Equal(t, testUID, string(uid), "uid")
				}
			}

			ss.fakeRuntime.portForwardFunc = func(podSandboxID string, port int32, stream io.ReadWriteCloser) error {
				defer close(portForwardFuncDone)
				assert.Equal(t, testPodSandboxID, podSandboxID, "pod sandbox id")
				// The port should be valid if it reaches here.
				testPort, err := strconv.ParseInt(test.port, 10, 32)
				require.NoError(t, err, "parse port")
				assert.Equal(t, int32(testPort), port, "port")

				if test.clientData != "" {
					fromClient := make([]byte, 32)
					n, err := stream.Read(fromClient)
					assert.NoError(t, err, "reading client data")
					assert.Equal(t, test.clientData, string(fromClient[0:n]), "client data")
				}

				if test.containerData != "" {
					_, err := stream.Write([]byte(test.containerData))
					assert.NoError(t, err, "writing container data")
				}

				return nil
			}

			var url string
			if test.uid {
				url = fmt.Sprintf("%s/portForward/%s/%s/%s", fw.testHTTPServer.URL, podNamespace, podName, testUID)
			} else {
				url = fmt.Sprintf("%s/portForward/%s/%s", fw.testHTTPServer.URL, podNamespace, podName)
			}

			var (
				upgradeRoundTripper httpstream.UpgradeRoundTripper
				c                   *http.Client
			)

			upgradeRoundTripper, err = spdy.NewRoundTripper(&tls.Config{})
			if err != nil {
				t.Fatalf("Error creating SpdyRoundTripper: %v", err)
			}
			c = &http.Client{Transport: upgradeRoundTripper}

			req := makeReq(t, "POST", url, "portforward.k8s.io")
			resp, err := c.Do(req)
			require.NoError(t, err, "POSTing")
			defer resp.Body.Close()

			assert.Equal(t, http.StatusSwitchingProtocols, resp.StatusCode, "status code")

			conn, err := upgradeRoundTripper.NewConnection(resp)
			require.NoError(t, err, "creating streaming connection")
			defer conn.Close()

			headers := http.Header{}
			headers.Set("streamType", "error")
			headers.Set("port", test.port)
			_, err = conn.CreateStream(headers)
			assert.Equal(t, test.shouldError, err != nil, "expect error")

			if test.shouldError {
				return
			}

			headers.Set("streamType", "data")
			headers.Set("port", test.port)
			dataStream, err := conn.CreateStream(headers)
			require.NoError(t, err, "create stream")

			if test.clientData != "" {
				_, err := dataStream.Write([]byte(test.clientData))
				assert.NoError(t, err, "writing client data")
			}

			if test.containerData != "" {
				fromContainer := make([]byte, 32)
				n, err := dataStream.Read(fromContainer)
				assert.NoError(t, err, "reading container data")
				assert.Equal(t, test.containerData, string(fromContainer[0:n]), "container data")
			}

			<-portForwardFuncDone
		})
	}
}

// 验证各端点路径被归入正确的指标 bucket，未知路径归入 other。
func TestMetricBuckets(t *testing.T) {
	tCtx := ktesting.Init(t)
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, zpagesfeatures.ComponentStatusz, true)

	tests := map[string]struct {
		url    string
		bucket string
	}{
		"healthz endpoint":                {url: "/healthz", bucket: "healthz"},
		"attach":                          {url: "/attach/podNamespace/podID/containerName", bucket: "attach"},
		"attach with uid":                 {url: "/attach/podNamespace/podID/uid/containerName", bucket: "attach"},
		"configz":                         {url: "/configz", bucket: "configz"},
		"containerLogs":                   {url: "/containerLogs/podNamespace/podID/containerName", bucket: "containerLogs"},
		"debug v flags":                   {url: "/debug/flags/v", bucket: "debug"},
		"pprof with sub":                  {url: "/debug/pprof/subpath", bucket: "debug"},
		"exec":                            {url: "/exec/podNamespace/podID/containerName", bucket: "exec"},
		"exec with uid":                   {url: "/exec/podNamespace/podID/uid/containerName", bucket: "exec"},
		"healthz":                         {url: "/healthz/", bucket: "healthz"},
		"healthz log sub":                 {url: "/healthz/log", bucket: "healthz"},
		"healthz ping":                    {url: "/healthz/ping", bucket: "healthz"},
		"healthz sync loop":               {url: "/healthz/syncloop", bucket: "healthz"},
		"logs":                            {url: "/logs/", bucket: "logs"},
		"logs with path":                  {url: "/logs/logpath", bucket: "logs"},
		"metrics":                         {url: "/metrics", bucket: "metrics"},
		"metrics cadvisor sub":            {url: "/metrics/cadvisor", bucket: "metrics/cadvisor"},
		"metrics probes sub":              {url: "/metrics/probes", bucket: "metrics/probes"},
		"metrics resource sub":            {url: "/metrics/resource", bucket: "metrics/resource"},
		"pods":                            {url: "/pods/", bucket: "pods"},
		"portForward":                     {url: "/portForward/podNamespace/podID", bucket: "portForward"},
		"portForward with uid":            {url: "/portForward/podNamespace/podID/uid", bucket: "portForward"},
		"run":                             {url: "/run/podNamespace/podID/containerName", bucket: "run"},
		"run with uid":                    {url: "/run/podNamespace/podID/uid/containerName", bucket: "run"},
		"runningpods":                     {url: "/runningpods/", bucket: "runningpods"},
		"stats":                           {url: "/stats/", bucket: "stats"},
		"stats summary sub":               {url: "/stats/summary", bucket: "stats"},
		"statusz":                         {url: "/statusz", bucket: "statusz"},
		"/flagz":                          {url: "/flagz", bucket: "flagz"},
		"invalid path":                    {url: "/junk", bucket: "other"},
		"invalid path starting with good": {url: "/healthzjunk", bucket: "other"},
	}
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, zpagesfeatures.ComponentFlagz, true)
	fw := newServerTest(tCtx)
	defer fw.testHTTPServer.Close()

	for _, test := range tests {
		path := test.url
		bucket := test.bucket
		require.Equal(t, fw.serverUnderTest.getMetricBucket(path), bucket)
	}
}

// 验证 HTTP 方法被归入正确的指标 bucket，未知方法归入 other。
func TestMetricMethodBuckets(t *testing.T) {
	tCtx := ktesting.Init(t)
	tests := map[string]struct {
		method string
		bucket string
	}{
		"normal GET":     {method: "GET", bucket: "GET"},
		"normal POST":    {method: "POST", bucket: "POST"},
		"invalid method": {method: "WEIRD", bucket: "other"},
	}

	fw := newServerTest(tCtx)
	defer fw.testHTTPServer.Close()

	for _, test := range tests {
		method := test.method
		bucket := test.bucket
		require.Equal(t, fw.serverUnderTest.getMetricMethodBucket(method), bucket)
	}
}

// 验证 EnableDebuggingHandlers 关闭时，即使单独开启日志/性能分析开关，
// 调试端点也不会被注册。
func TestDebuggingDisabledHandlers(t *testing.T) {
	tCtx := ktesting.Init(t)
	// 兼容性要求：仅开启 systemLog/profiling/debugFlags 而不开启 debugging handlers 时，
	// /logs、/pprof、/flags 都不应被注册。
	// for backward compatibility even if enablesystemLogHandler or enableProfilingHandler is set but not
	// enableDebuggingHandler then /logs, /pprof and /flags shouldn't be served.
	kubeCfg := &kubeletconfiginternal.KubeletConfiguration{
		EnableDebuggingHandlers: false,
		EnableSystemLogHandler:  true,
		EnableDebugFlagsHandler: true,
		EnableProfilingHandler:  true,
	}
	fw := newServerTestWithDebuggingHandlers(tCtx, kubeCfg, nil)
	defer fw.testHTTPServer.Close()

	paths := []string{
		"/run", "/exec", "/attach", "/portForward", "/containerLogs", "/runningpods",
		"/run/", "/exec/", "/attach/", "/portForward/", "/containerLogs/", "/runningpods/",
		"/run/xxx", "/exec/xxx", "/attach/xxx", "/debug/pprof/profile", "/logs/kubelet.log",
	}

	for _, p := range paths {
		verifyEndpointResponse(t, fw, p, "Debug endpoints are disabled.\n")
	}
}

// 验证调试端点开启但子开关关闭时，/logs、/debug/pprof、/debug/flags 返回禁用提示。
func TestDisablingLogAndProfilingHandler(t *testing.T) {
	tCtx := ktesting.Init(t)
	kubeCfg := &kubeletconfiginternal.KubeletConfiguration{
		EnableDebuggingHandlers: true,
	}
	fw := newServerTestWithDebuggingHandlers(tCtx, kubeCfg, nil)
	defer fw.testHTTPServer.Close()

	// 调试总开关打开但子开关关闭，各端点应返回对应的禁用提示。
	// verify debug endpoints are disabled
	verifyEndpointResponse(t, fw, "/logs/kubelet.log", "logs endpoint is disabled.\n")
	verifyEndpointResponse(t, fw, "/debug/pprof/profile?seconds=2", "profiling endpoint is disabled.\n")
	verifyEndpointResponse(t, fw, "/debug/flags/v", "flags endpoint is disabled.\n")
}

// 验证 /stats/summary 在表单解析失败时返回 500 并包含错误信息。
func TestFailedParseParamsSummaryHandler(t *testing.T) {
	tCtx := ktesting.Init(t)
	fw := newServerTest(tCtx)
	defer fw.testHTTPServer.Close()

	resp, err := http.Post(fw.testHTTPServer.URL+"/stats/summary", "invalid/content/type", nil)
	assert.NoError(t, err)
	defer resp.Body.Close()
	v, err := io.ReadAll(resp.Body)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	assert.Contains(t, string(v), "parse form failed")
}

// 以 GET 和 POST 请求给定路径，断言返回 405 且响应体为期望的禁用提示。
func verifyEndpointResponse(t *testing.T, fw *serverTestFramework, path string, expectedResponse string) {
	resp, err := http.Get(fw.testHTTPServer.URL + path)
	require.NoError(t, err)
	assert.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, expectedResponse, string(body))

	resp, err = http.Post(fw.testHTTPServer.URL+path, "", nil)
	require.NoError(t, err)
	assert.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)
	body, err = io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, expectedResponse, string(body))
}

// 验证 getURLRootPath 对路径的裁剪规则。
func TestTrimURLPath(t *testing.T) {
	tests := []struct {
		path, expected string
	}{
		{"", ""},
		{"//", ""},
		{"/pods", "pods"},
		{"pods", "pods"},
		{"pods/", "pods"},
		{"good/", "good"},
		{"pods/probes", "pods"},
		{"metrics", "metrics"},
		{"metrics/resource", "metrics/resource"},
		{"metrics/hello", "metrics/hello"},
	}

	for _, test := range tests {
		assert.Equalf(t, test.expected, getURLRootPath(test.path), "path is: %s", test.path)
	}
}

// 验证细粒度授权开启时，子资源授权被拒会回退到 proxy 授权，
// 并正确统计授权调用次数。
func TestFineGrainedAuthz(t *testing.T) {
	tCtx := ktesting.Init(t)
	// Enable features.ContainerCheckpoint during test
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.KubeletFineGrainedAuthz, true)

	fw := newServerTest(tCtx)
	defer fw.testHTTPServer.Close()

	attributesGetter := NewNodeAuthorizerAttributesGetter(authzTestNodeName)

	testCases := []struct {
		name                     string
		path                     string
		expectedSubResources     []string
		authorizer               func(authorizer.Attributes) (authorized authorizer.Decision, reason string, err error)
		wantStatusCode           int
		wantCalledAuthorizeCount int
	}{
		{
			name:                 "both subresources rejected",
			path:                 "/configz",
			expectedSubResources: []string{"configz", "proxy"},
			authorizer: func(authorizer.Attributes) (authorized authorizer.Decision, reason string, err error) {
				return authorizer.DecisionNoOpinion, "", nil
			},
			wantStatusCode:           403,
			wantCalledAuthorizeCount: 2,
		},
		{
			name:                 "fine grained rejected, proxy accepted",
			path:                 "/configz",
			expectedSubResources: []string{"configz", "proxy"},
			authorizer: func(a authorizer.Attributes) (authorized authorizer.Decision, reason string, err error) {
				switch a.GetSubresource() {
				case "configz":
					return authorizer.DecisionNoOpinion, "", nil
				case "proxy":
					return authorizer.DecisionAllow, "", nil
				default:
					return authorizer.DecisionNoOpinion, "", fmt.Errorf("unexpected subresource %v", a.GetSubresource())
				}
			},
			wantStatusCode:           200,
			wantCalledAuthorizeCount: 2,
		},
		{
			name:                 "fine grained accepted",
			path:                 "/configz",
			expectedSubResources: []string{"configz", "proxy"},
			authorizer: func(a authorizer.Attributes) (authorized authorizer.Decision, reason string, err error) {
				switch a.GetSubresource() {
				case "configz":
					return authorizer.DecisionAllow, "", nil
				case "proxy":
					return authorizer.DecisionNoOpinion, "", fmt.Errorf("did not expect code to reach here")
				default:
					return authorizer.DecisionNoOpinion, "", fmt.Errorf("unexpected subresource %v", a.GetSubresource())
				}
			},
			wantStatusCode:           200,
			wantCalledAuthorizeCount: 1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {

			calledAuthenticate := false
			calledAuthorizeCount := 0
			calledAttributes := false

			fw.fakeAuth.authenticateFunc = func(req *http.Request) (*authenticator.Response, bool, error) {
				calledAuthenticate = true
				return &authenticator.Response{User: AuthzTestUser()}, true, nil
			}
			fw.fakeAuth.attributesFunc = func(u user.Info, req *http.Request) []authorizer.Attributes {
				calledAttributes = true
				attrs := attributesGetter.GetRequestAttributes(tCtx, u, req)
				var gotSubresources []string
				for _, attr := range attrs {
					gotSubresources = append(gotSubresources, attr.GetSubresource())
				}
				require.Equal(t, tc.expectedSubResources, gotSubresources)
				return attrs
			}
			fw.fakeAuth.authorizeFunc = func(a authorizer.Attributes) (authorized authorizer.Decision, reason string, err error) {
				calledAuthorizeCount += 1
				return tc.authorizer(a)
			}

			req, err := http.NewRequest("GET", fw.testHTTPServer.URL+tc.path, nil)
			require.NoError(t, err)

			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			assert.Equal(t, tc.wantStatusCode, resp.StatusCode)
			assert.True(t, calledAuthenticate, "Authenticate was not called")
			assert.True(t, calledAttributes, "Attributes were not called")
			assert.Equal(t, tc.wantCalledAuthorizeCount, calledAuthorizeCount)
		})
	}
}

// 验证两次创建 Server 不会因 /metrics/slis 重复注册而冲突。
func TestNewServerRegistersMetricsSLIsEndpointTwice(t *testing.T) {
	tCtx := ktesting.Init(t)
	host := &fakeKubelet{}
	resourceAnalyzer := stats.NewResourceAnalyzer(tCtx, nil, time.Minute, &record.FakeRecorder{})

	server1 := NewServer(tCtx, host, resourceAnalyzer, []healthz.HealthChecker{}, flagz.NamedFlagSetsReader{}, nil, nil)
	server2 := NewServer(tCtx, host, resourceAnalyzer, []healthz.HealthChecker{}, flagz.NamedFlagSetsReader{}, nil, nil)

	// 两个 Server 实例都应成功注册 /metrics/slis，不能因重复注册 panic。
	// Check if both servers registered the /metrics/slis endpoint
	assert.Contains(t, server1.restfulCont.RegisteredHandlePaths(), "/metrics/slis", "First server should register /metrics/slis")
	assert.Contains(t, server2.restfulCont.RegisteredHandlePaths(), "/metrics/slis", "Second server should register /metrics/slis")
}

// 验证 HTTP 请求耗时指标记录的是真实处理时间，而非固定的小值。
func TestServeHTTPRequestDurationMetric(t *testing.T) {
	tCtx := ktesting.Init(t)
	fw := newServerTest(tCtx)
	defer fw.testHTTPServer.Close()

	// 测试前注册并重置指标，避免历史样本干扰。
	// Register and reset the metric before the test
	servermetrics.Register()
	servermetrics.HTTPRequestsDuration.Reset()

	// 在 pods handler 中人为延迟以模拟真实处理耗时。
	// 50ms 足以与约 2 微秒的错误值区分，同时不会明显拖慢测试。
	// Add a delay to the pods handler to simulate request processing time.
	// We use 50ms which is long enough to be clearly distinguishable from
	// the ~2 microsecond bug, but short enough to not slow down tests.
	handlerDelay := 50 * time.Millisecond
	fw.fakeKubelet.podsFunc = func() []*v1.Pod {
		time.Sleep(handlerDelay)
		return []*v1.Pod{}
	}

	// 请求 pods 端点以产生一次指标观测。
	// Make a request to the pods endpoint
	resp, err := http.Get(fw.testHTTPServer.URL + "/pods/")
	require.NoError(t, err)
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// 读完响应体，确保请求已完整处理后再读取指标。
	// Read the body to ensure the request is fully processed
	_, err = io.ReadAll(resp.Body)
	require.NoError(t, err)

	// 从指标中取出记录的耗时值。
	// Get the recorded duration from the metric
	observerMetric := servermetrics.HTTPRequestsDuration.WithLabelValues("GET", "pods", "readwrite", "false")
	metricValue, err := testutil.GetHistogramMetricValue(observerMetric)
	require.NoError(t, err)

	// 以 handler 延迟作为耗时下界，避免基于百分比的时序敏感断言。
	// Use the handler delay as the minimum expected duration. This avoids any
	// timing-sensitive percentage-based checks.
	minExpectedDuration := handlerDelay.Seconds()
	assert.GreaterOrEqual(t, metricValue, minExpectedDuration,
		"HTTP request duration metric recorded %v seconds, expected at least %v seconds. ",
		metricValue, minExpectedDuration,
	)
}

// 验证 getExec 在 ExtendWebSocketsToKubelet 开启且收到 v5 请求时选用协议转换 handler，
// 否则回退到普通 UpgradeAwareHandler。
// 可观测差异：转换 handler 直接接受 v5 并在 101 响应中回显 v5.channel.k8s.io；
// 否则 v5 升级会被仅支持 v4 的 wsstream 层拒绝（客户端表现为 bad handshake）。
// 客户端使用 v4 时，普通 handler 也能把升级代理给流式服务端并成功。
func TestGetExecWebSocketHandlerSelection(t *testing.T) {
	tests := []struct {
		name                   string
		enableExtendWebSockets bool
		subprotocols           []string // defaults to v5 if nil
		expectDialError        bool
		expectedSubprotocol    string
		expectMetricInc        bool
	}{
		{
			name:                   "feature gate enabled uses translating handler, v5 negotiated",
			enableExtendWebSockets: true,
			expectDialError:        false,
			expectedSubprotocol:    "v5.channel.k8s.io",
			expectMetricInc:        true,
		},
		{
			name:                   "feature gate disabled uses UpgradeAwareHandler, v5 rejected",
			enableExtendWebSockets: false,
			expectDialError:        true,
		},
		{
			// When the gate is enabled but the request is not v5, IsWebSocketRequestWithStreamCloseProtocol
			// returns false so getExec falls through to the plain UpgradeAwareHandler, which proxies the
			// request to the streaming server. The streaming server understands v4 and accepts it.
			name:                   "feature gate enabled, v4 request uses UpgradeAwareHandler, v4 negotiated",
			enableExtendWebSockets: true,
			subprotocols:           []string{"v4.channel.k8s.io"},
			expectDialError:        false,
			expectedSubprotocol:    "v4.channel.k8s.io",
		},
	}

	servermetrics.Register()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			servermetrics.ResetForTest()
			tCtx := ktesting.Init(t)
			ss, err := newTestStreamingServer(0)
			require.NoError(t, err)
			defer ss.testHTTPServer.Close()
			fw := newServerTestWithDebug(tCtx, true, ss)
			defer fw.testHTTPServer.Close()

			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.ExtendWebSocketsToKubelet, tt.enableExtendWebSockets)

			ss.fakeRuntime.execFunc = func(_ string, _ []string, _ io.Reader, stdout, _ io.WriteCloser, _ bool, _ <-chan remotecommandserver.TerminalSize) error {
				stdout.Close() //nolint:errcheck
				return nil
			}

			execURL := strings.Replace(fw.testHTTPServer.URL, "http://", "ws://", 1) +
				"/exec/default/foo/baz?command=echo&output=1"

			subprotocols := tt.subprotocols
			if len(subprotocols) == 0 {
				subprotocols = []string{"v5.channel.k8s.io"}
			}
			dialer := gwebsocket.Dialer{
				Subprotocols: subprotocols,
			}
			conn, resp, err := dialer.Dial(execURL, nil)
			if tt.expectDialError {
				require.Error(t, err, "expected dial to fail when feature gate is disabled")
				return
			}
			require.NoError(t, err)
			defer conn.Close() //nolint:errcheck

			assert.Equal(t, http.StatusSwitchingProtocols, resp.StatusCode)
			assert.Equal(t, tt.expectedSubprotocol, conn.Subprotocol())

			if tt.expectMetricInc {
				expected := `
# HELP kubelet_websocket_streaming_requests_total [ALPHA] Total number of WebSocket streaming requests (exec/attach/portforward) received by the kubelet.
# TYPE kubelet_websocket_streaming_requests_total counter
kubelet_websocket_streaming_requests_total{subresource="exec"} 1
`
				if err := testutil.GatherAndCompare(legacyregistry.DefaultGatherer, strings.NewReader(expected),
					"kubelet_websocket_streaming_requests_total"); err != nil {
					t.Errorf("unexpected metric output: %v", err)
				}
			}
		})
	}
}

// 与 TestGetExecWebSocketHandlerSelection 类似，验证 getAttach 的 handler 选择。
func TestGetAttachWebSocketHandlerSelection(t *testing.T) {
	tests := []struct {
		name                   string
		enableExtendWebSockets bool
		subprotocols           []string // defaults to v5 if nil
		expectDialError        bool
		expectedSubprotocol    string
		expectMetricInc        bool
	}{
		{
			name:                   "feature gate enabled uses translating handler, v5 negotiated",
			enableExtendWebSockets: true,
			expectDialError:        false,
			expectedSubprotocol:    "v5.channel.k8s.io",
			expectMetricInc:        true,
		},
		{
			name:                   "feature gate disabled uses UpgradeAwareHandler, v5 rejected",
			enableExtendWebSockets: false,
			expectDialError:        true,
		},
		{
			// When the gate is enabled but the request is not v5, IsWebSocketRequestWithStreamCloseProtocol
			// returns false so getAttach falls through to the plain UpgradeAwareHandler, which proxies the
			// request to the streaming server. The streaming server understands v4 and accepts it.
			name:                   "feature gate enabled, v4 request uses UpgradeAwareHandler, v4 negotiated",
			enableExtendWebSockets: true,
			subprotocols:           []string{"v4.channel.k8s.io"},
			expectDialError:        false,
			expectedSubprotocol:    "v4.channel.k8s.io",
		},
	}

	servermetrics.Register()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			servermetrics.ResetForTest()
			tCtx := ktesting.Init(t)
			ss, err := newTestStreamingServer(0)
			require.NoError(t, err)
			defer ss.testHTTPServer.Close()
			fw := newServerTestWithDebug(tCtx, true, ss)
			defer fw.testHTTPServer.Close()

			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.ExtendWebSocketsToKubelet, tt.enableExtendWebSockets)

			ss.fakeRuntime.attachFunc = func(_ string, _ io.Reader, stdout, _ io.WriteCloser, _ bool, _ <-chan remotecommandserver.TerminalSize) error {
				stdout.Close() //nolint:errcheck
				return nil
			}

			attachURL := strings.Replace(fw.testHTTPServer.URL, "http://", "ws://", 1) +
				"/attach/default/foo/baz?output=1"

			subprotocols := tt.subprotocols
			if len(subprotocols) == 0 {
				subprotocols = []string{"v5.channel.k8s.io"}
			}
			dialer := gwebsocket.Dialer{
				Subprotocols: subprotocols,
			}
			conn, resp, err := dialer.Dial(attachURL, nil)
			if tt.expectDialError {
				require.Error(t, err, "expected dial to fail when feature gate is disabled")
				return
			}
			require.NoError(t, err)
			defer conn.Close() //nolint:errcheck

			assert.Equal(t, http.StatusSwitchingProtocols, resp.StatusCode)
			assert.Equal(t, tt.expectedSubprotocol, conn.Subprotocol())

			if tt.expectMetricInc {
				expected := `
# HELP kubelet_websocket_streaming_requests_total [ALPHA] Total number of WebSocket streaming requests (exec/attach/portforward) received by the kubelet.
# TYPE kubelet_websocket_streaming_requests_total counter
kubelet_websocket_streaming_requests_total{subresource="attach"} 1
`
				if err := testutil.GatherAndCompare(legacyregistry.DefaultGatherer, strings.NewReader(expected),
					"kubelet_websocket_streaming_requests_total"); err != nil {
					t.Errorf("unexpected metric output: %v", err)
				}
			}
		})
	}
}

// 验证 getPortForward 在 ExtendWebSocketsToKubelet 开启且客户端使用隧道子协议时
// 选用 TunnelingHandler，否则回退到普通 UpgradeAwareHandler。
// 开启时跳过 portforward.NewV4Options（WebSocket 请求没有 port 参数会被它拒绝），
// 由 TunnelingHandler 完成 WebSocket 隧道协议到 SPDY 的转换。
// 关闭时 NewV4Options 因缺少 port 参数直接返回 400。
func TestGetPortForwardWebSocketHandlerSelection(t *testing.T) {
	tests := []struct {
		name                   string
		enableExtendWebSockets bool
		expectDialError        bool
		expectedSubprotocol    string
		expectMetricInc        bool
	}{
		{
			name:                   "feature gate enabled uses tunneling handler, tunneling subprotocol negotiated",
			enableExtendWebSockets: true,
			expectDialError:        false,
			expectedSubprotocol:    "SPDY/3.1+portforward.k8s.io",
			expectMetricInc:        true,
		},
		{
			name:                   "feature gate disabled rejects tunneling request (no port parameter)",
			enableExtendWebSockets: false,
			expectDialError:        true,
		},
	}

	servermetrics.Register()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			servermetrics.ResetForTest()
			tCtx := ktesting.Init(t)
			ss, err := newTestStreamingServer(0)
			require.NoError(t, err)
			defer ss.testHTTPServer.Close()
			fw := newServerTestWithDebug(tCtx, true, ss)
			defer fw.testHTTPServer.Close()

			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.ExtendWebSocketsToKubelet, tt.enableExtendWebSockets)

			portForwardURL := strings.Replace(fw.testHTTPServer.URL, "http://", "ws://", 1) +
				"/portForward/default/foo"

			dialer := gwebsocket.Dialer{
				Subprotocols: []string{"SPDY/3.1+portforward.k8s.io"},
			}
			conn, resp, err := dialer.Dial(portForwardURL, nil)
			if tt.expectDialError {
				require.Error(t, err, "expected dial to fail when feature gate is disabled")
				return
			}
			require.NoError(t, err)
			defer conn.Close() //nolint:errcheck

			assert.Equal(t, http.StatusSwitchingProtocols, resp.StatusCode)
			assert.Equal(t, tt.expectedSubprotocol, conn.Subprotocol())

			if tt.expectMetricInc {
				expected := `
# HELP kubelet_websocket_streaming_requests_total [ALPHA] Total number of WebSocket streaming requests (exec/attach/portforward) received by the kubelet.
# TYPE kubelet_websocket_streaming_requests_total counter
kubelet_websocket_streaming_requests_total{subresource="portforward"} 1
`
				if err := testutil.GatherAndCompare(legacyregistry.DefaultGatherer, strings.NewReader(expected),
					"kubelet_websocket_streaming_requests_total"); err != nil {
					t.Errorf("unexpected metric output: %v", err)
				}
			}
		})
	}
}

// 在 NativeHistograms 特性门控开启时验证 kubelet 指标以原生直方图形式暴露。
func TestKubeletNativeHistogramMetrics(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, metricsfeatures.NativeHistograms, true)
	metricsfeatures.ApplyFeatureGates(utilfeature.DefaultFeatureGate)
	kubeletmetrics.Register()

	tCtx := ktesting.Init(t)
	fw := newServerTest(tCtx)
	defer fw.testHTTPServer.Close()

	histogramMetric := "kubelet_pod_start_duration_seconds"
	metrics, err := testutil.ScrapeMetricsProto(fw.testHTTPServer.URL+"/metrics", fw.testHTTPServer.Client())
	if err != nil {
		t.Fatalf("failed to scrape metrics: %v", err)
	}

	mf, ok := metrics[histogramMetric]
	if !ok {
		t.Fatalf("metric %q not found in kubelet metrics endpoint", histogramMetric)
	}

	testutil.AssertHasNativeHistogram(t, mf, nil)
}

// 在 KubeletAllocatedPodsEndpoint 特性门控开/关时，
// 验证 /allocatedPods 列表与单个查询的状态码、返回内容以及 UID 匹配行为。
func TestGetAllocatedPods(t *testing.T) {
	pod1 := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod1",
			Namespace: "ns1",
			UID:       "uid1",
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{Name: "c1", Image: "img1"},
			},
		},
		Status: v1.PodStatus{
			Phase: v1.PodRunning,
		},
	}
	pod2 := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod2",
			Namespace: "ns1",
			UID:       "uid2",
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{Name: "c2", Image: "img2"},
			},
		},
		Status: v1.PodStatus{
			Phase: v1.PodRunning,
		},
	}

	tests := []struct {
		name           string
		featureEnabled bool
		path           string
		allocatedPods  []*v1.Pod
		allocatedPod   *v1.Pod
		expectedStatus int
		expectedPods   []v1.Pod
		expectedPod    *v1.Pod
	}{
		{
			name:           "feature disabled - list",
			featureEnabled: false,
			path:           "/allocatedPods",
			expectedStatus: http.StatusNotFound,
		},
		{
			name:           "feature disabled - get",
			featureEnabled: false,
			path:           "/allocatedPods/ns1/pod1",
			expectedStatus: http.StatusNotFound,
		},
		{
			name:           "feature enabled - list empty",
			featureEnabled: true,
			path:           "/allocatedPods",
			allocatedPods:  []*v1.Pod{},
			expectedStatus: http.StatusOK,
			expectedPods:   []v1.Pod{},
		},
		{
			name:           "feature enabled - list pods",
			featureEnabled: true,
			path:           "/allocatedPods",
			allocatedPods:  []*v1.Pod{pod1, pod2},
			expectedStatus: http.StatusOK,
			expectedPods: []v1.Pod{
				{
					ObjectMeta: pod1.ObjectMeta,
					Spec:       pod1.Spec,
				},
				{
					ObjectMeta: pod2.ObjectMeta,
					Spec:       pod2.Spec,
				},
			},
		},
		{
			name:           "feature enabled - get pod found",
			featureEnabled: true,
			path:           "/allocatedPods/ns1/pod1",
			allocatedPod:   pod1,
			expectedStatus: http.StatusOK,
			expectedPod: &v1.Pod{
				ObjectMeta: pod1.ObjectMeta,
				Spec:       pod1.Spec,
			},
		},
		{
			name:           "feature enabled - get pod not found",
			featureEnabled: true,
			path:           "/allocatedPods/ns1/pod3",
			expectedStatus: http.StatusNotFound,
		},
		{
			name:           "feature enabled - get pod with matching UID",
			featureEnabled: true,
			path:           "/allocatedPods/ns1/pod1/uid1",
			allocatedPod:   pod1,
			expectedStatus: http.StatusOK,
			expectedPod: &v1.Pod{
				ObjectMeta: pod1.ObjectMeta,
				Spec:       pod1.Spec,
			},
		},
		{
			name:           "feature enabled - get pod with mismatch UID",
			featureEnabled: true,
			path:           "/allocatedPods/ns1/pod1/wrong-uid",
			allocatedPod:   pod1,
			expectedStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.KubeletAllocatedPodsEndpoint, tt.featureEnabled)

			tCtx := ktesting.Init(t)
			fw := newServerTest(tCtx)
			defer fw.testHTTPServer.Close()

			fw.fakeKubelet.allocatedPodsFunc = func() []*v1.Pod {
				return tt.allocatedPods
			}
			fw.fakeKubelet.allocatedPodByNameFunc = func(namespace, name string) *v1.Pod {
				if tt.allocatedPod != nil && namespace == tt.allocatedPod.Namespace && name == tt.allocatedPod.Name {
					return tt.allocatedPod
				}
				return nil
			}

			req, err := http.NewRequest(http.MethodGet, fw.testHTTPServer.URL+tt.path, nil)
			require.NoError(t, err)

			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			assert.Equal(t, tt.expectedStatus, resp.StatusCode)

			if tt.expectedStatus != http.StatusOK {
				return
			}

			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)

			if tt.expectedPods != nil {
				var podList v1.PodList
				err = json.Unmarshal(body, &podList)
				require.NoError(t, err)
				assert.Len(t, podList.Items, len(tt.expectedPods))
				for i, expectedPod := range tt.expectedPods {
					assert.Equal(t, expectedPod.Name, podList.Items[i].Name)
					assert.Equal(t, expectedPod.Spec, podList.Items[i].Spec)
					assert.Empty(t, podList.Items[i].Status)
				}
			}

			if tt.expectedPod != nil {
				var pod v1.Pod
				err = json.Unmarshal(body, &pod)
				require.NoError(t, err)
				assert.Equal(t, tt.expectedPod.Name, pod.Name)
				assert.Equal(t, tt.expectedPod.Spec, pod.Spec)
				assert.Empty(t, pod.Status)
			}
		})
	}
}
