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
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/pprof"
	"net/url"
	"os"
	"reflect"
	goruntime "runtime"
	"strconv"
	"strings"
	"time"

	"golang.org/x/time/rate"

	"github.com/emicklei/go-restful/v3"
	cadvisormetrics "github.com/google/cadvisor/lib/container"
	"github.com/google/cadvisor/lib/metrics"
	cadvisorapi "github.com/google/cadvisor/lib/model"
	"go.opentelemetry.io/contrib/instrumentation/github.com/emicklei/go-restful/otelrestful"
	oteltrace "go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
	netutils "k8s.io/utils/net"
	"k8s.io/utils/ptr"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/httpstream/wsstream"
	"k8s.io/apimachinery/pkg/util/proxy"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/apiserver/pkg/server/flagz"
	"k8s.io/apiserver/pkg/server/healthz"
	"k8s.io/apiserver/pkg/server/httplog"

	"k8s.io/apiserver/pkg/server/dynamiccertificates"
	"k8s.io/apiserver/pkg/server/routes"
	"k8s.io/apiserver/pkg/server/statusz"
	"k8s.io/apiserver/pkg/util/compatibility"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/apiserver/pkg/util/flushwriter"
	translator "k8s.io/apiserver/pkg/util/proxy"
	"k8s.io/component-base/configz"
	"k8s.io/component-base/logs"
	compbasemetrics "k8s.io/component-base/metrics"
	metricsfeatures "k8s.io/component-base/metrics/features"
	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/component-base/metrics/prometheus/slis"
	zpagesfeatures "k8s.io/component-base/zpages/features"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/cri-client/pkg/util"
	"k8s.io/cri-streaming/pkg/streaming"
	"k8s.io/cri-streaming/pkg/streaming/portforward"
	remotecommandserver "k8s.io/cri-streaming/pkg/streaming/remotecommand"
	podresourcesapi "k8s.io/kubelet/pkg/apis/podresources/v1"
	podresourcesapiv1alpha1 "k8s.io/kubelet/pkg/apis/podresources/v1alpha1"
	podsv1alpha1 "k8s.io/kubelet/pkg/apis/pods/v1alpha1"
	kubelettypes "k8s.io/kubelet/pkg/types"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/apis/core/v1/validation"
	"k8s.io/kubernetes/pkg/features"
	kubeletconfiginternal "k8s.io/kubernetes/pkg/kubelet/apis/config"
	apisgrpc "k8s.io/kubernetes/pkg/kubelet/apis/grpc"
	"k8s.io/kubernetes/pkg/kubelet/apis/podresources"
	"k8s.io/kubernetes/pkg/kubelet/apis/pods"
	kubeletcadvisor "k8s.io/kubernetes/pkg/kubelet/cadvisor"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	"k8s.io/kubernetes/pkg/kubelet/metrics/collectors"
	oomwatcher "k8s.io/kubernetes/pkg/kubelet/oom"
	"k8s.io/kubernetes/pkg/kubelet/prober"
	servermetrics "k8s.io/kubernetes/pkg/kubelet/server/metrics"
	"k8s.io/kubernetes/pkg/kubelet/server/stats"
	"k8s.io/kubernetes/pkg/kubelet/util/format"
)

// init 把 metrics 组件用到的特性门控注册进全局 mutable gate。
func init() {
	// 把 metrics 相关的特性门控注册到全局 mutable gate，使其可被 --feature-gates 控制。
	utilruntime.Must(metricsfeatures.AddFeatureGates(utilfeature.DefaultMutableFeatureGate))
}

// 以下是 kubelet HTTP 服务暴露的各端点路径。
const (
	metricsPath         = "/metrics"
	cadvisorMetricsPath = "/metrics/cadvisor"
	resourceMetricsPath = "/metrics/resource"
	proberMetricsPath   = "/metrics/probes"
	statsPath           = "/stats/"
	logsPath            = "/logs/"
	checkpointPath      = "/checkpoint/"
	pprofBasePath       = "/debug/pprof/"
	debugFlagPath       = "/debug/flags/v"
	allocatedPodsPath   = "/allocatedPods"
	podsPath            = "/pods"
	runningPodsPath     = "/runningpods/"
)

// kubelet 组件名常量，用于 healthz、flagz、statusz 等组件的标识。
const (
	// ComponentKubelet 是 kubelet 组件名。
	ComponentKubelet = "kubelet"
)

// Server 是一个 http.Handler，通过 HTTP 暴露 kubelet 功能。
type Server struct {
	flagz                flagz.Reader
	auth                 AuthInterface
	host                 HostInterface
	restfulCont          containerInterface
	metricsBuckets       sets.Set[string]
	metricsMethodBuckets sets.Set[string]
	resourceAnalyzer     stats.ResourceAnalyzer
	extendedCheckers     []healthz.HealthChecker
}

// TLSOptions 保存 TLS 相关配置。
type TLSOptions struct {
	MinVersion       uint16
	CipherSuites     []uint16
	CurvePreferences []tls.CurveID
	CertFile         string
	KeyFile          string
	ClientCAFile     string
}

// containerInterface 定义根 container 上使用的 restful.Container 方法子集。
type containerInterface interface {
	Add(service *restful.WebService) *restful.Container
	Handle(path string, handler http.Handler)
	Filter(filter restful.FilterFunction)
	ServeHTTP(w http.ResponseWriter, r *http.Request)
	RegisteredWebServices() []*restful.WebService

	// RegisteredHandlePaths 返回直接注册到 container（非 web-service）的 handler 路径。
	// 用于测试 filter 是否也应用在非 web-service 的 handler 上。
	RegisteredHandlePaths() []string
}

// filteringContainer 把所有 Handle(...) 调用委托给 Container.HandleWithFilter(...)，
// 从而确保所有 handler 都经过 restful.FilterFunctions 处理。
type filteringContainer struct {
	*restful.Container

	registeredHandlePaths []string
}

// Handle 注册 handler 并记录路径，便于 /statusz 列出所有已注册端点。
func (a *filteringContainer) Handle(path string, handler http.Handler) {
	a.HandleWithFilter(path, handler)
	a.registeredHandlePaths = append(a.registeredHandlePaths, path)
}

// RegisteredHandlePaths 返回通过 Handle 注册的路径列表。
func (a *filteringContainer) RegisteredHandlePaths() []string {
	return a.registeredHandlePaths
}

// ListenAndServeKubeletServer 初始化一个响应 Kubelet HTTP 网络请求的服务端。
func ListenAndServeKubeletServer(
	ctx context.Context,
	host HostInterface,
	resourceAnalyzer stats.ResourceAnalyzer,
	checkers []healthz.HealthChecker,
	flagz flagz.Reader,
	kubeCfg *kubeletconfiginternal.KubeletConfiguration,
	tlsConfig *tls.Config,
	auth AuthInterface,
	tp oteltrace.TracerProvider) {

	address := netutils.ParseIPSloppy(kubeCfg.Address)
	port := uint(kubeCfg.Port)
	logger := klog.FromContext(ctx)
	logger.Info("Starting to listen", "address", address, "port", port)
	handler := NewServer(ctx, host, resourceAnalyzer, checkers, flagz, auth, kubeCfg)
	handler.InstallTracingFilter(tp)

	// IdleTimeout 与 http.DefaultTransport 的 keep-alive 超时保持一致。
	// 读写超时设为 4 分钟，需覆盖 exec/attach/port-forward 等长连接的建立过程。
	s := &http.Server{
		Addr:           net.JoinHostPort(address.String(), strconv.FormatUint(uint64(port), 10)),
		Handler:        &handler,
		IdleTimeout:    90 * time.Second,
		ReadTimeout:    4 * 60 * time.Minute,
		WriteTimeout:   4 * 60 * time.Minute,
		MaxHeaderBytes: 1 << 20,
	}

	if tlsConfig != nil {
		s.TLSConfig = tlsConfig
		if err := s.ListenAndServeTLS("", ""); err != nil {
			logger.Error(err, "Failed to listen and serve")
			os.Exit(1)
		}

		// 支持以纯 HTTP 方式运行的空壳节点（hollow node）。
	} else if err := s.ListenAndServe(); err != nil {
		logger.Error(err, "Failed to listen and serve")
		os.Exit(1)
	}
}

// ListenAndServeKubeletReadOnlyServer 初始化一个只读的 Kubelet HTTP 服务端。
func ListenAndServeKubeletReadOnlyServer(
	ctx context.Context,
	host HostInterface,
	resourceAnalyzer stats.ResourceAnalyzer,
	checkers []healthz.HealthChecker,
	flagz flagz.Reader,
	address net.IP,
	port uint,
	tp oteltrace.TracerProvider) {
	logger := klog.FromContext(ctx)
	logger.Info("Starting to listen read-only", "address", address, "port", port)
	s := NewServer(ctx, host, resourceAnalyzer, checkers, nil, nil, nil)
	s.InstallTracingFilter(tp, otelrestful.WithPublicEndpoint())

	// IdleTimeout 与 http.DefaultTransport 的 keep-alive 超时保持一致。
	// 读写超时设为 4 分钟，需覆盖 exec/attach/port-forward 等长连接的建立过程。
	server := &http.Server{
		Addr:           net.JoinHostPort(address.String(), strconv.FormatUint(uint64(port), 10)),
		Handler:        &s,
		IdleTimeout:    90 * time.Second,
		ReadTimeout:    4 * 60 * time.Minute,
		WriteTimeout:   4 * 60 * time.Minute,
		MaxHeaderBytes: 1 << 20,
	}

	if err := server.ListenAndServe(); err != nil {
		logger.Error(err, "Failed to listen and serve")
		os.Exit(1)
	}
}

// ListenAndServePodResources 初始化一个 gRPC 服务端来提供 PodResources 服务。
func ListenAndServePodResources(ctx context.Context, endpoint string, providers podresources.PodResourcesProviders) {
	logger := klog.FromContext(ctx)
	// 通过限流器限制 QPS，避免 podresources 查询拖垮 kubelet。
	server := grpc.NewServer(apisgrpc.WithRateLimiter(ctx, "podresources", podresources.DefaultQPS, podresources.DefaultBurstTokens))

	podresourcesapiv1alpha1.RegisterPodResourcesListerServer(server, podresources.NewV1alpha1PodResourcesServer(providers))
	podresourcesapi.RegisterPodResourcesListerServer(server, podresources.NewV1PodResourcesServer(ctx, providers))

	l, err := util.CreateListener(endpoint)
	if err != nil {
		logger.Error(err, "Failed to create listener for podResources endpoint")
		os.Exit(1)
	}

	logger.Info("Starting to serve the podresources API", "endpoint", endpoint)
	if err := server.Serve(l); err != nil {
		logger.Error(err, "Failed to serve")
		os.Exit(1)
	}
}

// ListenAndServePodsServer 初始化一个 HTTP 服务端来提供 Pod API。
func ListenAndServePodsServer(ctx context.Context, endpoint string, srv podsv1alpha1.PodsServer) {
	logger := klog.FromContext(ctx)
	// 同时挂载限流与指标拦截器，分别用于保护 kubelet 和观测调用。
	server := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			apisgrpc.LimiterUnaryServerInterceptor(rate.NewLimiter(rate.Limit(pods.DefaultQPS), int(pods.DefaultBurstTokens))),
			pods.MetricsUnaryServerInterceptor,
		),
		grpc.StreamInterceptor(pods.MetricsStreamServerInterceptor),
	)

	podsv1alpha1.RegisterPodsServer(server, srv)

	l, err := util.CreateListener(endpoint)
	if err != nil {
		logger.Error(err, "Failed to create listener for pods API endpoint")
		os.Exit(1)
	}

	logger.Info("Starting to serve the pods API", "endpoint", endpoint)
	go func() {
		if err := server.Serve(l); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			logger.Error(err, "Failed to serve")
			os.Exit(1)
		}
	}()

	// 等待 ctx 取消后优雅关闭，确保在途请求完成。
	<-ctx.Done()
	logger.Info("Shutting down pods API server")
	server.GracefulStop()
}

// NodeRequestAttributesGetter 根据请求生成用于鉴权的属性列表。
type NodeRequestAttributesGetter interface {
	GetRequestAttributes(ctx context.Context, u user.Info, r *http.Request) []authorizer.Attributes
}

// AuthInterface 包含 auth filter 所需的全部方法。
type AuthInterface interface {
	authenticator.Request
	NodeRequestAttributesGetter
	authorizer.UnconditionalAuthorizer
	dynamiccertificates.CAContentProvider
}

// HostInterface 包含 server 所需的全部 kubelet 方法，便于测试替换。
type HostInterface interface {
	stats.Provider
	GetVersionInfo() (*cadvisorapi.VersionInfo, error)
	GetCachedMachineInfo() (*cadvisorapi.MachineInfo, error)
	GetRunningPods(ctx context.Context) ([]*v1.Pod, error)
	RunInContainer(ctx context.Context, name string, uid types.UID, container string, cmd []string) ([]byte, error)
	CheckpointContainer(ctx context.Context, podUID types.UID, podFullName, containerName string, options *runtimeapi.CheckpointContainerRequest) error
	GetKubeletContainerLogs(ctx context.Context, podFullName, containerName string, logOptions *v1.PodLogOptions, stdout, stderr io.Writer) error
	ServeLogs(w http.ResponseWriter, req *http.Request)
	SyncLoopHealthCheck(req *http.Request) error
	GetExec(ctx context.Context, podFullName string, podUID types.UID, containerName string, cmd []string, streamOpts remotecommandserver.Options) (*url.URL, error)
	GetAttach(ctx context.Context, podFullName string, podUID types.UID, containerName string, streamOpts remotecommandserver.Options) (*url.URL, error)
	GetPortForward(ctx context.Context, podName, podNamespace string, podUID types.UID, portForwardOpts portforward.V4Options) (*url.URL, error)
	ListMetricDescriptors(ctx context.Context) ([]*runtimeapi.MetricDescriptor, error)
	ListPodSandboxMetrics(ctx context.Context) ([]*runtimeapi.PodSandboxMetrics, error)
	GetAllocatedPods() ([]*v1.Pod, error)
	GetAllocatedPodByName(namespace, name string) (*v1.Pod, error)
}

// NewServer 初始化并配置一个 kubelet.Server 对象来处理 HTTP 请求。
func NewServer(
	ctx context.Context,
	host HostInterface,
	resourceAnalyzer stats.ResourceAnalyzer,
	checkers []healthz.HealthChecker,
	flagz flagz.Reader,
	auth AuthInterface,
	kubeCfg *kubeletconfiginternal.KubeletConfiguration) Server {

	logger := klog.FromContext(ctx)
	server := Server{
		flagz:                flagz,
		host:                 host,
		resourceAnalyzer:     resourceAnalyzer,
		auth:                 auth,
		restfulCont:          &filteringContainer{Container: restful.NewContainer()},
		metricsBuckets:       sets.New[string](),
		metricsMethodBuckets: sets.New[string]("OPTIONS", "GET", "HEAD", "POST", "PUT", "DELETE", "TRACE", "CONNECT"),
		extendedCheckers:     checkers,
	}
	if auth != nil {
		server.InstallAuthFilter(ctx)
	}
	server.InstallAuthNotRequiredHandlers(ctx)
	if kubeCfg != nil && kubeCfg.EnableDebuggingHandlers {
		logger.Info("Adding debug handlers to kubelet server")
		// 为保持向后兼容，仅当 enableDebuggingHandlers 也启用时才提供 logs 与 pprof，
		// 参见 https://github.com/kubernetes/kubernetes/pull/87273。
		server.InstallSystemLogHandler(kubeCfg.EnableSystemLogHandler, kubeCfg.EnableSystemLogQuery)
		server.InstallProfilingHandler(kubeCfg.EnableProfilingHandler, kubeCfg.EnableContentionProfiling)
		server.InstallDebugFlagsHandler(kubeCfg.EnableDebugFlagsHandler)
		// 必须最后执行，确保所有路径都被纳入 /statusz。
		server.InstallAuthRequiredHandlers(ctx)
	} else {
		server.InstallDebuggingDisabledHandlers()
	}

	return server
}

// InstallAuthFilter 在 restful Container 上安装认证与授权 filter。
func (s *Server) InstallAuthFilter(ctx context.Context) {
	logger := klog.FromContext(ctx)
	s.restfulCont.Filter(func(req *restful.Request, resp *restful.Response, chain *restful.FilterChain) {
		// 认证。
		info, ok, err := s.auth.AuthenticateRequest(req.Request)
		if err != nil {
			logger.Error(err, "Unable to authenticate the request due to an error")
			resp.WriteErrorString(http.StatusUnauthorized, "Unauthorized")
			return
		}
		if !ok {
			resp.WriteErrorString(http.StatusUnauthorized, "Unauthorized")
			return
		}

		// 获取鉴权属性。
		attrs := s.auth.GetRequestAttributes(ctx, info.User, req.Request)
		var allowed bool
		var msg string
		var subresources []string
		for _, attr := range attrs {
			subresources = append(subresources, attr.GetSubresource())
			decision, _, err := s.auth.Authorize(req.Request.Context(), attr)
			if err != nil {
				logger.Error(err, "Authorization error", "user", attr.GetUser().GetName(), "verb", attr.GetVerb(), "resource", attr.GetResource(), "subresource", attr.GetSubresource())
				msg = fmt.Sprintf("Authorization error (user=%s, verb=%s, resource=%s, subresource=%s)", attr.GetUser().GetName(), attr.GetVerb(), attr.GetResource(), attr.GetSubresource())
				resp.WriteErrorString(http.StatusInternalServerError, msg)
				return

			}

			// 多个属性是“或”关系：任一允许即可放行。
			if decision == authorizer.DecisionAllow {
				allowed = true
				break
			}
		}

		if !allowed {
			if len(attrs) == 0 {
				logger.Error(fmt.Errorf("could not determine attributes for request"), "Authorization error")
				resp.WriteErrorString(http.StatusForbidden, "Authorization error: could not determine attributes for request")
				return
			}
			// 各属性仅在 subresource 上有差异，因此取第一个即可用于日志。
			logger.V(2).Info("Forbidden", "user", attrs[0].GetUser().GetName(), "verb", attrs[0].GetVerb(), "resource", attrs[0].GetResource(), "subresource(s)", subresources)
			resp.WriteErrorString(http.StatusForbidden, fmt.Sprintf("Forbidden (user=%s, verb=%s, resource=%s, subresource(s)=%v)\n", attrs[0].GetUser().GetName(), attrs[0].GetVerb(), attrs[0].GetResource(), subresources))
			return
		}

		// 继续后续处理。
		chain.ProcessFilter(req, resp)
	})
}

// InstallTracingFilter 在 restful Container 上安装 OpenTelemetry 链路追踪 filter。
func (s *Server) InstallTracingFilter(tp oteltrace.TracerProvider, opts ...otelrestful.Option) {
	s.restfulCont.Filter(otelrestful.OTelFilter("kubelet", append(opts, otelrestful.WithTracerProvider(tp))...))
}

// InstallZPages 注册 z-pages 端点（flagz/statusz），受对应特性门控控制。
func (s *Server) InstallZPages() {
	if utilfeature.DefaultFeatureGate.Enabled(zpagesfeatures.ComponentFlagz) {
		if s.flagz != nil {
			s.addMetricsBucketMatcher("flagz")
			flagz.Install(s.restfulCont, ComponentKubelet, s.flagz)
		}
	}
	// 必须最后执行，确保所有路径都被纳入 /statusz。
	if utilfeature.DefaultFeatureGate.Enabled(zpagesfeatures.ComponentStatusz) {
		s.addMetricsBucketMatcher("statusz")
		statusz.Install(s.restfulCont, ComponentKubelet, statusz.NewRegistry(compatibility.DefaultBuildEffectiveVersion(), statusz.WithListedPaths(s.restfulCont.RegisteredHandlePaths())))
	}
}

// addMetricsBucketMatcher 添加一个 metrics 桶名称，命中时使用对应桶上报。
// 注意：该方法不是线程安全的，不应在运行时动态调用。
func (s *Server) addMetricsBucketMatcher(bucket string) {
	s.metricsBuckets.Insert(bucket)
}

// getMetricBucket 为给定路径找到合适的 metrics 上报桶。
func (s *Server) getMetricBucket(path string) string {
	root := getURLRootPath(path)
	if s.metricsBuckets.Has(root) {
		return root
	}
	return "other"
}

// getMetricMethodBucket 检查是否为未知或非法的 HTTP 方法。
func (s *Server) getMetricMethodBucket(method string) string {
	if s.metricsMethodBuckets.Has(method) {
		return method
	}
	return "other"
}

// InstallAuthNotRequiredHandlers 注册无需授权的请求 handler，这些 handler 会同时
// 安装在不安全与安全（TLS）服务端上。
// NOTE: 该方法仅为向后兼容而保留，不应再向此集合添加新端点。
// 新 handler 应添加到 InstallAuthorizedHandlers 下。
func (s *Server) InstallAuthNotRequiredHandlers(ctx context.Context) {
	logger := klog.FromContext(ctx)
	s.addMetricsBucketMatcher("healthz")
	checkers := []healthz.HealthChecker{
		healthz.PingHealthz,
		healthz.LogHealthz,
		healthz.NamedCheck("syncloop", s.host.SyncLoopHealthCheck),
	}
	checkers = append(checkers, s.extendedCheckers...)
	healthz.InstallHandler(s.restfulCont, checkers...)

	slis.SLIMetricsWithReset{}.Install(s.restfulCont)

	// /pods 返回 kubelet 绑定的 Pod 列表及其 spec。
	s.addMetricsBucketMatcher("pods")
	ws := new(restful.WebService)
	ws.Filter(GETOnlyRestfulFilter())
	ws.
		Path(podsPath).
		Produces(restful.MIME_JSON)
	ws.Route(ws.GET("").
		To(func(r1 *restful.Request, r2 *restful.Response) {
			s.getPods(logger, r1, r2)
		}).
		Operation("getPods"))
	s.restfulCont.Add(ws)

	s.addMetricsBucketMatcher("stats")
	s.restfulCont.Add(stats.CreateHandlers(statsPath, s.host, s.resourceAnalyzer))

	s.addMetricsBucketMatcher("metrics")
	s.addMetricsBucketMatcher("metrics/cadvisor")
	s.addMetricsBucketMatcher("metrics/probes")
	s.addMetricsBucketMatcher("metrics/resource")
	s.restfulCont.Handle(metricsPath, legacyregistry.Handler())

	includedMetrics := cadvisormetrics.MetricSet{
		cadvisormetrics.CpuUsageMetrics:     struct{}{},
		cadvisormetrics.MemoryUsageMetrics:  struct{}{},
		cadvisormetrics.DiskIOMetrics:       struct{}{},
		cadvisormetrics.DiskUsageMetrics:    struct{}{},
		cadvisormetrics.NetworkUsageMetrics: struct{}{},
		cadvisormetrics.AppMetrics:          struct{}{},
		cadvisormetrics.ProcessMetrics:      struct{}{},
		cadvisormetrics.OOMMetrics:          struct{}{},
	}

	if utilfeature.DefaultFeatureGate.Enabled(features.KubeletPSI) {
		if kubeletcadvisor.IsPsiEnabled(logger) {
			includedMetrics[cadvisormetrics.PressureMetrics] = struct{}{}
		}
	}

	// cAdvisor 指标同时通过安全 handler 暴露。
	r := compbasemetrics.NewKubeRegistry()
	r.RawMustRegister(metrics.NewPrometheusMachineCollector(prometheusHostAdapter{s.host}, includedMetrics))
	if utilfeature.DefaultFeatureGate.Enabled(features.PodAndContainerStatsFromCRI) {
		// 从 CRI 获取容器统计，避免依赖 cAdvisor 的容器采集。
		r.CustomRegister(collectors.NewCRIMetricsCollector(context.TODO(), s.host.ListPodSandboxMetrics, s.host.ListMetricDescriptors))
		servermetrics.SetMetricsProvider(servermetrics.CRIMetricsProvider)
	} else {
		cadvisorOpts := cadvisorapi.RequestOptions{
			IdType:    cadvisorapi.TypeName,
			Count:     1,
			Recursive: true,
		}
		r.RawMustRegister(metrics.NewPrometheusCollector(oomEventsHostAdapter{prometheusHostAdapter{s.host}}, containerPrometheusLabelsFunc(s.host), includedMetrics, clock.RealClock{}, cadvisorOpts))
		servermetrics.SetMetricsProvider(servermetrics.CAdvisorMetricsProvider)
	}
	s.restfulCont.Handle(cadvisorMetricsPath,
		compbasemetrics.HandlerFor(r, compbasemetrics.HandlerOpts{ErrorHandling: compbasemetrics.ContinueOnError}),
	)

	s.addMetricsBucketMatcher("metrics/resource")
	resourceRegistry := compbasemetrics.NewKubeRegistry()
	resourceRegistry.CustomMustRegister(collectors.NewResourceMetricsCollector(s.resourceAnalyzer))
	s.restfulCont.Handle(resourceMetricsPath,
		compbasemetrics.HandlerFor(resourceRegistry, compbasemetrics.HandlerOpts{ErrorHandling: compbasemetrics.ContinueOnError}),
	)

	// prober 指标通过独立端点暴露。

	s.addMetricsBucketMatcher("metrics/probes")
	p := compbasemetrics.NewKubeRegistry()
	_ = compbasemetrics.RegisterProcessStartTime(p.Register)
	p.MustRegister(prober.ProberResults)
	p.MustRegister(prober.ProberDuration)
	s.restfulCont.Handle(proberMetricsPath,
		compbasemetrics.HandlerFor(p, compbasemetrics.HandlerOpts{ErrorHandling: compbasemetrics.ContinueOnError}),
	)

	// 不要在此处添加新 handler！
	// 参见方法注释中的说明。
}

// InstallAuthRequiredHandlers 注册仅在启用授权时安装的 HTTP handler。
// NOTE: 新端点必须要求授权。
func (s *Server) InstallAuthRequiredHandlers(ctx context.Context) {
	logger := klog.FromContext(ctx)
	s.addMetricsBucketMatcher("run")
	ws := new(restful.WebService)
	getRun := func(request *restful.Request, response *restful.Response) {
		s.getRun(logger, request, response)
	}
	ws.
		Path("/run")
	ws.Route(ws.POST("/{podNamespace}/{podID}/{containerName}").
		To(getRun).
		Operation("getRun"))
	ws.Route(ws.POST("/{podNamespace}/{podID}/{uid}/{containerName}").
		To(getRun).
		Operation("getRun"))
	s.restfulCont.Add(ws)

	s.addMetricsBucketMatcher("exec")
	ws = new(restful.WebService)
	ws.
		Path("/exec")
	ws.Route(ws.GET("/{podNamespace}/{podID}/{containerName}").
		To(s.getExec).
		Operation("getExec"))
	ws.Route(ws.POST("/{podNamespace}/{podID}/{containerName}").
		To(s.getExec).
		Operation("getExec"))
	ws.Route(ws.GET("/{podNamespace}/{podID}/{uid}/{containerName}").
		To(s.getExec).
		Operation("getExec"))
	ws.Route(ws.POST("/{podNamespace}/{podID}/{uid}/{containerName}").
		To(s.getExec).
		Operation("getExec"))
	s.restfulCont.Add(ws)

	s.addMetricsBucketMatcher("attach")
	ws = new(restful.WebService)
	ws.
		Path("/attach")
	ws.Route(ws.GET("/{podNamespace}/{podID}/{containerName}").
		To(s.getAttach).
		Operation("getAttach"))
	ws.Route(ws.POST("/{podNamespace}/{podID}/{containerName}").
		To(s.getAttach).
		Operation("getAttach"))
	ws.Route(ws.GET("/{podNamespace}/{podID}/{uid}/{containerName}").
		To(s.getAttach).
		Operation("getAttach"))
	ws.Route(ws.POST("/{podNamespace}/{podID}/{uid}/{containerName}").
		To(s.getAttach).
		Operation("getAttach"))
	s.restfulCont.Add(ws)

	s.addMetricsBucketMatcher("portForward")
	ws = new(restful.WebService)
	ws.
		Path("/portForward")
	ws.Route(ws.GET("/{podNamespace}/{podID}").
		To(s.getPortForward).
		Operation("getPortForward"))
	ws.Route(ws.POST("/{podNamespace}/{podID}").
		To(s.getPortForward).
		Operation("getPortForward"))
	ws.Route(ws.GET("/{podNamespace}/{podID}/{uid}").
		To(s.getPortForward).
		Operation("getPortForward"))
	ws.Route(ws.POST("/{podNamespace}/{podID}/{uid}").
		To(s.getPortForward).
		Operation("getPortForward"))
	s.restfulCont.Add(ws)

	s.addMetricsBucketMatcher("containerLogs")
	ws = new(restful.WebService)
	ws.Filter(GETOnlyRestfulFilter())
	ws.
		Path("/containerLogs")
	ws.Route(ws.GET("/{podNamespace}/{podID}/{containerName}").
		To(s.getContainerLogs).
		Operation("getContainerLogs"))
	s.restfulCont.Add(ws)

	s.addMetricsBucketMatcher("configz")
	configz.InstallHandler(s.restfulCont)

	if utilfeature.DefaultFeatureGate.Enabled(features.KubeletAllocatedPodsEndpoint) {
		s.addMetricsBucketMatcher("allocatedPods")
		ws = new(restful.WebService)
		ws.Filter(GETOnlyRestfulFilter())
		ws.
			Path(allocatedPodsPath).
			Produces(restful.MIME_JSON)
		ws.Route(ws.GET("").
			To(s.getAllocatedPods).
			Operation("getAllocatedPods"))
		ws.Route(ws.GET("/{podNamespace}/{podID}").
			To(s.getAllocatedPod).
			Operation("getAllocatedPod"))
		ws.Route(ws.GET("/{podNamespace}/{podID}/{podUID}").
			To(s.getAllocatedPod).
			Operation("getAllocatedPod"))
		s.restfulCont.Add(ws)
	}

	// /runningpods 端点仅用于测试。
	s.addMetricsBucketMatcher("runningpods")
	ws = new(restful.WebService)
	ws.Filter(GETOnlyRestfulFilter())
	ws.
		Path(runningPodsPath).
		Produces(restful.MIME_JSON)
	ws.Route(ws.GET("").
		To(s.getRunningPods).
		Operation("getRunningPods"))
	s.restfulCont.Add(ws)

	// 仅在特性门控启用时才开启 checkpoint API。
	if utilfeature.DefaultFeatureGate.Enabled(features.ContainerCheckpoint) {
		s.addMetricsBucketMatcher("checkpoint")
		ws = &restful.WebService{}
		ws.Path(checkpointPath).Produces(restful.MIME_JSON)
		ws.Route(ws.POST("/{podNamespace}/{podID}/{containerName}").
			To(s.checkpoint).
			Operation("checkpoint"))
		s.restfulCont.Add(ws)
	}

	// 必须最后执行，确保所有路径都被纳入 /statusz。
	s.InstallZPages()
}

// InstallDebuggingDisabledHandlers 为调试端点注册返回更明确错误信息的 handler。
func (s *Server) InstallDebuggingDisabledHandlers() {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "Debug endpoints are disabled.", http.StatusMethodNotAllowed)
	})

	s.addMetricsBucketMatcher("run")
	s.addMetricsBucketMatcher("exec")
	s.addMetricsBucketMatcher("attach")
	s.addMetricsBucketMatcher("portForward")
	s.addMetricsBucketMatcher("containerLogs")
	s.addMetricsBucketMatcher("runningpods")
	s.addMetricsBucketMatcher("pprof")
	s.addMetricsBucketMatcher("logs")
	paths := []string{
		"/run/", "/exec/", "/attach/", "/portForward/", "/containerLogs/",
		runningPodsPath, pprofBasePath, logsPath}
	for _, p := range paths {
		s.restfulCont.Handle(p, h)
	}
}

// InstallSystemLogHandler 注册日志端点的 HTTP 请求模式。
func (s *Server) InstallSystemLogHandler(enableSystemLogHandler bool, enableSystemLogQuery bool) {
	s.addMetricsBucketMatcher("logs")
	if enableSystemLogHandler {
		ws := new(restful.WebService)
		ws.Filter(GETOnlyRestfulFilter())
		ws.Path(logsPath)
		ws.Route(ws.GET("").
			To(s.getLogs).
			Operation("getLogs"))
		if !enableSystemLogQuery {
			ws.Route(ws.GET("/{logpath:*}").
				To(s.getLogs).
				Operation("getLogs").
				Param(ws.PathParameter("logpath", "path to the log").DataType("string")))
		} else {
			// 启用日志查询时额外接受过滤参数，由 ServeLogs 负责解析。
			ws.Route(ws.GET("/{logpath:*}").
				To(s.getLogs).
				Operation("getLogs").
				Param(ws.PathParameter("logpath", "path to the log").DataType("string")).
				Param(ws.QueryParameter("query", "query specifies services(s) or files from which to return logs").DataType("string")).
				Param(ws.QueryParameter("sinceTime", "sinceTime is an RFC3339 timestamp from which to show logs").DataType("string")).
				Param(ws.QueryParameter("untilTime", "untilTime is an RFC3339 timestamp until which to show logs").DataType("string")).
				Param(ws.QueryParameter("tailLines", "tailLines is used to retrieve the specified number of lines from the end of the log").DataType("string")).
				Param(ws.QueryParameter("pattern", "pattern filters log entries by the provided regex pattern").DataType("string")).
				Param(ws.QueryParameter("boot", "boot show messages from a specific system boot").DataType("string")))
		}
		s.restfulCont.Add(ws)
	} else {
		s.restfulCont.Handle(logsPath, getHandlerForDisabledEndpoint("logs endpoint is disabled."))
	}
}

// getHandlerForDisabledEndpoint 返回一个固定返回 405 的 handler，用于已禁用的端点。
func getHandlerForDisabledEndpoint(errorMessage string) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, errorMessage, http.StatusMethodNotAllowed)
	})
}

// InstallDebugFlagsHandler 注册 /debug/flags/v 端点的 HTTP 请求模式。
func (s *Server) InstallDebugFlagsHandler(enableDebugFlagsHandler bool) {
	if enableDebugFlagsHandler {
		// 设置 flags handler。目前只有日志相关端点被认为是适合通过该调试 flags 调整的。
		s.restfulCont.Handle(debugFlagPath, routes.StringFlagPutHandler(logs.GlogSetter))
	} else {
		s.restfulCont.Handle(debugFlagPath, getHandlerForDisabledEndpoint("flags endpoint is disabled."))
		return
	}
}

// InstallProfilingHandler 注册 /debug/pprof 端点的 HTTP 请求模式。
func (s *Server) InstallProfilingHandler(enableProfilingLogHandler bool, enableContentionProfiling bool) {
	s.addMetricsBucketMatcher("debug")
	if !enableProfilingLogHandler {
		s.restfulCont.Handle(pprofBasePath, getHandlerForDisabledEndpoint("profiling endpoint is disabled."))
		return
	}

	// 根据子路径分发到对应的 pprof handler。
	handlePprofEndpoint := func(req *restful.Request, resp *restful.Response) {
		name := strings.TrimPrefix(req.Request.URL.Path, pprofBasePath)
		switch name {
		case "profile":
			pprof.Profile(resp, req.Request)
		case "symbol":
			pprof.Symbol(resp, req.Request)
		case "cmdline":
			pprof.Cmdline(resp, req.Request)
		case "trace":
			pprof.Trace(resp, req.Request)
		default:
			pprof.Index(resp, req.Request)
		}
	}

	// 设置 pprof handlers。
	ws := new(restful.WebService).Path(pprofBasePath)
	ws.Filter(GETOnlyRestfulFilter())
	ws.Route(ws.GET("/{subpath:*}").To(handlePprofEndpoint)).Doc("pprof endpoint")
	s.restfulCont.Add(ws)

	if enableContentionProfiling {
		// 采样所有阻塞事件，否则 block profile 为空；仅在显式启用时开启以免影响性能。
		goruntime.SetBlockProfileRate(1)
	}
}

// getContainerLogs 处理针对 Kubelet 的 containerLogs 请求。
func (s *Server) getContainerLogs(request *restful.Request, response *restful.Response) {
	podNamespace := request.PathParameter("podNamespace")
	podID := request.PathParameter("podID")
	containerName := request.PathParameter("containerName")
	ctx := request.Request.Context()

	if len(podID) == 0 {
		// TODO: Why return JSON when the rest return plaintext errors?
		// TODO: Why return plaintext errors?
		response.WriteError(http.StatusBadRequest, fmt.Errorf(`{"message": "Missing podID."}`))
		return
	}
	if len(containerName) == 0 {
		// TODO: Why return JSON when the rest return plaintext errors?
		response.WriteError(http.StatusBadRequest, fmt.Errorf(`{"message": "Missing container name."}`))
		return
	}
	if len(podNamespace) == 0 {
		// TODO: Why return JSON when the rest return plaintext errors?
		response.WriteError(http.StatusBadRequest, fmt.Errorf(`{"message": "Missing podNamespace."}`))
		return
	}

	query := request.Request.URL.Query()
	// 兼容旧的 "tail" 查询参数。
	if tail := request.QueryParameter("tail"); len(tail) > 0 {
		query["tailLines"] = []string{tail}
		// "all" 等同于不设置 tail。
		if tail == "all" {
			delete(query, "tailLines")
		}
	}
	// kubelet 上的容器日志固定使用 v1 版本的 PodLogOptions。
	logOptions := &v1.PodLogOptions{}
	if err := legacyscheme.ParameterCodec.DecodeParameters(query, v1.SchemeGroupVersion, logOptions); err != nil {
		response.WriteError(http.StatusBadRequest, fmt.Errorf(`{"message": "Unable to decode query."}`))
		return
	}
	if utilfeature.DefaultFeatureGate.Enabled(features.PodLogsQuerySplitStreams) {
		// 即使有默认值，完全没有参数时 logOptions.Stream 仍可能为 nil。
		if logOptions.Stream == nil {
			// 默认为 "All"，保持向后兼容。
			logOptions.Stream = ptr.To(v1.LogStreamAll)
		}
	}
	logOptions.TypeMeta = metav1.TypeMeta{}
	if errs := validation.ValidatePodLogOptions(logOptions); len(errs) > 0 {
		response.WriteError(http.StatusUnprocessableEntity, fmt.Errorf(`{"message": "Invalid request."}`))
		return
	}

	pod, ok := s.host.GetPodByName(podNamespace, podID)
	if !ok {
		response.WriteError(http.StatusNotFound, fmt.Errorf("pod %q does not exist", podID))
		return
	}
	// 检查 containerName 是否有效。
	if kubecontainer.GetContainerSpec(pod, containerName) == nil {
		response.WriteError(http.StatusNotFound, fmt.Errorf("container %q not found in pod %q", containerName, podID))
		return
	}

	// 日志需要流式输出，因此底层 ResponseWriter 必须支持 Flusher。
	if _, ok := response.ResponseWriter.(http.Flusher); !ok {
		response.WriteError(http.StatusInternalServerError, fmt.Errorf("unable to convert %v into http.Flusher, cannot show logs", reflect.TypeOf(response)))
		return
	}

	var (
		stdout io.Writer
		stderr io.Writer
		fw     = flushwriter.Wrap(response.ResponseWriter)
	)
	if utilfeature.DefaultFeatureGate.Enabled(features.PodLogsQuerySplitStreams) {
		wantedStream := logOptions.Stream
		// 未指定流类型时默认为 All。
		if wantedStream == nil {
			allStream := v1.LogStreamAll
			wantedStream = &allStream
		}
		switch *wantedStream {
		case v1.LogStreamStdout:
			stdout, stderr = fw, nil
		case v1.LogStreamStderr:
			stdout, stderr = nil, fw
		case v1.LogStreamAll:
			stdout, stderr = fw, fw
		default:
			_ = response.WriteError(http.StatusBadRequest, fmt.Errorf("invalid stream type %q", *logOptions.Stream))
			return
		}
	} else {
		// 未启用分流特性时只支持合并的 All 流，显式指定 stdout/stderr 会报错。
		if logOptions.Stream != nil && *logOptions.Stream != v1.LogStreamAll {
			_ = response.WriteError(http.StatusBadRequest, fmt.Errorf("unable to return the given log stream: %q. Please enable PodLogsQuerySplitStreams feature gate in kubelet", *logOptions.Stream))
			return
		}
		stdout, stderr = fw, fw
	}

	// 使用 chunked 编码，以便日志持续流式返回而不是等待完整内容。
	response.Header().Set("Transfer-Encoding", "chunked")
	if err := s.host.GetKubeletContainerLogs(ctx, kubecontainer.GetPodFullName(pod), containerName, logOptions, stdout, stderr); err != nil {
		response.WriteError(http.StatusBadRequest, err)
		return
	}
}

// encodePods 把 pods 转换为 v1.PodList 并返回编码后的结果。
func encodePods(pods []*v1.Pod) (data []byte, err error) {
	podList := new(v1.PodList)
	for _, pod := range pods {
		podList.Items = append(podList.Items, *pod)
	}
	// TODO: this needs to be parameterized to the kubelet, not hardcoded. Depends on Kubelet
	//   as API server refactor.
	// TODO: Locked to v1, needs to be made generic
	codec := legacyscheme.Codecs.LegacyCodec(v1.SchemeGroupVersion)
	return runtime.Encode(codec, podList)
}

// encodePod 把单个 Pod 编码为 v1 格式的字节流。
func encodePod(pod *v1.Pod) (data []byte, err error) {
	codec := legacyscheme.Codecs.LegacyCodec(v1.SchemeGroupVersion)
	return runtime.Encode(codec, pod)
}

// getPods 返回绑定到 Kubelet 的 Pod 列表及其 spec。
func (s *Server) getPods(logger klog.Logger, request *restful.Request, response *restful.Response) {
	pods := s.host.GetPods()
	data, err := encodePods(pods)
	if err != nil {
		response.WriteError(http.StatusInternalServerError, err)
		return
	}
	writeJSONResponse(logger, response, data)
}

// getRunningPods 返回 Kubelet 上正在运行的 Pod 列表。该列表来自容器运行时，
// 与 getPods 返回的“期望运行的 Pod 集合”不同。
func (s *Server) getRunningPods(request *restful.Request, response *restful.Response) {
	ctx := request.Request.Context()
	logger := klog.FromContext(ctx)
	pods, err := s.host.GetRunningPods(ctx)
	if err != nil {
		response.WriteError(http.StatusInternalServerError, err)
		return
	}
	data, err := encodePods(pods)
	if err != nil {
		response.WriteError(http.StatusInternalServerError, err)
		return
	}
	writeJSONResponse(logger, response, data)
}

// getAllocatedPods 返回已分配给 kubelet 的 Pod 列表，响应中不包含状态信息。
func (s *Server) getAllocatedPods(request *restful.Request, response *restful.Response) {
	ctx := request.Request.Context()
	logger := klog.FromContext(ctx)
	pods, err := s.host.GetAllocatedPods()
	if err != nil {
		logger.Error(err, "Failed to GetAllocatedPods")
		if err := response.WriteError(http.StatusInternalServerError, err); err != nil {
			logger.Error(err, "Failed to write error response")
		}
		return
	}

	// 从已分配的 Pod 中过滤掉状态。
	copiedPods := make([]*v1.Pod, len(pods))
	for i, pod := range pods {
		// 浅拷贝后清空 Status，避免把状态数据泄露到分配接口的响应里。
		copied := *pod
		copied.Status = v1.PodStatus{}
		copiedPods[i] = &copied
	}
	data, err := encodePods(copiedPods)
	if err != nil {
		logger.Error(err, "Failed to encode allocated pods")
		if err := response.WriteError(http.StatusInternalServerError, err); err != nil {
			logger.Error(err, "Failed to write error response")
		}
		return
	}
	writeJSONResponse(logger, response, data)
}

// getAllocatedPod 按命名空间与名称返回单个已分配的 Pod，响应中不包含状态信息。
func (s *Server) getAllocatedPod(request *restful.Request, response *restful.Response) {
	ctx := request.Request.Context()
	logger := klog.FromContext(ctx)
	podNamespace := request.PathParameter("podNamespace")
	podID := request.PathParameter("podID")
	podUID := types.UID(request.PathParameter("podUID"))

	if len(podNamespace) == 0 || len(podID) == 0 {
		logger.Error(nil, "Invalid request path: missing podNamespace or podID", "path", request.Request.URL.Path)
		if err := response.WriteErrorString(http.StatusBadRequest, "podNamespace and podID are required"); err != nil {
			logger.Error(err, "Failed to write error response")
		}
		return
	}

	pod, err := s.host.GetAllocatedPodByName(podNamespace, podID)
	if err != nil {
		logger.Error(err, "Failed to get allocated pod", "podNamespace", podNamespace, "podName", podID)
		if err := response.WriteError(http.StatusInternalServerError, err); err != nil {
			logger.Error(err, "Failed to write error response")
		}
		return
	}
	if pod == nil {
		logger.V(3).Info("Allocated pod not found", "podNamespace", podNamespace, "podName", podID)
		if err := response.WriteErrorString(http.StatusNotFound, "pod not found"); err != nil {
			logger.Error(err, "Failed to write error response")
		}
		return
	}

	// 若请求指定了 UID，则必须匹配，避免返回同名重建后的新 Pod。
	if podUID != "" && pod.UID != podUID {
		logger.V(3).Info("Allocated pod UID mismatch", "podNamespace", podNamespace, "podName", podID, "expectedUID", podUID, "actualUID", pod.UID)
		if err := response.WriteErrorString(http.StatusNotFound, "pod not found"); err != nil {
			logger.Error(err, "Failed to write error response")
		}
		return
	}

	// 从已分配的 Pod 中过滤掉状态。
	// 浅拷贝后清空 Status，避免把状态数据泄露到分配接口的响应里。
	copied := *pod
	copied.Status = v1.PodStatus{}
	data, err := encodePod(&copied)
	if err != nil {
		logger.Error(err, "Failed to encode allocated pod", "pod", format.Pod(pod))
		if err := response.WriteError(http.StatusInternalServerError, err); err != nil {
			logger.Error(err, "Failed to write error response")
		}
		return
	}
	writeJSONResponse(logger, response, data)
}

// getLogs 处理针对 Kubelet 的日志请求。
func (s *Server) getLogs(request *restful.Request, response *restful.Response) {
	s.host.ServeLogs(response, request.Request)
}

// GETOnlyRestfulFilter 仅允许 GET 方法，用于注册只读 kubelet API 的 WebService。
func GETOnlyRestfulFilter() restful.FilterFunction {
	return AllowedMethodsRestfulFilter(http.MethodGet)
}

// AllowedMethodsRestfulFilter 返回一个 restful.FilterFunction，拒绝 HTTP 方法不在
// allowed 中的请求。拒绝时返回 405 Method Not Allowed，并在 Allow 头中列出允许的
// 方法（RFC 9110）。
func AllowedMethodsRestfulFilter(allowed ...string) restful.FilterFunction {
	allowedSet := make(map[string]struct{}, len(allowed))
	for _, m := range allowed {
		allowedSet[m] = struct{}{}
	}
	allowHeader := strings.Join(allowed, ", ")

	return func(req *restful.Request, resp *restful.Response, chain *restful.FilterChain) {
		if _, ok := allowedSet[req.Request.Method]; ok {
			chain.ProcessFilter(req, resp)
			return
		}
		resp.Header().Set("Allow", allowHeader)
		_ = resp.WriteErrorString(http.StatusMethodNotAllowed, "Method Not Allowed")
	}
}

// execRequestParams 保存 exec 类请求的路径参数。
type execRequestParams struct {
	podNamespace  string
	podName       string
	podUID        types.UID
	containerName string
	cmd           []string
}

// getExecRequestParams 从 restful 请求中解析 exec 参数。
func getExecRequestParams(req *restful.Request) execRequestParams {
	return execRequestParams{
		podNamespace:  req.PathParameter("podNamespace"),
		podName:       req.PathParameter("podID"),
		podUID:        types.UID(req.PathParameter("uid")),
		containerName: req.PathParameter("containerName"),
		cmd:           req.Request.URL.Query()[api.ExecCommandParam],
	}
}

// portForwardRequestParams 保存 port-forward 请求的路径参数。
type portForwardRequestParams struct {
	podNamespace string
	podName      string
	podUID       types.UID
}

// getPortForwardRequestParams 从 restful 请求中解析 port-forward 参数。
func getPortForwardRequestParams(req *restful.Request) portForwardRequestParams {
	return portForwardRequestParams{
		podNamespace: req.PathParameter("podNamespace"),
		podName:      req.PathParameter("podID"),
		podUID:       types.UID(req.PathParameter("uid")),
	}
}

// responder 实现 proxy 的错误响应接口，把代理错误转换为 500 响应。
type responder struct{}

// Error 记录代理错误并返回 500。
func (r *responder) Error(w http.ResponseWriter, req *http.Request, err error) {
	logger := klog.FromContext(req.Context())
	logger.Error(err, "Error while proxying request")
	http.Error(w, err.Error(), http.StatusInternalServerError)
}

// getAttach 处理 attach 到容器的请求。
func (s *Server) getAttach(request *restful.Request, response *restful.Response) {
	params := getExecRequestParams(request)
	streamOpts, err := remotecommandserver.NewOptions(request.Request)
	if err != nil {
		utilruntime.HandleError(err)
		response.WriteError(http.StatusBadRequest, err)
		return
	}
	pod, ok := s.host.GetPodByName(params.podNamespace, params.podName)
	if !ok {
		response.WriteError(http.StatusNotFound, fmt.Errorf("pod does not exist"))
		return
	}

	podFullName := kubecontainer.GetPodFullName(pod)
	url, err := s.host.GetAttach(request.Request.Context(), podFullName, params.podUID, params.containerName, *streamOpts)
	if err != nil {
		streaming.WriteError(err, response.ResponseWriter)
		return
	}
	var handler http.Handler = proxy.NewUpgradeAwareHandler(url, nil /*transport*/, false /*wrapTransport*/, true /*upgradeRequired*/, &responder{})
	// 若升级请求使用 V5 websocket，则包装为 websocket/spdy 流转换。
	if utilfeature.DefaultFeatureGate.Enabled(features.ExtendWebSocketsToKubelet) &&
		wsstream.IsWebSocketRequestWithStreamCloseProtocol(request.Request) {
		servermetrics.IncWebSocketStreamingRequest("attach")
		streamOptions := translator.Options{
			Stdin:  streamOpts.Stdin,
			Stdout: streamOpts.Stdout,
			Stderr: streamOpts.Stderr,
			Tty:    streamOpts.TTY,
		}
		handler = translator.NewStreamTranslatorHandler(url, nil, 0, streamOptions)
	}
	handler.ServeHTTP(response.ResponseWriter, request.Request)
}

// getExec 处理在容器内执行命令的请求。
func (s *Server) getExec(request *restful.Request, response *restful.Response) {
	params := getExecRequestParams(request)
	streamOpts, err := remotecommandserver.NewOptions(request.Request)
	if err != nil {
		utilruntime.HandleError(err)
		response.WriteError(http.StatusBadRequest, err)
		return
	}
	pod, ok := s.host.GetPodByName(params.podNamespace, params.podName)
	if !ok {
		response.WriteError(http.StatusNotFound, fmt.Errorf("pod does not exist"))
		return
	}

	podFullName := kubecontainer.GetPodFullName(pod)
	url, err := s.host.GetExec(request.Request.Context(), podFullName, params.podUID, params.containerName, params.cmd, *streamOpts)
	if err != nil {
		streaming.WriteError(err, response.ResponseWriter)
		return
	}
	var handler http.Handler = proxy.NewUpgradeAwareHandler(url, nil /*transport*/, false /*wrapTransport*/, true /*upgradeRequired*/, &responder{})
	// 若升级请求使用 V5 websocket，则包装为 websocket/spdy 流转换。
	if utilfeature.DefaultFeatureGate.Enabled(features.ExtendWebSocketsToKubelet) &&
		wsstream.IsWebSocketRequestWithStreamCloseProtocol(request.Request) {
		servermetrics.IncWebSocketStreamingRequest("exec")
		streamOptions := translator.Options{
			Stdin:  streamOpts.Stdin,
			Stdout: streamOpts.Stdout,
			Stderr: streamOpts.Stderr,
			Tty:    streamOpts.TTY,
		}
		handler = translator.NewStreamTranslatorHandler(url, nil, 0, streamOptions)
	}
	handler.ServeHTTP(response.ResponseWriter, request.Request)
}

// getRun 处理在容器内运行命令的请求。
func (s *Server) getRun(logger klog.Logger, request *restful.Request, response *restful.Response) {
	params := getExecRequestParams(request)
	pod, ok := s.host.GetPodByName(params.podNamespace, params.podName)
	if !ok {
		response.WriteError(http.StatusNotFound, fmt.Errorf("pod does not exist"))
		return
	}

	// 出于历史原因，run 使用的查询参数与 exec 不同。
	params.cmd = strings.Split(request.QueryParameter("cmd"), " ")
	data, err := s.host.RunInContainer(request.Request.Context(), kubecontainer.GetPodFullName(pod), params.podUID, params.containerName, params.cmd)
	if err != nil {
		response.WriteError(http.StatusInternalServerError, err)
		return
	}
	writeJSONResponse(logger, response, data)
}

// writeJSONResponse 派生自 go-restful 的 writeJSON：data 为 nil 时只写状态码，
// 不写入 "null" 表示，避免污染响应体。
func writeJSONResponse(logger klog.Logger, response *restful.Response, data []byte) {
	if data == nil {
		response.WriteHeader(http.StatusOK)
		// 不写入 nil 表示。
		return
	}
	response.Header().Set(restful.HEADER_ContentType, restful.MIME_JSON)
	response.WriteHeader(http.StatusOK)
	if _, err := response.Write(data); err != nil {
		logger.Error(err, "Error writing response")
	}
}

// getPortForward 处理新的 restful port-forward 请求：解析 Pod 名称与 uid，
// 然后调用 ServePortForward。
func (s *Server) getPortForward(request *restful.Request, response *restful.Response) {
	params := getPortForwardRequestParams(request)

	// 隧道模式下请求走 websocket，无法按 SPDY 的头部解析端口，因此使用空的 V4Options。
	websocketSPDYTunnel := utilfeature.DefaultFeatureGate.Enabled(features.ExtendWebSocketsToKubelet) &&
		wsstream.IsWebSocketRequestWithTunnelingProtocol(request.Request)
	var portForwardOptions *portforward.V4Options
	var err error
	if websocketSPDYTunnel {
		portForwardOptions = &portforward.V4Options{}
	} else {
		portForwardOptions, err = portforward.NewV4Options(request.Request)
		if err != nil {
			utilruntime.HandleError(err)
			response.WriteError(http.StatusBadRequest, err) //nolint:errcheck
			return
		}
	}

	pod, ok := s.host.GetPodByName(params.podNamespace, params.podName)
	if !ok {
		response.WriteError(http.StatusNotFound, fmt.Errorf("pod does not exist"))
		return
	}
	// 指定了 uid 时必须与当前 Pod 匹配，防止转发到同名重建后的新 Pod。
	if len(params.podUID) > 0 && pod.UID != params.podUID {
		response.WriteError(http.StatusNotFound, fmt.Errorf("pod not found"))
		return
	}

	url, err := s.host.GetPortForward(request.Request.Context(), pod.Name, pod.Namespace, pod.UID, *portForwardOptions)
	if err != nil {
		streaming.WriteError(err, response.ResponseWriter)
		return
	}
	var handler http.Handler = proxy.NewUpgradeAwareHandler(url, nil /*transport*/, false /*wrapTransport*/, true /*upgradeRequired*/, &responder{})
	// 若升级请求使用隧道式 websocket，则包装为隧道处理。
	if websocketSPDYTunnel {
		servermetrics.IncWebSocketStreamingRequest("portforward")
		handler = translator.NewTunnelingHandler(handler)
	}
	handler.ServeHTTP(response.ResponseWriter, request.Request)
}

// checkpoint 处理 checkpoint API 请求。它先确认请求的 podNamespace、Pod 与容器
// 确实存在，然后才调用运行时执行真正的容器 checkpoint。
func (s *Server) checkpoint(request *restful.Request, response *restful.Response) {
	ctx := request.Request.Context()
	logger := klog.FromContext(ctx)
	pod, ok := s.host.GetPodByName(request.PathParameter("podNamespace"), request.PathParameter("podID"))
	if !ok {
		response.WriteError(http.StatusNotFound, fmt.Errorf("pod does not exist"))
		return
	}

	containerName := request.PathParameter("containerName")

	found := false
	for _, container := range pod.Spec.Containers {
		if container.Name == containerName {
			found = true
			break
		}
	}
	if !found {
		for _, container := range pod.Spec.InitContainers {
			if container.Name == containerName {
				found = true
				break
			}
		}
	}
	if !found {
		for _, container := range pod.Spec.EphemeralContainers {
			if container.Name == containerName {
				found = true
				break
			}
		}
	}
	if !found {
		response.WriteError(
			http.StatusNotFound,
			fmt.Errorf("container %v does not exist", containerName),
		)
		return
	}

	options := &runtimeapi.CheckpointContainerRequest{}
	// 可选查询参数用于指定超时；不提供时 checkpoint 命令使用 CRI 的默认超时。
	timeouts := request.Request.URL.Query()["timeout"]
	if len(timeouts) > 0 {
		// 若用户提供多个超时值，使用最后一个可用值。
		timeout, err := strconv.ParseInt(timeouts[len(timeouts)-1], 10, 64)
		if err != nil {
			response.WriteError(
				http.StatusNotFound,
				fmt.Errorf("cannot parse value of timeout parameter"),
			)
			return
		}
		options.Timeout = timeout
	}

	if err := s.host.CheckpointContainer(ctx, pod.UID, kubecontainer.GetPodFullName(pod), containerName, options); err != nil {
		response.WriteError(
			http.StatusInternalServerError,
			fmt.Errorf(
				"checkpointing of %v/%v/%v failed (%v)",
				request.PathParameter("podNamespace"),
				request.PathParameter("podID"),
				containerName,
				err,
			),
		)
		return
	}
	writeJSONResponse(
		logger,
		response,
		[]byte(fmt.Sprintf("{\"items\":[\"%s\"]}", options.Location)),
	)
}

// getURLRootPath 截取 URL 路径的根部分。
// 对于 "/metrics/xxx" 形式的路径，返回 "metrics/xxx"；
// 其余路径返回第一段。
func getURLRootPath(path string) string {
	parts := strings.SplitN(strings.TrimPrefix(path, "/"), "/", 3)
	if len(parts) == 0 {
		return path
	}

	if parts[0] == "metrics" && len(parts) > 1 {
		return fmt.Sprintf("%s/%s", parts[0], parts[1])

	}
	return parts[0]
}

// longRunningRequestPathMap 列出长连接端点，用于在指标中区分请求类型。
var longRunningRequestPathMap = map[string]bool{
	"exec":        true,
	"attach":      true,
	"portforward": true,
	"debug":       true,
}

// isLongRunningRequest 判断请求是否为长连接请求。
func isLongRunningRequest(path string) bool {
	_, ok := longRunningRequestPathMap[path]
	return ok
}

// statusesNoTracePred 定义哪些响应状态不记录 trace 日志，避免把正常返回刷屏。
var statusesNoTracePred = httplog.StatusIsNot(
	http.StatusOK,
	http.StatusFound,
	http.StatusMovedPermanently,
	http.StatusTemporaryRedirect,
	http.StatusBadRequest,
	http.StatusNotFound,
	http.StatusSwitchingProtocols,
)

// ServeHTTP 响应发往 Kubelet 的 HTTP 请求。
func (s *Server) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	handler := httplog.WithLogging(s.restfulCont, statusesNoTracePred)

	// 监控 http 请求；auth 为 nil 说明是只读服务端。
	var serverType string
	if s.auth == nil {
		serverType = "readonly"
	} else {
		serverType = "readwrite"
	}

	method, path := s.getMetricMethodBucket(req.Method), s.getMetricBucket(req.URL.Path)

	longRunning := strconv.FormatBool(isLongRunningRequest(path))

	servermetrics.HTTPRequests.WithLabelValues(method, path, serverType, longRunning).Inc()

	servermetrics.HTTPInflightRequests.WithLabelValues(method, path, serverType, longRunning).Inc()
	defer servermetrics.HTTPInflightRequests.WithLabelValues(method, path, serverType, longRunning).Dec()

	// 使用 ObserveSince 确保时长在 defer 执行时计算，而非在声明时。
	// Go 中 defer 的函数参数会立即求值，直接传入 time.Since 会记录接近零的值
	// （约 2 微秒）而不是真实的请求处理耗时。
	defer servermetrics.HTTPRequestsDuration.ObserveSince(time.Now(), method, path, serverType, longRunning)()

	handler.ServeHTTP(w, req)
}

// prometheusHostAdapter 把 HostInterface 适配为 cAdvisor prometheus collector
// 所需的接口。
type prometheusHostAdapter struct {
	host HostInterface
}

// GetRequestedContainersInfo 转发到 HostInterface 获取容器信息。
func (a prometheusHostAdapter) GetRequestedContainersInfo(containerName string, options cadvisorapi.RequestOptions) (map[string]*cadvisorapi.ContainerInfo, error) {
	return a.host.GetRequestedContainersInfo(containerName, options)
}

// GetVersionInfo 转发到 HostInterface 获取版本信息。
func (a prometheusHostAdapter) GetVersionInfo() (*cadvisorapi.VersionInfo, error) {
	return a.host.GetVersionInfo()
}

// GetMachineInfo 转发到 HostInterface 获取机器信息。
func (a prometheusHostAdapter) GetMachineInfo() (*cadvisorapi.MachineInfo, error) {
	return a.host.GetCachedMachineInfo()
}

// oomEventsHostAdapter 包装 prometheusHostAdapter，把每个容器的 OOM 事件计数替换为
// kubelet OOM watcher（pkg/kubelet/oom）维护的计数，使 container_oom_events_total
// 由 kubelet 而非 cadvisor 生成。
type oomEventsHostAdapter struct {
	prometheusHostAdapter
}

// GetRequestedContainersInfo 覆盖容器统计中的 OOM 事件计数。
func (a oomEventsHostAdapter) GetRequestedContainersInfo(containerName string, options cadvisorapi.RequestOptions) (map[string]*cadvisorapi.ContainerInfo, error) {
	infos, err := a.prometheusHostAdapter.GetRequestedContainersInfo(containerName, options)
	for _, info := range infos {
		count := oomwatcher.OOMEventsForContainer(info.Name)
		for _, stats := range info.Stats {
			stats.OOMEvents = count
		}
	}
	return infos, err
}

// containerPrometheusLabelsFunc 返回一个把 cAdvisor 标签映射为 prometheus 标签的函数。
func containerPrometheusLabelsFunc(s stats.Provider) metrics.ContainerLabelsFunc {
	// containerPrometheusLabels 把 cAdvisor 标签映射为 prometheus 标签。
	return func(c *cadvisorapi.ContainerInfo) map[string]string {
		// Prometheus 要求同一指标族的所有指标拥有相同标签，因此缺失标签用空字符串补齐。
		var name, image, podName, namespace, containerName string
		if len(c.Aliases) > 0 {
			name = c.Aliases[0]
		}
		image = c.Spec.Image
		if v, ok := c.Spec.Labels[kubelettypes.KubernetesPodNameLabel]; ok {
			podName = v
		}
		if v, ok := c.Spec.Labels[kubelettypes.KubernetesPodNamespaceLabel]; ok {
			namespace = v
		}
		if v, ok := c.Spec.Labels[kubelettypes.KubernetesContainerNameLabel]; ok {
			containerName = v
		}
		// 把 Pod cgroup 关联到 Pod，以便准确统计 sandbox 资源。
		if podName == "" && namespace == "" {
			if pod, found := s.GetPodByCgroupfs(c.Name); found {
				podName = pod.Name
				namespace = pod.Namespace
			}
		}
		set := map[string]string{
			metrics.LabelID:    c.Name,
			metrics.LabelName:  name,
			metrics.LabelImage: image,
			"pod":              podName,
			"namespace":        namespace,
			"container":        containerName,
		}
		return set
	}
}
