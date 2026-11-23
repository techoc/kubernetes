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

// Package server implements a Server object for running the admission controller as a webhook server.
//
// 本包只负责把 admission 逻辑挂到 HTTP 上，策略判断本身由 admission 包完成。
package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"

	admissionv1 "k8s.io/api/admission/v1"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	apiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/server/healthz"
	kubeinformers "k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	compbasemetrics "k8s.io/component-base/metrics"
	"k8s.io/component-base/version/verflag"
	"k8s.io/klog/v2"
	"k8s.io/pod-security-admission/admission"
	admissionapi "k8s.io/pod-security-admission/admission/api"
	podsecurityconfigloader "k8s.io/pod-security-admission/admission/api/load"
	"k8s.io/pod-security-admission/api"
	"k8s.io/pod-security-admission/cmd/webhook/server/options"
	"k8s.io/pod-security-admission/metrics"
	"k8s.io/pod-security-admission/policy"
)

// AdmissionReview 请求体通常远小于此值，限制上限主要是防止恶意大包把 webhook 内存打满。
const maxRequestSize = int64(3 * 1024 * 1024)

// NewSchedulerCommand creates a *cobra.Command object with default parameters and registryOptions
func NewServerCommand() *cobra.Command {
	opts := options.NewOptions()

	cmdName := "podsecurity-webhook"
	if executable, err := os.Executable(); err == nil {
		cmdName = filepath.Base(executable)
	}
	cmd := &cobra.Command{
		Use: cmdName,
		Long: `The PodSecurity webhook is a standalone webhook server implementing the Pod
Security Standards.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			verflag.PrintAndExitIfRequested()
			return runServer(cmd.Context(), opts)
		},
		Args: cobra.NoArgs,
	}
	opts.AddFlags(cmd.Flags())
	verflag.AddFlags(cmd.Flags())

	return cmd
}

// 用信号处理器把进程退出信号转成 context 取消，让两个 Serve 能一起优雅关闭。
func runServer(ctx context.Context, opts *options.Options) error {
	config, err := LoadConfig(opts)
	if err != nil {
		return err
	}
	server, err := Setup(config)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		stopCh := apiserver.SetupSignalHandler()
		<-stopCh
		cancel()
	}()

	return server.Start(ctx)
}

// Server 聚合监听配置、informers 和准入代理；把依赖塞进一个结构便于 Start 统一生命周期。
type Server struct {
	secureServing   *apiserver.SecureServingInfo
	insecureServing *apiserver.DeprecatedInsecureServingInfo

	informerFactory kubeinformers.SharedInformerFactory

	delegate *admission.Admission

	metricsRegistry compbasemetrics.KubeRegistry
}

// 先等监听器退出再等已有连接处理完，避免日志里出现“还在收请求但已宣布退出”的假象。
func (s *Server) Start(ctx context.Context) error {
	s.informerFactory.Start(ctx.Done())
	logger := klog.FromContext(ctx)

	mux := http.NewServeMux()
	healthz.InstallHandler(mux, healthz.PingHealthz)
	healthz.InstallReadyzHandler(mux, healthz.NewInformerSyncHealthz(s.informerFactory))
	// The webhook is stateless, so it's safe to expose everything on the insecure port for
	// debugging or proxy purposes. The API server will not connect to an http webhook.
	mux.HandleFunc("/", s.HandleValidate)

	// Serve the metrics.
	mux.Handle("/metrics",
		compbasemetrics.HandlerFor(s.metricsRegistry, compbasemetrics.HandlerOpts{ErrorHandling: compbasemetrics.ContinueOnError}))

	if s.insecureServing != nil {
		if err := s.insecureServing.Serve(mux, 0, ctx.Done()); err != nil {
			return fmt.Errorf("failed to start insecure server: %w", err)
		}
	}

	var shutdownCh <-chan struct{}
	var listenerStoppedCh <-chan struct{}
	if s.secureServing != nil {
		var err error
		shutdownCh, listenerStoppedCh, err = s.secureServing.Serve(mux, 0, ctx.Done())
		if err != nil {
			return fmt.Errorf("failed to start secure server: %w", err)
		}
	}

	<-listenerStoppedCh
	logger.V(1).Info("[graceful-termination] HTTP Server has stopped listening")

	// Wait for graceful shutdown.
	<-shutdownCh
	logger.V(1).Info("[graceful-termination] HTTP Server is exiting")

	return nil
}

// HandleValidate 严格只接受 v1 AdmissionReview：API server 期望相同 kind 的响应，Kind 不一致必须早退。
func (s *Server) HandleValidate(w http.ResponseWriter, r *http.Request) {
	defer utilruntime.HandleCrash(func(_ interface{}) {
		// Assume the crash happened before the response was written.
		http.Error(w, "internal server error", http.StatusInternalServerError)
	})

	var (
		body   []byte
		err    error
		ctx    = r.Context()
		logger = klog.FromContext(ctx)
	)

	if timeout, ok, err := parseTimeout(r); err != nil {
		// Ignore an invalid timeout.
		logger.V(2).Info("Invalid timeout", "error", err)
	} else if ok {
		var cancel context.CancelFunc
		// 调用方给的 timeout 只约束本次校验；取消时释放，防止超时后 goroutine 泄漏。
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	if r.Body == nil || r.Body == http.NoBody {
		err = errors.New("request body is empty")
		logger.Error(err, "bad request")
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	defer r.Body.Close()
	// 先限量再 ReadAll：直接读无限 body 会让单个请求拖垮服务。
	limitedReader := &io.LimitedReader{R: r.Body, N: maxRequestSize}
	if body, err = io.ReadAll(limitedReader); err != nil {
		logger.Error(err, "unable to read the body from the incoming request")
		http.Error(w, "unable to read the body from the incoming request", http.StatusBadRequest)
		return
	}
	if limitedReader.N <= 0 {
		logger.Error(err, "unable to read the body from the incoming request; limit reached")
		http.Error(w, fmt.Sprintf("request entity is too large; limit is %d bytes", maxRequestSize), http.StatusRequestEntityTooLarge)
		return
	}

	// verify the content type is accurate
	if contentType := r.Header.Get("Content-Type"); contentType != "application/json" {
		err = fmt.Errorf("contentType=%s, expected application/json", contentType)
		logger.Error(err, "unable to process a request with an unknown content type", "type", contentType)
		http.Error(w, "unable to process a request with a non-json content type", http.StatusBadRequest)
		return
	}

	v1AdmissionReviewKind := admissionv1.SchemeGroupVersion.WithKind("AdmissionReview")
	reviewObject, gvk, err := codecs.UniversalDeserializer().Decode(body, &v1AdmissionReviewKind, nil)
	if err != nil {
		logger.Error(err, "unable to decode the request")
		http.Error(w, "unable to decode the request", http.StatusBadRequest)
		return
	}
	if *gvk != v1AdmissionReviewKind {
		logger.Info("Unexpected AdmissionReview kind", "kind", gvk.String())
		http.Error(w, fmt.Sprintf("unexpected AdmissionReview kind: %s", gvk.String()), http.StatusBadRequest)
		return
	}
	review, ok := reviewObject.(*admissionv1.AdmissionReview)
	if !ok {
		logger.Info("Failed admissionv1.AdmissionReview type assertion")
		http.Error(w, "unexpected AdmissionReview type", http.StatusBadRequest)
	}
	logger.V(1).Info("received request", "UID", review.Request.UID, "kind", review.Request.Kind, "resource", review.Request.Resource)

	attributes := api.RequestAttributes(review.Request, codecs.UniversalDeserializer())
	response := s.delegate.Validate(ctx, attributes)
	response.UID = review.Request.UID // Response UID must match request UID
	review.Response = response
	writeResponse(w, review)
}

// Config holds the loaded options.Options used to set up the webhook server.
//
// 内部 Config 与命令行 Options 分开：后者可以被校验和修改，前者只承载已生效的配置。
type Config struct {
	SecureServing     *apiserver.SecureServingInfo
	InsecureServing   *apiserver.DeprecatedInsecureServingInfo
	KubeConfig        *restclient.Config
	PodSecurityConfig *admissionapi.PodSecurityConfiguration
}

// LoadConfig loads the Config from the Options.
//
// 客户端 QPS 和 Burst 单独设置，避免 webhook 在批量准入时被 client-go 默认限流拖慢。
func LoadConfig(opts *options.Options) (*Config, error) {
	if errs := opts.Validate(); len(errs) > 0 {
		return nil, utilerrors.NewAggregate(errs)
	}

	var c Config
	opts.SecureServing.ApplyTo(&c.SecureServing)

	// Load Kube Client
	kubeConfig, err := clientcmd.BuildConfigFromFlags("", opts.Kubeconfig)
	if err != nil {
		return nil, err
	}
	kubeConfig.QPS = opts.ClientQPSLimit
	kubeConfig.Burst = opts.ClientQPSBurst
	c.KubeConfig = restclient.AddUserAgent(kubeConfig, "podsecurity-webhook")

	// Load PodSecurity config
	c.PodSecurityConfig, err = podsecurityconfigloader.LoadFromFile(opts.Config)
	if err != nil {
		return nil, err
	}

	return &c, nil
}

// Setup creates an Admission object to handle the admission logic.
//
// 这里只注册 namespace informer：Pod 安全配置按 namespace 匹配，其它 informer 用不上。
func Setup(c *Config) (*Server, error) {
	s := &Server{
		secureServing:   c.SecureServing,
		insecureServing: c.InsecureServing,
	}

	if s.secureServing == nil && s.insecureServing == nil {
		return nil, errors.New("no serving info configured")
	}

	client, err := clientset.NewForConfig(c.KubeConfig)
	if err != nil {
		return nil, err
	}
	s.informerFactory = kubeinformers.NewSharedInformerFactory(client, 0 /* no resync */)
	namespaceInformer := s.informerFactory.Core().V1().Namespaces()
	namespaceLister := namespaceInformer.Lister()

	evaluator, err := policy.NewEvaluator(policy.DefaultChecks(), nil)
	if err != nil {
		return nil, fmt.Errorf("could not create PodSecurityRegistry: %w", err)
	}
	metrics := metrics.NewPrometheusRecorder(api.GetAPIVersion())
	// 独立 registry 避免把 webhook 指标和进程默认指标混在一起，/metrics 只暴露关心的部分。
	s.metricsRegistry = compbasemetrics.NewKubeRegistry()
	metrics.MustRegister(s.metricsRegistry.MustRegister)

	s.delegate = &admission.Admission{
		Configuration:    c.PodSecurityConfig,
		Evaluator:        evaluator,
		Metrics:          metrics,
		PodSpecExtractor: admission.DefaultPodSpecExtractor{},
		PodLister:        admission.PodListerFromClient(client),
		NamespaceGetter:  admission.NamespaceGetterFromListerAndClient(namespaceLister, client),
	}

	if err := s.delegate.CompleteConfiguration(); err != nil {
		return nil, fmt.Errorf("configuration error: %w", err)
	}
	if err := s.delegate.ValidateConfiguration(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return s, nil
}

// 响应码固定 200，真正的拒绝信息放在 AdmissionResponse 里；否则 API server 会当成 webhook 故障。
func writeResponse(w http.ResponseWriter, review *admissionv1.AdmissionReview) {
	// Webhooks should always respond with a 200 HTTP status code when an AdmissionResponse can be sent.
	// In an error case, the true status code is captured in the response.result.code
	if err := json.NewEncoder(w).Encode(review); err != nil {
		klog.ErrorS(err, "Failed to encode response")
		// Unable to send an AdmissionResponse, fall back to an HTTP error.
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// parseTimeout parses the given HTTP request URL and extracts the timeout query parameter
// value if specified by the user.
// If a timeout is not specified the function returns false and err is set to nil
// If the value specified is malformed then the function returns false and err is set
//
// timeout 只是可选的性能保护；非法值直接忽略，不让格式错误挡住正常的准入请求。
func parseTimeout(req *http.Request) (time.Duration, bool, error) {
	value := req.URL.Query().Get("timeout")
	if value == "" {
		return 0, false, nil
	}

	timeout, err := time.ParseDuration(value)
	if err != nil {
		return 0, false, fmt.Errorf("invalid timeout query: %w", err)
	}

	return timeout, true, nil
}
