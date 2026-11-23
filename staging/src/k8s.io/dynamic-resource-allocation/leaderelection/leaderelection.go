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

// Package leaderelection wraps k8s.io/client-go/tools/leaderelection with a
// simpler API. It's derived from https://github.com/kubernetes-csi/csi-lib-utils/tree/v0.11.0/leaderelection
// 本包以更简洁的 API 封装 k8s.io/client-go/tools/leaderelection，
// 支持通过函数式选项配置租约、选举身份与健康检查，并派生自 csi-lib-utils 的 leaderelection 实现。
package leaderelection

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
)

const (
	// defaultLeaseDuration 是 leader 持有租约的默认时长（15 秒），其他成员在该时长内不会发起夺主。
	// defaultRenewDeadline 是leader 续约租约的默认超时（10 秒），超时将放弃领导权。
	// defaultRetryPeriod 是非 leader 成员重试获取锁的默认间隔（5 秒）。
	// defaultHealthCheckTimeout 是租约到期后仍允许的最大宽限期（20 秒），超过即上报不健康。
	defaultLeaseDuration      = 15 * time.Second
	defaultRenewDeadline      = 10 * time.Second
	defaultRetryPeriod        = 5 * time.Second
	defaultHealthCheckTimeout = 20 * time.Second

	// HealthCheckerAddress is the address at which the leader election health
	// checker reports status.
	// The caller sidecar should document this address in appropriate flag
	// descriptions.
	// HealthCheckerAddress 是领导者选举健康检查端点注册的 HTTP 路径。
	// 调用方的 sidecar 应在相应的命令行参数说明中记录该地址。
	HealthCheckerAddress = "/healthz/leader-election"
)

// leaderElection is a convenience wrapper around client-go's leader election library.
// leaderElection 是 client-go 领导者选举库的便捷封装，
// 集中保存选举配置（锁、身份、租约时长等）、健康检查适配器以及当选后要执行的业务逻辑。
type leaderElection struct {
	// runFunc 是当前实例当选 leader 后要执行的业务逻辑，参数为选举上下文。
	runFunc func(ctx context.Context)

	// the lockName identifies the leader election config and should be shared across all members
	// lockName 标识本次领导者选举的配置，所有参与选举的成员必须使用相同的值。
	lockName string
	// the identity is the unique identity of the currently running member
	// identity 是当前运行成员的唯一身份标识。
	identity string
	// the namespace to store the lock resource
	// namespace 是存放锁资源（Lease）的命名空间。
	namespace string
	// resourceLock defines the type of leaderelection that should be used
	// Only resourcelock.LeasesResourceLock is valid at the moment.
	// resourceLock 指定要使用的锁类型，目前只接受 LeasesResourceLock。
	resourceLock string
	// healthCheck reports unhealthy if leader election fails to renew leadership
	// within a timeout period.
	// healthCheck 是健康检查适配器：若在超时时间内未能续约领导权，则上报不健康。
	healthCheck *leaderelection.HealthzAdaptor

	// 以下四个字段是选举时序参数：租约时长、续约期限、重试间隔以及健康检查宽限期。
	leaseDuration      time.Duration
	renewDeadline      time.Duration
	retryPeriod        time.Duration
	healthCheckTimeout time.Duration

	// ctx 是运行选举时使用的上下文，未设置时 Run 会回退到 context.Background。
	ctx context.Context

	// clientset 用于访问 Kubernetes API（创建事件与操作 Lease 锁对象）。
	clientset kubernetes.Interface
}

// Option implements functional options for New.
// Option 是 New 使用的函数式选项类型，每个选项负责修改 leaderElection 的一个配置字段。
type Option func(l *leaderElection)

// New constructs a new leader election instance.
// New 构造一个领导者选举实例：先填入锁类型、各项时序参数的默认值以及调用方传入的客户端、锁名和业务逻辑，
// 再依次应用所有函数式选项，允许调用方覆盖这些默认值。
func New(clientset kubernetes.Interface, lockName string, runFunc func(ctx context.Context), opts ...Option) *leaderElection {
	// 使用默认值初始化实例：锁固定为 Leases，租约/续约/重试/健康检查超时均取包级默认常量。
	l := &leaderElection{
		runFunc:            runFunc,
		lockName:           lockName,
		resourceLock:       resourcelock.LeasesResourceLock,
		leaseDuration:      defaultLeaseDuration,
		renewDeadline:      defaultRenewDeadline,
		retryPeriod:        defaultRetryPeriod,
		healthCheckTimeout: defaultHealthCheckTimeout,
		clientset:          clientset,
	}

	// 依次应用调用方传入的选项，覆盖上面的默认配置。
	for _, opt := range opts {
		opt(l)
	}

	// 返回配置完成的选举实例（此时尚未启动选举，需调用 Run）。
	return l
}

// Identity 设置当前成员在选举中的身份标识。
// 若未设置，Run 会回退为主机名（见 defaultLeaderElectionIdentity）。
func Identity(identity string) Option {
	return func(l *leaderElection) {
		l.identity = identity
	}
}

// Namespace 设置存放锁资源（Lease）的命名空间。
// 若未设置，Run 会尝试从 Pod 环境变量或 ServiceAccount 文件中推断。
func Namespace(namespace string) Option {
	return func(l *leaderElection) {
		l.namespace = namespace
	}
}

// LeaseDuration 设置 leader 持有租约的时长，即其他成员在没有续约时可接管前需要等待的时间。
func LeaseDuration(leaseDuration time.Duration) Option {
	return func(l *leaderElection) {
		l.leaseDuration = leaseDuration
	}
}

// RenewDeadline 设置 leader 续约租约的超时时间；超过该时间仍未完成续约将放弃领导权。
func RenewDeadline(renewDeadline time.Duration) Option {
	return func(l *leaderElection) {
		l.renewDeadline = renewDeadline
	}
}

// RetryPeriod 设置非 leader 成员尝试获取锁的重试间隔。
func RetryPeriod(retryPeriod time.Duration) Option {
	return func(l *leaderElection) {
		l.retryPeriod = retryPeriod
	}
}

// HealthCheckTimeout 设置租约到期后判定健康检查失败前允许的最大宽限期。
func HealthCheckTimeout(timeout time.Duration) Option {
	return func(l *leaderElection) {
		l.healthCheckTimeout = timeout
	}
}

// Context 设置运行选举时使用的上下文。
// 若未设置，Run 会使用 context.Background。
func Context(ctx context.Context) Option {
	return func(l *leaderElection) {
		l.ctx = ctx
	}
}

// Server represents any type that could serve HTTP requests for the leader
// election health check endpoint.
// Server 抽象了能够注册 HTTP 处理器的服务（如 http.ServeMux），
// 供 PrepareHealthCheck 在其上挂载领导者选举健康检查端点。
type Server interface {
	Handle(pattern string, handler http.Handler)
}

// PrepareHealthCheck creates a health check for this leader election object
// with the given healthCheckTimeout and registers its HTTP handler to the given
// server at the path specified by the constant "healthCheckerAddress".
// healthCheckTimeout determines the max duration beyond lease expiration
// allowed before reporting unhealthy.
// The caller sidecar should document the handler address in appropriate flag
// descriptions.
// PrepareHealthCheck 为当前选举实例创建健康检查适配器，并把它的 HTTP 处理器注册到给定的 Server 上，
// 注册路径为 HealthCheckerAddress。healthCheckTimeout 决定租约到期后多久判定为不健康；
// 调用方的 sidecar 应在相应的参数说明中记录该处理器地址。
func (l *leaderElection) PrepareHealthCheck(s Server) {
	// 用配置的宽限期创建健康检查适配器，它同时作为选举的 WatchDog 使用。
	l.healthCheck = leaderelection.NewLeaderHealthzAdaptor(l.healthCheckTimeout)
	// 把适配器的 Check 方法包装成 HTTP 处理器，并注册到健康检查路径。
	s.Handle(HealthCheckerAddress, adaptCheckToHandler(l.healthCheck.Check))
}

// Run 启动领导者选举并阻塞直到上下文结束或进程退出。
// 它依次确定上下文、选举身份与命名空间，创建事件广播器与 Lease 锁，
// 装配好回调和健康检查看门狗后调用 client-go 的 RunOrDie 进入选举循环。
func (l *leaderElection) Run() error {
	// 未显式设置上下文时使用 Background，保证选举可以一直运行。
	ctx := l.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	// 未显式设置身份时，使用主机名作为当前成员的默认身份。
	if l.identity == "" {
		id, err := defaultLeaderElectionIdentity()
		if err != nil {
			// 获取默认身份失败时带上下文信息返回，便于排查主机名获取问题。
			return fmt.Errorf("error getting the default leader identity: %v", err)
		}

		l.identity = id
	}

	// 未显式设置命名空间时，从 Pod 环境或 ServiceAccount 文件中推断。
	if l.namespace == "" {
		l.namespace = inClusterNamespace()
	}

	// 创建事件广播器并写入当前命名空间的 Event，以便记录锁的获取/丢失事件。
	broadcaster := record.NewBroadcaster(record.WithContext(ctx))
	broadcaster.StartRecordingToSink(&corev1.EventSinkImpl{Interface: l.clientset.CoreV1().Events(l.namespace)})
	// 事件来源组件名由“锁名/成员身份”组成，方便在事件中区分不同选举与成员。
	eventRecorder := broadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: fmt.Sprintf("%s/%s", l.lockName, l.identity)})

	// 配置锁：身份与锁名都需要清洗为合法名称（仅字母、数字与连字符，且不以连字符结尾）。
	rlConfig := resourcelock.ResourceLockConfig{
		Identity:      sanitizeName(l.identity),
		EventRecorder: eventRecorder,
	}

	// 创建 Lease 锁对象；锁类型无效或参数不合法时直接返回错误。
	lock, err := resourcelock.New(l.resourceLock, l.namespace, sanitizeName(l.lockName), l.clientset.CoreV1(), l.clientset.CoordinationV1(), rlConfig)
	if err != nil {
		return err
	}

	// 组装 client-go 的选举配置：锁、时序参数、生命周期回调以及健康检查看门狗。
	leaderConfig := leaderelection.LeaderElectionConfig{
		Lock:          lock,
		LeaseDuration: l.leaseDuration,
		RenewDeadline: l.renewDeadline,
		RetryPeriod:   l.retryPeriod,
		// 当选/失去领导权/发现新 leader 时分别记录日志并执行相应的业务动作。
		Callbacks: leaderelection.LeaderCallbacks{
			// 当选 leader：记录日志并执行调用方传入的业务逻辑。
			OnStartedLeading: func(ctx context.Context) {
				klog.FromContext(ctx).Info("became leader, starting")
				l.runFunc(ctx)
			},
			// 失去领导权：记录错误日志，刷新日志后以状态码 1 退出进程，避免双主。
			OnStoppedLeading: func() {
				klog.FromContext(ctx).Error(nil, "stopped leading")
				klog.FlushAndExit(klog.ExitFlushTimeout, 1)
			},
			// 检测到新的 leader：记录其身份（属性名沿用已有拼写 idendity）。
			OnNewLeader: func(identity string) {
				klog.FromContext(ctx).Info("new leader detected", "idendity", identity)
			},
		},
		// 把健康检查适配器作为看门狗接入，续约失败时由它触发不健康状态。
		WatchDog: l.healthCheck,
	}

	// 进入选举循环：RunOrDie 不会在正常返回路径上把控制权交还调用方。
	leaderelection.RunOrDie(ctx, leaderConfig)
	// 实际上不可达；保留 nil 返回值以匹配函数签名。
	return nil // should never reach here
}

// defaultLeaderElectionIdentity 返回当前主机的主机名，作为未配置身份时的默认选举身份。
func defaultLeaderElectionIdentity() (string, error) {
	return os.Hostname()
}

// sanitizeName sanitizes the provided string so it can be consumed by leader election library
// sanitizeName 对输入字符串做清洗，使其符合领导者选举库对资源名称的要求：
// 只保留字母、数字和连字符，并保证名称不以连字符结尾。
func sanitizeName(name string) string {
	// 匹配所有非字母、非数字、非连字符的字符。
	re := regexp.MustCompile("[^a-zA-Z0-9-]")
	// 将这些非法字符统一替换为连字符。
	name = re.ReplaceAllString(name, "-")
	if name[len(name)-1] == '-' {
		// name must not end with '-'
		// 名称不允许以连字符结尾，追加一个 X 使其合法。
		name = name + "X"
	}
	// 返回清洗后的合法名称。
	return name
}

// inClusterNamespace returns the namespace in which the pod is running in by checking
// the env var POD_NAMESPACE, then the file /var/run/secrets/kubernetes.io/serviceaccount/namespace.
// if neither returns a valid namespace, the "default" namespace is returned
// inClusterNamespace 推断 Pod 所在的命名空间：先查环境变量 POD_NAMESPACE，
// 再读 ServiceAccount 挂载的 namespace 文件；两者都拿不到有效值时回退到 "default"。
func inClusterNamespace() string {
	// 优先使用环境变量 POD_NAMESPACE，通常由 Downward API 注入。
	if ns := os.Getenv("POD_NAMESPACE"); ns != "" {
		return ns
	}

	// 其次读取 ServiceAccount 的 namespace 文件；仅在读取成功且内容非空时采用。
	if data, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace"); err == nil {
		if ns := strings.TrimSpace(string(data)); len(ns) > 0 {
			return ns
		}
	}

	// 集群外运行或两种来源都不可用时，使用默认命名空间。
	return "default"
}

// adaptCheckToHandler returns an http.HandlerFunc that serves the provided checks.
// adaptCheckToHandler 把返回 error 的检查函数包装为 HTTP 处理器：
// 检查通过返回 "ok"，否则以 500 状态码返回错误信息，供健康检查探针使用。
func adaptCheckToHandler(c func(r *http.Request) error) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 执行底层检查函数（这里是健康检查适配器的 Check 方法）。
		err := c(r)
		if err != nil {
			// 检查失败：返回 500 并在响应体中带上具体错误原因。
			http.Error(w, fmt.Sprintf("internal server error: %v", err), http.StatusInternalServerError)
		} else {
			// 检查通过：返回简单的成功标记。
			fmt.Fprint(w, "ok")
		}
	})
}
