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

package prober

import (
	"context"
	"fmt"
	"io"
	"time"

	v1 "k8s.io/api/core/v1"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/features"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	"k8s.io/kubernetes/pkg/kubelet/events"
	"k8s.io/kubernetes/pkg/kubelet/prober/results"
	"k8s.io/kubernetes/pkg/kubelet/util/format"
	"k8s.io/kubernetes/pkg/probe"
	execprobe "k8s.io/kubernetes/pkg/probe/exec"
	grpcprobe "k8s.io/kubernetes/pkg/probe/grpc"
	httpprobe "k8s.io/kubernetes/pkg/probe/http"
	tcpprobe "k8s.io/kubernetes/pkg/probe/tcp"
	"k8s.io/utils/exec"
)

// maxProbeRetries 是探针遇到错误时的最大尝试次数。
const maxProbeRetries = 3

// prober 用于检查容器的存活、就绪和启动状态。
type prober struct {
	// exec 执行容器内命令探针。
	exec execprobe.Prober
	// http 执行 HTTP(S) 探针。
	http httpprobe.Prober
	// tcp 执行 TCP 端口探针。
	tcp tcpprobe.Prober
	// grpc 执行 gRPC 健康检查探针。
	grpc grpcprobe.Prober
	// runner 用于在容器内执行命令。
	runner kubecontainer.CommandRunner

	// recorder 用于上报探针相关事件。
	recorder record.EventRecorderLogger
}

// newProber 创建 prober，需要传入命令执行器和事件记录器。
func newProber(
	runner kubecontainer.CommandRunner,
	recorder record.EventRecorderLogger) *prober {

	// 探针不跟随跨主机重定向，避免把检查目标带到 Pod 网络之外。
	const followNonLocalRedirects = false
	return &prober{
		exec:     execprobe.New(),
		http:     httpprobe.New(followNonLocalRedirects),
		tcp:      tcpprobe.New(),
		grpc:     grpcprobe.New(),
		runner:   runner,
		recorder: recorder,
	}
}

// recordContainerEvent 用于上报与容器相关的所有探针事件。
func (pb *prober) recordContainerEvent(ctx context.Context, pod *v1.Pod, container *v1.Container, eventType, reason, message string, args ...interface{}) {
	logger := klog.FromContext(ctx)
	// 生成容器引用，使事件能关联到具体的 Pod 和容器。
	ref, err := kubecontainer.GenerateContainerRef(pod, container)
	if err != nil {
		logger.Error(err, "Can't make a ref to pod and container", "pod", klog.KObj(pod), "containerName", container.Name)
		return
	}
	pb.recorder.WithLogger(logger).Eventf(ref, eventType, reason, message, args...)
}

// probe 对容器执行指定类型的探针，并返回探针结果。
func (pb *prober) probe(ctx context.Context, probeType probeType, pod *v1.Pod, status v1.PodStatus, container v1.Container, containerID kubecontainer.ContainerID) (results.Result, error) {
	var probeSpec *v1.Probe
	// 根据探针类型取出对应的探针配置。
	switch probeType {
	case readiness:
		probeSpec = container.ReadinessProbe
	case liveness:
		probeSpec = container.LivenessProbe
	case startup:
		probeSpec = container.StartupProbe
	default:
		return results.Failure, fmt.Errorf("unknown probe type: %q", probeType)
	}

	logger := klog.FromContext(ctx)
	// nil 探针视为成功，表示该容器没有配置对应类型的探针。
	if probeSpec == nil {
		logger.Info("Probe is nil", "probeType", probeType, "pod", klog.KObj(pod), "podUID", pod.UID, "containerName", container.Name)
		return results.Success, nil
	}

	result, output, err := pb.runProbeWithRetries(ctx, probeType, probeSpec, pod, status, container, containerID, maxProbeRetries)

	if err != nil {
		// 探针执行报错属于失败，需要上报事件并返回 Failure。
		logger.V(1).Info("Probe errored", "probeType", probeType, "pod", klog.KObj(pod), "podUID", pod.UID, "containerName", container.Name, "probeResult", result, "err", err)
		pb.recordContainerEvent(ctx, pod, &container, v1.EventTypeWarning, events.ContainerUnhealthy, "%s probe errored and resulted in %s state: %s", probeType, result, err)
		return results.Failure, err
	}

	switch result {
	case probe.Success:
		// 探针明确成功。
		logger.V(3).Info("Probe succeeded", "probeType", probeType, "pod", klog.KObj(pod), "podUID", pod.UID, "containerName", container.Name)
		return results.Success, nil

	case probe.Warning:
		// 警告仍然算作成功，但会上报事件提醒用户。
		pb.recordContainerEvent(ctx, pod, &container, v1.EventTypeWarning, events.ContainerProbeWarning, "%s probe warning: %s", probeType, output)
		logger.V(3).Info("Probe succeeded with a warning", "probeType", probeType, "pod", klog.KObj(pod), "podUID", pod.UID, "containerName", container.Name, "output", output)
		return results.Success, nil

	case probe.Failure:
		// 探针失败，记录输出以便诊断。
		logger.V(1).Info("Probe failed", "probeType", probeType, "pod", klog.KObj(pod), "podUID", pod.UID, "containerName", container.Name, "probeResult", result, "output", output)
		pb.recordContainerEvent(ctx, pod, &container, v1.EventTypeWarning, events.ContainerUnhealthy, "%s probe failed: %s", probeType, output)
		return results.Failure, nil

	case probe.Unknown:
		// 没有错误但结果未知时按失败处理，避免容器被错误地标记为健康。
		logger.V(1).Info("Probe unknown without error", "probeType", probeType, "pod", klog.KObj(pod), "podUID", pod.UID, "containerName", container.Name, "probeResult", result)
		return results.Failure, nil

	default:
		logger.V(1).Info("Unsupported probe result", "probeType", probeType, "pod", klog.KObj(pod), "podUID", pod.UID, "containerName", container.Name, "probeResult", result)
		return results.Failure, nil
	}
}

// runProbeWithRetries 在有限次数内重试探针；如果始终失败，则返回最后一次结果。
func (pb *prober) runProbeWithRetries(ctx context.Context, probeType probeType, p *v1.Probe, pod *v1.Pod, status v1.PodStatus, container v1.Container, containerID kubecontainer.ContainerID, retries int) (probe.Result, string, error) {
	var err error
	var result probe.Result
	var output string
	for i := 0; i < retries; i++ {
		result, output, err = pb.runProbe(ctx, probeType, p, pod, status, container, containerID)
		// 只有探针执行报错时才重试；一旦得到确定结果就立即返回。
		if err == nil {
			return result, output, nil
		}
	}
	return result, output, err
}

// runProbe 根据探针配置选择 exec、HTTP、TCP 或 gRPC 中的一种方式执行检查。
func (pb *prober) runProbe(ctx context.Context, probeType probeType, p *v1.Probe, pod *v1.Pod, status v1.PodStatus, container v1.Container, containerID kubecontainer.ContainerID) (probe.Result, string, error) {
	logger := klog.FromContext(ctx)
	// 探针超时以秒为单位配置，统一换算为 time.Duration。
	timeout := time.Duration(p.TimeoutSeconds) * time.Second
	switch {
	case p.Exec != nil:
		logger.V(4).Info("Exec-Probe runProbe", "pod", klog.KObj(pod), "containerName", container.Name, "execCommand", p.Exec.Command)
		// 先静态展开容器环境变量，再在容器内执行命令。
		command := kubecontainer.ExpandContainerCommandOnlyStatic(p.Exec.Command, container.Env)
		return pb.exec.Probe(pb.newExecInContainer(ctx, pod, container, containerID, command, timeout))

	case p.HTTPGet != nil:
		req, err := httpprobe.NewRequestForHTTPGetAction(p.HTTPGet, &container, status.PodIP, "probe")
		if err != nil {
			// 请求构造失败属于未知结果，返回错误交给重试逻辑处理。
			logger.V(4).Info("HTTP-Probe failed to create request", "error", err)
			return probe.Unknown, "", err
		}
		if loggerV4 := logger.V(4); logger.Enabled() {
			port := req.URL.Port()
			host := req.URL.Hostname()
			path := req.URL.Path
			scheme := req.URL.Scheme
			headers := p.HTTPGet.HTTPHeaders
			loggerV4.Info("HTTP-Probe", "scheme", scheme, "host", host, "port", port, "path", path, "timeout", timeout, "headers", headers, "probeType", probeType)
		}
		// HTTP2 (H2C) 探针需要显式开启特性门控才生效。
		if p.HTTPGet.Protocol != nil && *p.HTTPGet.Protocol == v1.HTTPProtocolHTTP2 &&
			utilfeature.DefaultFeatureGate.Enabled(features.H2CContainerProbe) {
			return pb.http.ProbeH2C(req, timeout)
		}
		return pb.http.Probe(req, timeout)

	case p.TCPSocket != nil:
		port, err := probe.ResolveContainerPort(p.TCPSocket.Port, &container)
		if err != nil {
			logger.V(4).Info("TCP-Probe failed to resolve port", "error", err)
			return probe.Unknown, "", err
		}
		// 未指定 host 时默认探测 Pod IP。
		host := p.TCPSocket.Host
		if host == "" {
			host = status.PodIP
		}
		logger.V(4).Info("TCP-Probe", "host", host, "port", port, "timeout", timeout)
		return pb.tcp.Probe(host, port, timeout)

	case p.GRPC != nil:
		host := status.PodIP
		service := ""
		if p.GRPC.Service != nil {
			service = *p.GRPC.Service
		}
		// 只有开启特性门控且探针显式使用 TLS 模式时才启用 TLS。
		useTLS := utilfeature.DefaultFeatureGate.Enabled(features.GRPCContainerProbeTLS) &&
			p.GRPC.Mode != nil && *p.GRPC.Mode == v1.GRPCProbeModeTLS
		logger.V(4).Info("GRPC-Probe", "host", host, "service", service, "port", p.GRPC.Port, "timeout", timeout, "tls", useTLS)
		return pb.grpc.Probe(host, service, int(p.GRPC.Port), timeout, grpcprobe.ProbeOptions{UseTLS: useTLS})

	default:
		// 没有任何探针处理器说明探针配置非法。
		logger.V(4).Info("Failed to find probe builder for container", "containerName", container.Name)
		return probe.Unknown, "", fmt.Errorf("missing probe handler for %s:%s", format.Pod(pod), container.Name)
	}
}

// execInContainer 适配 kubecontainer 的命令执行器，使其满足 exec.Cmd 接口，
// 供 exec 探针使用。
type execInContainer struct {
	// run 在容器内执行命令，始终返回合并后的 stdout 和 stderr；出错时返回错误。
	run       func() ([]byte, error)
	writer    io.Writer
	pod       *v1.Pod
	container v1.Container
}

// newExecInContainer 构造 exec.Cmd 适配器，将探针命令委托给容器运行时执行。
func (pb *prober) newExecInContainer(ctx context.Context, pod *v1.Pod, container v1.Container, containerID kubecontainer.ContainerID, cmd []string, timeout time.Duration) exec.Cmd {
	return &execInContainer{
		run:       func() ([]byte, error) { return pb.runner.RunInContainer(ctx, containerID, cmd, timeout) },
		pod:       pod,
		container: container,
	}
}

// Run 是 exec.Cmd 接口的一部分；命令实际在 CombinedOutput/Start 中同步执行，因此这里无事可做。
func (eic *execInContainer) Run() error {
	return nil
}

// CombinedOutput 执行命令并返回合并后的输出。
func (eic *execInContainer) CombinedOutput() ([]byte, error) {
	return eic.run()
}

// Output 未实现：探针始终通过 CombinedOutput 获取输出。
func (eic *execInContainer) Output() ([]byte, error) {
	return nil, fmt.Errorf("unimplemented")
}

// SetDir 未实现：探针命令不支持指定工作目录。
func (eic *execInContainer) SetDir(dir string) {
	// 未实现。
}

// SetStdin 未实现：探针命令不接收标准输入。
func (eic *execInContainer) SetStdin(in io.Reader) {
	// 未实现。
}

// SetStdout 记录输出写入目标。
func (eic *execInContainer) SetStdout(out io.Writer) {
	eic.writer = out
}

// SetStderr 与 SetStdout 共用同一个写入目标，从而合并输出。
func (eic *execInContainer) SetStderr(out io.Writer) {
	eic.writer = out
}

// SetEnv 未实现：探针命令的环境变量由容器本身提供。
func (eic *execInContainer) SetEnv(env []string) {
	// 未实现。
}

// Stop 未实现：命令超时由运行时负责终止。
func (eic *execInContainer) Stop() {
	// 未实现。
}

// Start 同步执行命令，并把输出写入已设置的 writer。
func (eic *execInContainer) Start() error {
	data, err := eic.run()
	if eic.writer != nil {
		// 只记录写入错误，不用它覆盖命令本身的执行错误。
		if p, err := eic.writer.Write(data); err != nil {
			// 当前没有合适的 context/logger 可传递，因此使用 klog.TODO()；
			// 后续为该函数引入 context 参数时应替换为相应的 logger。
			klog.TODO().Error(err, "Unable to write all bytes from execInContainer", "expectedBytes", len(data), "actualBytes", p, "pod", klog.KObj(eic.pod), "containerName", eic.container.Name)
		}
	}
	return err
}

// Wait 未实现：Start 已同步执行完成。
func (eic *execInContainer) Wait() error {
	return nil
}

// StdoutPipe 未实现。
func (eic *execInContainer) StdoutPipe() (io.ReadCloser, error) {
	return nil, fmt.Errorf("unimplemented")
}

// StderrPipe 未实现。
func (eic *execInContainer) StderrPipe() (io.ReadCloser, error) {
	return nil, fmt.Errorf("unimplemented")
}
