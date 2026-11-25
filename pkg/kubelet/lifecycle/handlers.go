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

package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	versionutil "k8s.io/apimachinery/pkg/util/version"
	"k8s.io/client-go/tools/record"
	ndf "k8s.io/component-helpers/nodedeclaredfeatures"
	"k8s.io/klog/v2"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	"k8s.io/kubernetes/pkg/kubelet/metrics"
	kubetypes "k8s.io/kubernetes/pkg/kubelet/types"
	"k8s.io/kubernetes/pkg/kubelet/util/format"
	httpprobe "k8s.io/kubernetes/pkg/probe/http"
	"k8s.io/kubernetes/pkg/security/apparmor"
)

const (
	// 单次 HTTP 响应体读取上限，防止生命周期钩子返回超大响应体时占用过多内存。
	maxRespBodyLength = 10 * 1 << 10

	// AppArmorNotAdmittedReason 是 Pod 因 AppArmor 校验失败而被拒绝准入的原因。
	AppArmorNotAdmittedReason = "AppArmor"
	// PodLevelResourcesNotAdmittedReason 是节点不支持 Pod 级别资源时拒绝准入的原因。
	PodLevelResourcesNotAdmittedReason = "PodLevelResourcesNotSupported"

	// Pod 特性准入失败的原因。
	PodFeatureUnsupported = "PodFeatureUnsupported"
)

// handlerRunner 实现 kubecontainer.HandlerRunner，负责执行容器的 Exec、HTTPGet
// 和 Sleep 三类生命周期钩子。
type handlerRunner struct {
	// httpDoer 用于发出 HTTPGet 钩子请求。
	httpDoer kubetypes.HTTPDoer
	// commandRunner 用于在容器内执行 Exec 钩子命令。
	commandRunner kubecontainer.CommandRunner
	// containerManager 在钩子未指定 host 时提供 Pod 及其 IP 信息。
	containerManager podStatusProvider
	// eventRecorder 用于上报 HTTP 钩子降级等事件。
	eventRecorder record.EventRecorder
}

// podStatusProvider 抽象出执行生命周期钩子所需的 Pod 和 PodStatus 查询能力。
type podStatusProvider interface {
	GetPod(ctx context.Context, podUID types.UID) (*kubecontainer.Pod, error)
	GetPodStatus(ctx context.Context, pod *kubecontainer.Pod) (*kubecontainer.PodStatus, error)
}

// NewHandlerRunner 返回为容器配置好的生命周期钩子执行器。
func NewHandlerRunner(httpDoer kubetypes.HTTPDoer, commandRunner kubecontainer.CommandRunner, containerManager podStatusProvider, eventRecorder record.EventRecorder) kubecontainer.HandlerRunner {
	return &handlerRunner{
		httpDoer:         httpDoer,
		commandRunner:    commandRunner,
		containerManager: containerManager,
		eventRecorder:    eventRecorder,
	}
}

// Run 执行指定的生命周期钩子，三种钩子互斥；返回用于事件上报的描述信息和错误。
func (hr *handlerRunner) Run(ctx context.Context, containerID kubecontainer.ContainerID, pod *v1.Pod, container *v1.Container, handler *v1.LifecycleHandler) (string, error) {
	logger := klog.FromContext(ctx)
	switch {
	case handler.Exec != nil:
		var msg string
		// TODO(tallclair): 传入合适的超时值。
		// 超时传 0 表示使用运行时默认值；失败时附带命令输出，便于排查。
		output, err := hr.commandRunner.RunInContainer(ctx, containerID, handler.Exec.Command, 0)
		if err != nil {
			msg = fmt.Sprintf("Exec lifecycle hook (%v) for Container %q in Pod %q failed - error: %v, message: %q", handler.Exec.Command, container.Name, format.Pod(pod), err, string(output))
			logger.V(1).Info("Exec lifecycle hook for Container in Pod failed", "execCommand", handler.Exec.Command, "containerName", container.Name, "pod", klog.KObj(pod), "message", string(output), "err", err)
		}
		return msg, err
	case handler.HTTPGet != nil:
		// 钩子成功时 err 为 nil，msg 保持为空字符串。
		err := hr.runHTTPHandler(ctx, pod, container, handler, hr.eventRecorder)
		var msg string
		if err != nil {
			msg = fmt.Sprintf("HTTP lifecycle hook (%s) for Container %q in Pod %q failed - error: %v", handler.HTTPGet.Path, container.Name, format.Pod(pod), err)
			logger.V(1).Info("HTTP lifecycle hook for Container in Pod failed", "path", handler.HTTPGet.Path, "containerName", container.Name, "pod", klog.KObj(pod), "err", err)
		}
		return msg, err
	case handler.Sleep != nil:
		// 睡眠钩子只在上下文取消时返回错误，其余情况都属于成功。
		err := hr.runSleepHandler(ctx, handler.Sleep.Seconds)
		var msg string
		if err != nil {
			msg = fmt.Sprintf("Sleep lifecycle hook (%d) for Container %q in Pod %q failed - error: %v", handler.Sleep.Seconds, container.Name, format.Pod(pod), err)
			logger.V(1).Info("Sleep lifecycle hook for Container in Pod failed", "sleepSeconds", handler.Sleep.Seconds, "containerName", container.Name, "pod", klog.KObj(pod), "err", err)
		}
		return msg, err
	default:
		// 未设置任何钩子说明 handler 非法，应显式报错而不是静默忽略。
		err := fmt.Errorf("invalid handler: %v", handler)
		msg := fmt.Sprintf("Cannot run handler: %v", err)
		logger.Error(err, "Cannot run handler")
		return msg, err
	}
}

// runSleepHandler 等待指定的秒数；容器在等待期间终止时立即返回错误，
// 避免调用方被阻塞到睡眠结束。
func (hr *handlerRunner) runSleepHandler(ctx context.Context, seconds int64) error {
	// 使用 time.After 而不是 time.Sleep，以便同时响应上下文取消。
	c := time.After(time.Duration(seconds) * time.Second)
	select {
	case <-ctx.Done():
		// 上下文取消意味着容器在睡眠结束前被终止，属于非预期终止。
		metrics.LifecycleHandlerSleepTerminated.Inc()
		return fmt.Errorf("container terminated before sleep hook finished")
	case <-c:
		return nil
	}
}

// runHTTPHandler 向 Pod 发送 HTTP 生命周期钩子请求：未指定 host 时使用 Pod IP；
// 若 HTTPS 请求收到明文 HTTP 响应，则降级为 HTTP 重试一次。
func (hr *handlerRunner) runHTTPHandler(ctx context.Context, pod *v1.Pod, container *v1.Container, handler *v1.LifecycleHandler, eventRecorder record.EventRecorder) error {
	logger := klog.FromContext(ctx)
	host := handler.HTTPGet.Host
	podIP := host
	if len(host) == 0 {
		// 未显式指定 host 时必须从运行时查询到 Pod IP，否则请求无法定位目标。
		runtimePod, err := hr.containerManager.GetPod(ctx, pod.UID)
		if err != nil {
			return fmt.Errorf("unable to get pod, event handlers may be invalid: %w", err)
		}
		status, err := hr.containerManager.GetPodStatus(ctx, runtimePod)
		if err != nil {
			return fmt.Errorf("unable to get pod status, event handlers may be invalid: %w", err)
		}
		if len(status.IPs) == 0 {
			return fmt.Errorf("failed to find networking container: %v", status)
		}
		host = status.IPs[0]
		podIP = host
	}

	// 复用 httpprobe 的请求构造逻辑，保证钩子与探针在 host/port/path 处理上一致。
	req, err := httpprobe.NewRequestForHTTPGetAction(handler.HTTPGet, container, podIP, "lifecycle")
	if err != nil {
		return err
	}
	resp, err := hr.httpDoer.Do(req)
	// 无论请求成败都要排空并关闭响应体，否则底层连接无法复用。
	discardHTTPRespBody(resp)

	if isHTTPResponseError(err) {
		// 典型场景是服务端对 HTTPS 请求返回了明文 HTTP 响应，降级重试一次。
		logger.V(1).Info("HTTPS request to lifecycle hook got HTTP response, retrying with HTTP.", "pod", klog.KObj(pod), "host", req.URL.Host, "err", err)

		req := req.Clone(ctx)
		req.URL.Scheme = "http"
		// 降级后通道不再加密，必须移除凭据，避免 Authorization 头泄漏。
		req.Header.Del("Authorization")
		resp, httpErr := hr.httpDoer.Do(req)

		// 降级重试成功，清除最初的错误。
		if httpErr == nil {
			metrics.LifecycleHandlerHTTPFallbacks.Inc()
			if eventRecorder != nil {
				// 通过事件暴露降级行为，提醒用户修正服务端 TLS 配置。
				eventRecorder.Event(pod, v1.EventTypeWarning, "LifecycleHTTPFallback", fmt.Sprintf("request to HTTPS lifecycle hook %s got HTTP response, retry with HTTP succeeded", req.URL.Host))
			}
			err = nil
		}
		discardHTTPRespBody(resp)
	}
	return err
}

// discardHTTPRespBody 读取并关闭响应体；只有完整读完响应体，底层 TCP 连接才能被复用。
func discardHTTPRespBody(resp *http.Response) {
	if resp == nil {
		return
	}

	// 在重新连接前确保响应体被完整读取并关闭，从而复用同一条 TCP 连接。
	defer resp.Body.Close()

	// 超过上限的响应体不读取，直接关闭，避免被超大响应拖住。
	if resp.ContentLength <= maxRespBodyLength {
		io.Copy(io.Discard, &io.LimitedReader{R: resp.Body, N: maxRespBodyLength})
	}
}

// NewAppArmorAdmitHandler 返回一个 PodAdmitHandler，用于从 AppArmor 角度
// 判断 Pod 是否允许准入。
func NewAppArmorAdmitHandler(validator apparmor.Validator) PodAdmitHandler {
	return &appArmorAdmitHandler{
		Validator: validator,
	}
}

// appArmorAdmitHandler 借助 apparmor.Validator 校验 Pod 的 AppArmor 配置。
type appArmorAdmitHandler struct {
	apparmor.Validator
}

// Admit 仅在 Pod 处于 Pending 阶段时重新校验 AppArmor，已运行或已终止的 Pod 直接放行。
func (a *appArmorAdmitHandler) Admit(_ context.Context, attrs *PodAdmitAttributes) PodAdmitResult {
	// Pod 已运行或已终止时无需重复检查 AppArmor，避免准入路径上的多余开销。
	if attrs.Pod.Status.Phase != v1.PodPending {
		return PodAdmitResult{Admit: true}
	}

	// 校验失败时返回明确原因，便于用户定位 AppArmor 配置问题。
	err := a.Validate(attrs.Pod)
	if err == nil {
		return PodAdmitResult{Admit: true}
	}
	return PodAdmitResult{
		Admit:   false,
		Reason:  AppArmorNotAdmittedReason,
		Message: fmt.Sprintf("Cannot enforce AppArmor: %v", err),
	}
}

// isHTTPResponseError 判断错误是否为 "服务器对 HTTPS 请求返回明文 HTTP 响应"，
// 这种场景可以安全地降级为 HTTP 重试。
func isHTTPResponseError(err error) bool {
	if err == nil {
		return false
	}
	urlErr := &url.Error{}
	if !errors.As(err, &urlErr) {
		return false
	}
	return strings.Contains(urlErr.Err.Error(), "server gave HTTP response to HTTPS client")
}

// NewPodFeaturesAdmitHandler 返回一个 PodAdmitHandler，用于从 Pod 特性兼容性
// 角度判断 Pod 是否允许准入。
func NewPodFeaturesAdmitHandler() PodAdmitHandler {
	return &podFeaturesAdmitHandler{}
}

// podFeaturesAdmitHandler 检查节点是否支持 Pod 级别资源。
type podFeaturesAdmitHandler struct{}

// Admit 在节点不支持 Pod 级别资源时拒绝 Pod 准入。
func (h *podFeaturesAdmitHandler) Admit(_ context.Context, attrs *PodAdmitAttributes) PodAdmitResult {
	return isPodLevelResourcesSupported(attrs.Pod)
}

// declaredFeaturesAdmitHandler 是一个检查 Pod 特性需求的 PodAdmitHandler。
type declaredFeaturesAdmitHandler struct {
	ndfFramework *ndf.Framework
	ndfSet       ndf.FeatureSet
	version      *versionutil.Version
}

// NewDeclaredFeaturesAdmitHandler 返回新的节点声明特性准入处理器。
func NewDeclaredFeaturesAdmitHandler(nodeDeclaredFeaturesHelper *ndf.Framework, nodeDeclaredFeaturesSet ndf.FeatureSet, version *versionutil.Version) PodAdmitHandler {
	return &declaredFeaturesAdmitHandler{
		ndfFramework: nodeDeclaredFeaturesHelper,
		ndfSet:       nodeDeclaredFeaturesSet,
		version:      version,
	}
}

// Admit 检查节点声明的特性集是否满足 Pod 的特性需求。
func (c *declaredFeaturesAdmitHandler) Admit(_ context.Context, attrs *PodAdmitAttributes) PodAdmitResult {
	pod := attrs.Pod

	// PodInfo 同时包含 Spec 与 Status，推断逻辑可能依赖两者。
	podInfo := &ndf.PodInfo{Spec: &pod.Spec, Status: &pod.Status}
	reqs, err := c.ndfFramework.InferForPodScheduling(podInfo, c.version)
	if err != nil {
		return PodAdmitResult{
			Admit:   false,
			Reason:  PodFeatureUnsupported,
			Message: fmt.Sprintf("Failed to infer pod's feature requirements: %v", err),
		}
	}

	// Pod 没有任何特性需求时，准入结果不受节点特性集影响。
	if reqs.IsEmpty() {
		return PodAdmitResult{Admit: true}
	}

	matchResult, err := c.ndfFramework.MatchNodeFeatureSet(reqs, c.ndfSet)
	if err != nil {
		return PodAdmitResult{
			Admit:   false,
			Reason:  PodFeatureUnsupported,
			Message: fmt.Sprintf("Failed to match pod's feature requirements against the node: %v", err),
		}
	}

	// 列出未满足的特性需求，帮助用户判断是升级节点还是调整 Pod。
	if !matchResult.IsMatch {
		return PodAdmitResult{
			Admit:   false,
			Reason:  PodFeatureUnsupported,
			Message: fmt.Sprintf("Pod requires node features that are not available: %s", strings.Join(matchResult.UnsatisfiedRequirements, ", ")),
		}
	}

	return PodAdmitResult{Admit: true}
}
