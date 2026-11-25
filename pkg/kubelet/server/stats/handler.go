/*
Copyright 2016 The Kubernetes Authors.

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

//go:generate mockery
package stats

import (
	"context"
	"fmt"
	"net/http"

	restful "github.com/emicklei/go-restful/v3"
	cadvisorapi "github.com/google/cadvisor/lib/model"
	"k8s.io/klog/v2"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	statsapi "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
	"k8s.io/kubernetes/pkg/kubelet/cm"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	"k8s.io/kubernetes/pkg/volume"
)

// Provider 汇总 stats handler 所需的方法。
type Provider interface {
	// 返回所有 Pod 管理的容器统计。
	ListPodStats(ctx context.Context) ([]statsapi.PodStats, error)
	// 返回所有 Pod 管理容器的 CPU 与内存统计。
	ListPodCPUAndMemoryStats(ctx context.Context) ([]statsapi.PodStats, error)
	// 返回所有 Pod 管理容器的统计，并强制刷新 cpu usageNanoCores；
	// 这是为未集成 cadvisor 的 CRI 运行时提供的变通方案，
	// 参见 https://github.com/kubernetes/kubernetes/issues/72788。
	ListPodStatsAndUpdateCPUNanoCoreUsage(ctx context.Context) ([]statsapi.PodStats, error)
	// 返回镜像文件系统的统计。
	// kubelet 支持三种容器文件系统布局：全部在节点根文件系统上（无独立 imagefs）；
	// 容器存储位于独立磁盘（imagefs 与 root 分离）；容器文件系统在 root 上而镜像存放在 imagefs。
	// 第一个返回值是镜像文件系统，第二个是容器文件系统。
	ImageFsStats(ctx context.Context) (imageFs *statsapi.FsStats, containerFs *statsapi.FsStats, callErr error)
	// 返回指定 cgroup 的统计与网络使用量。
	GetCgroupStats(ctx context.Context, cgroupName string, updateStats bool) (*statsapi.ContainerStats, *statsapi.NetworkStats, error)
	// 返回指定 cgroup 的 CPU 与内存统计。
	GetCgroupCPUAndMemoryStats(cgroupName string, updateStats bool) (*statsapi.ContainerStats, error)

	// 返回节点根文件系统的统计。
	RootFsStats() (*statsapi.FsStats, error)

	// 返回指定容器及 cAdvisor 选项对应的容器信息。
	GetRequestedContainersInfo(containerName string, options cadvisorapi.RequestOptions) (map[string]*cadvisorapi.ContainerInfo, error)

	// 返回指定命名空间中 Pod 的 spec。
	GetPodByName(namespace, name string) (*v1.Pod, bool)
	// 返回本地节点的 spec。
	GetNode(context.Context) (*v1.Node, error)
	// 返回本地节点配置。
	GetNodeConfig() cm.NodeConfig
	// 返回指定 Pod 使用的卷统计。
	ListVolumesForPod(podUID types.UID) (map[string]volume.Volume, bool)
	// 返回指定 Pod 使用的块设备卷统计。
	ListBlockVolumesForPod(podUID types.UID) (map[string]volume.BlockVolume, bool)
	// 返回本节点上所有 Pod 的 spec。
	GetPods() []*v1.Pod

	// 返回系统 rlimit 统计。
	RlimitStats() (*statsapi.RlimitStats, error)

	// 返回包含所有 Pod 的 cgroupfs 字面值。
	GetPodCgroupRoot() string

	// 返回映射到指定 cgroup 字面值的 Pod 及是否找到。
	GetPodByCgroupfs(cgroupfs string) (*v1.Pod, bool)
}

// handler 是 stats 处理器，依赖 Provider 与 SummaryProvider。
type handler struct {
	// provider 提供节点、Pod 与卷的原始统计。
	provider Provider
	// summaryProvider 生成对外返回的统计摘要。
	summaryProvider SummaryProvider
}

// CreateHandlers 创建 stats 的 REST handler。
func CreateHandlers(rootPath string, provider Provider, summaryProvider SummaryProvider) *restful.WebService {
	h := &handler{provider, summaryProvider}

	// 注册 /summary 端点，同时支持 GET 与 POST。
	ws := &restful.WebService{}
	ws.Path(rootPath).
		Produces(restful.MIME_JSON)

	endpoints := []struct {
		path    string
		handler restful.RouteFunction
	}{
		{"/summary", h.handleSummary},
	}

	for _, e := range endpoints {
		for _, method := range []string{"GET", "POST"} {
			ws.Route(ws.
				Method(method).
				Path(e.path).
				To(e.handler))
		}
	}

	return ws
}

// 处理 /stats/summary 请求。
// 若 GET 参数 only_cpu_and_memory 为 true，则响应只包含 CPU 与内存。
func (h *handler) handleSummary(request *restful.Request, response *restful.Response) {
	ctx := request.Request.Context()
	logger := klog.FromContext(ctx)
	// 解析 only_cpu_and_memory 参数，只有唯一值为 "true" 时才生效。
	onlyCPUAndMemory := false
	err := request.Request.ParseForm()
	if err != nil {
		handleError(logger, response, "/stats/summary", fmt.Errorf("parse form failed: %w", err))
		return
	}
	if onlyCluAndMemoryParam, found := request.Request.Form["only_cpu_and_memory"]; found &&
		len(onlyCluAndMemoryParam) == 1 && onlyCluAndMemoryParam[0] == "true" {
		onlyCPUAndMemory = true
	}
	var summary *statsapi.Summary
	if onlyCPUAndMemory {
		summary, err = h.summaryProvider.GetCPUAndMemoryStats(ctx)
	} else {
		// 外部调用 summary API 时使用缓存统计，避免频繁触发全量采集。
		forceStatsUpdate := false
		summary, err = h.summaryProvider.Get(ctx, forceStatsUpdate)
	}
	if err != nil {
		handleError(logger, response, "/stats/summary", err)
	} else {
		writeResponse(logger, response, summary)
	}
}

// writeResponse 把结果序列化为 JSON 写回响应。
func writeResponse(logger klog.Logger, response *restful.Response, stats interface{}) {
	if err := response.WriteAsJson(stats); err != nil {
		logger.Error(err, "Error writing response")
	}
}

// handleError 把错误序列化为 HTTP 响应；request 仅用于日志。
func handleError(logger klog.Logger, response *restful.Response, request string, err error) {
	// 容器不存在映射为 404，其余错误统一为 500。
	switch err {
	case kubecontainer.ErrContainerNotFound:
		response.WriteError(http.StatusNotFound, err)
	default:
		msg := fmt.Sprintf("Internal Error: %v", err)
		logger.Error(err, "HTTP InternalServerError serving", "request", request)
		response.WriteErrorString(http.StatusInternalServerError, msg)
	}
}
