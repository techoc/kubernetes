/*
Copyright 2015 The Kubernetes Authors.

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

package scheduler

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/apimachinery/pkg/util/sets"
	restclient "k8s.io/client-go/rest"
	extenderv1 "k8s.io/kube-scheduler/extender/v1"
	fwk "k8s.io/kube-scheduler/framework"
	schedulerapi "k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

const (
	// DefaultExtenderTimeout 定义 extender 的默认超时时间（秒）。
	DefaultExtenderTimeout = 5 * time.Second
)

// HTTPExtender 是 Extender 接口的 HTTP 实现，通过调用外部服务参与过滤、打分、抢占与绑定。
type HTTPExtender struct {
	extenderURL      string
	preemptVerb      string
	filterVerb       string
	prioritizeVerb   string
	bindVerb         string
	weight           int64
	client           *http.Client
	nodeCacheCapable bool
	managedResources sets.Set[string]
	ignorable        bool
}

// makeTransport 根据 extender 配置构造 HTTP 传输层，必要时附带 TLS 设置。
func makeTransport(config *schedulerapi.Extender) (http.RoundTripper, error) {
	var cfg restclient.Config
	if config.TLSConfig != nil {
		cfg.TLSClientConfig.Insecure = config.TLSConfig.Insecure
		cfg.TLSClientConfig.ServerName = config.TLSConfig.ServerName
		cfg.TLSClientConfig.CertFile = config.TLSConfig.CertFile
		cfg.TLSClientConfig.KeyFile = config.TLSConfig.KeyFile
		cfg.TLSClientConfig.CAFile = config.TLSConfig.CAFile
		cfg.TLSClientConfig.CertData = config.TLSConfig.CertData
		cfg.TLSClientConfig.KeyData = config.TLSConfig.KeyData
		cfg.TLSClientConfig.CAData = config.TLSConfig.CAData
	}
	if config.EnableHTTPS {
		hasCA := len(cfg.CAFile) > 0 || len(cfg.CAData) > 0
		// 未配置 CA 时若强制校验证书将无法建立 HTTPS 连接，此处退化为不校验。
		if !hasCA {
			cfg.Insecure = true
		}
	}
	tlsConfig, err := restclient.TLSConfigFor(&cfg)
	if err != nil {
		return nil, err
	}
	if tlsConfig != nil {
		return utilnet.SetTransportDefaults(&http.Transport{
			TLSClientConfig: tlsConfig,
		}), nil
	}
	return utilnet.SetTransportDefaults(&http.Transport{}), nil
}

// NewHTTPExtender 根据配置创建一个 HTTPExtender。
func NewHTTPExtender(config *schedulerapi.Extender) (fwk.Extender, error) {
	// 未配置超时时补上默认值，避免请求无限期挂起。
	if config.HTTPTimeout.Duration.Nanoseconds() == 0 {
		config.HTTPTimeout.Duration = time.Duration(DefaultExtenderTimeout)
	}

	transport, err := makeTransport(config)
	if err != nil {
		return nil, err
	}
	client := &http.Client{
		Transport: transport,
		Timeout:   config.HTTPTimeout.Duration,
	}
	managedResources := sets.New[string]()
	for _, r := range config.ManagedResources {
		managedResources.Insert(string(r.Name))
	}
	return &HTTPExtender{
		extenderURL:      config.URLPrefix,
		preemptVerb:      config.PreemptVerb,
		filterVerb:       config.FilterVerb,
		prioritizeVerb:   config.PrioritizeVerb,
		bindVerb:         config.BindVerb,
		weight:           config.Weight,
		client:           client,
		nodeCacheCapable: config.NodeCacheCapable,
		managedResources: managedResources,
		ignorable:        config.Ignorable,
	}, nil
}

// Name 返回 extenderURL，作为该 extender 的标识。
func (h *HTTPExtender) Name() string {
	return h.extenderURL
}

// IsIgnorable 返回 true 表示此 extender 不可用时不应导致调度失败。
func (h *HTTPExtender) IsIgnorable() bool {
	return h.ignorable
}

// SupportsPreemption 返回 extender 是否支持抢占。
// extender 应定义 preempt verb 并启用自身的节点缓存。
func (h *HTTPExtender) SupportsPreemption() bool {
	return len(h.preemptVerb) > 0
}

// ProcessPreemption 调用 extender 的抢占逻辑，返回筛选后的候选节点及对应的牺牲者。
func (h *HTTPExtender) ProcessPreemption(
	pod *v1.Pod,
	nodeNameToVictims map[string]*extenderv1.Victims,
	nodeInfos fwk.NodeInfoLister,
) (map[string]*extenderv1.Victims, error) {
	var (
		result extenderv1.ExtenderPreemptionResult
		args   *extenderv1.ExtenderPreemptionArgs
	)

	if !h.SupportsPreemption() {
		return nil, fmt.Errorf("preempt verb is not defined for extender %v but run into ProcessPreemption", h.extenderURL)
	}

	if h.nodeCacheCapable {
		// extender 自带节点缓存时只传 UID 等精简信息，避免传输完整 Pod 对象。
		nodeNameToMetaVictims := convertToMetaVictims(nodeNameToVictims)
		args = &extenderv1.ExtenderPreemptionArgs{
			Pod:                   pod,
			NodeNameToMetaVictims: nodeNameToMetaVictims,
		}
	} else {
		args = &extenderv1.ExtenderPreemptionArgs{
			Pod:               pod,
			NodeNameToVictims: nodeNameToVictims,
		}
	}

	if err := h.send(h.preemptVerb, args, &result); err != nil {
		return nil, err
	}

	// extender 总是返回 NodeNameToMetaVictims，需用 nodeInfos 还原成 NodeNameToVictims。
	newNodeNameToVictims, err := h.convertToVictims(result.NodeNameToMetaVictims, nodeInfos)
	if err != nil {
		return nil, err
	}
	// 返回新结果，不修改入参 nodeNameToVictims。
	return newNodeNameToVictims, nil
}

// convertToVictims 把 nodeNameToMetaVictims 中的 UID 等对象标识还原为对象指针。
func (h *HTTPExtender) convertToVictims(
	nodeNameToMetaVictims map[string]*extenderv1.MetaVictims,
	nodeInfos fwk.NodeInfoLister,
) (map[string]*extenderv1.Victims, error) {
	nodeNameToVictims := map[string]*extenderv1.Victims{}
	for nodeName, metaVictims := range nodeNameToMetaVictims {
		nodeInfo, err := nodeInfos.Get(nodeName)
		if err != nil {
			return nil, err
		}
		victims := &extenderv1.Victims{
			Pods:             []*v1.Pod{},
			NumPDBViolations: metaVictims.NumPDBViolations,
		}
		for _, metaPod := range metaVictims.Pods {
			pod, err := h.convertPodUIDToPod(metaPod, nodeInfo)
			if err != nil {
				return nil, err
			}
			victims.Pods = append(victims.Pods, pod)
		}
		nodeNameToVictims[nodeName] = victims
	}
	return nodeNameToVictims, nil
}

// convertPodUIDToPod 根据 MetaPod 与节点信息从 nodeInfo.GetPods() 中还原出 Pod 对象。
// 若默认调度器与 extender 缓存不一致（在节点上找不到该 Pod），则返回错误。
func (h *HTTPExtender) convertPodUIDToPod(
	metaPod *extenderv1.MetaPod,
	nodeInfo fwk.NodeInfo) (*v1.Pod, error) {
	for _, p := range nodeInfo.GetPods() {
		if string(p.GetPod().UID) == metaPod.UID {
			return p.GetPod(), nil
		}
	}
	return nil, fmt.Errorf("extender: %v claims to preempt pod (UID: %v) on node: %v, but the pod is not found on that node",
		h.extenderURL, metaPod, nodeInfo.Node().Name)
}

// convertToMetaVictims 把 Victims 转换为只含 UID 的 MetaVictims，便于轻量传输。
func convertToMetaVictims(
	nodeNameToVictims map[string]*extenderv1.Victims,
) map[string]*extenderv1.MetaVictims {
	nodeNameToMetaVictims := map[string]*extenderv1.MetaVictims{}
	for node, victims := range nodeNameToVictims {
		metaVictims := &extenderv1.MetaVictims{
			Pods:             []*extenderv1.MetaPod{},
			NumPDBViolations: victims.NumPDBViolations,
		}
		for _, pod := range victims.Pods {
			metaPod := &extenderv1.MetaPod{
				UID: string(pod.UID),
			}
			metaVictims.Pods = append(metaVictims.Pods, metaPod)
		}
		nodeNameToMetaVictims[node] = metaVictims
	}
	return nodeNameToMetaVictims
}

// Filter 调用 extender 的过滤逻辑。返回的节点列表应为输入列表的子集，否则报错。
// failedNodes 与 failedAndUnresolvableNodes 分别携带失败节点及原因，
// 后者中的节点被判定为不可恢复。
func (h *HTTPExtender) Filter(
	pod *v1.Pod,
	nodes []fwk.NodeInfo,
) (filteredList []fwk.NodeInfo, failedNodes, failedAndUnresolvableNodes extenderv1.FailedNodesMap, err error) {
	var (
		result     extenderv1.ExtenderFilterResult
		nodeList   *v1.NodeList
		nodeNames  *[]string
		nodeResult []fwk.NodeInfo
		args       *extenderv1.ExtenderArgs
	)
	// 建立名称到节点信息的索引，便于把 extender 返回的节点名还原为 NodeInfo。
	fromNodeName := make(map[string]fwk.NodeInfo)
	for _, n := range nodes {
		fromNodeName[n.Node().Name] = n
	}

	if h.filterVerb == "" {
		return nodes, extenderv1.FailedNodesMap{}, extenderv1.FailedNodesMap{}, nil
	}

	if h.nodeCacheCapable {
		nodeNameSlice := make([]string, 0, len(nodes))
		for _, node := range nodes {
			nodeNameSlice = append(nodeNameSlice, node.Node().Name)
		}
		nodeNames = &nodeNameSlice
	} else {
		nodeList = &v1.NodeList{}
		for _, node := range nodes {
			nodeList.Items = append(nodeList.Items, *node.Node())
		}
	}

	args = &extenderv1.ExtenderArgs{
		Pod:       pod,
		Nodes:     nodeList,
		NodeNames: nodeNames,
	}

	if err := h.send(h.filterVerb, args, &result); err != nil {
		return nil, nil, nil, err
	}
	if result.Error != "" {
		return nil, nil, nil, errors.New(result.Error)
	}

	if h.nodeCacheCapable && result.NodeNames != nil {
		nodeResult = make([]fwk.NodeInfo, len(*result.NodeNames))
		for i, nodeName := range *result.NodeNames {
			if n, ok := fromNodeName[nodeName]; ok {
				nodeResult[i] = n
			} else {
				// extender 返回了输入中不存在的节点，说明缓存不一致，视为致命错误。
				return nil, nil, nil, fmt.Errorf(
					"extender %q claims a filtered node %q which is not found in the input node list",
					h.extenderURL, nodeName)
			}
		}
	} else if result.Nodes != nil {
		nodeResult = make([]fwk.NodeInfo, len(result.Nodes.Items))
		for i := range result.Nodes.Items {
			nodeResult[i] = framework.NewNodeInfo()
			nodeResult[i].SetNode(&result.Nodes.Items[i])
		}
	}

	return nodeResult, result.FailedNodes, result.FailedAndUnresolvableNodes, nil
}

// Prioritize 调用 extender 的优先级函数，返回各节点得分及其权重。
// 每项得分为 priority*weight，会累加到 Kubernetes 调度器算出的分数上，
// 最终以总分决定主机选择。
func (h *HTTPExtender) Prioritize(pod *v1.Pod, nodes []fwk.NodeInfo) (*extenderv1.HostPriorityList, int64, error) {
	var (
		result    extenderv1.HostPriorityList
		nodeList  *v1.NodeList
		nodeNames *[]string
		args      *extenderv1.ExtenderArgs
	)

	if h.prioritizeVerb == "" {
		result := extenderv1.HostPriorityList{}
		for _, node := range nodes {
			result = append(result, extenderv1.HostPriority{Host: node.Node().Name, Score: 0})
		}
		return &result, 0, nil
	}

	if h.nodeCacheCapable {
		nodeNameSlice := make([]string, 0, len(nodes))
		for _, node := range nodes {
			nodeNameSlice = append(nodeNameSlice, node.Node().Name)
		}
		nodeNames = &nodeNameSlice
	} else {
		nodeList = &v1.NodeList{}
		for _, node := range nodes {
			nodeList.Items = append(nodeList.Items, *node.Node())
		}
	}

	args = &extenderv1.ExtenderArgs{
		Pod:       pod,
		Nodes:     nodeList,
		NodeNames: nodeNames,
	}

	if err := h.send(h.prioritizeVerb, args, &result); err != nil {
		return nil, 0, err
	}
	return &result, h.weight, nil
}

// Bind 把 Pod 与节点的绑定动作委托给 extender 执行。
func (h *HTTPExtender) Bind(binding *v1.Binding) error {
	var result extenderv1.ExtenderBindingResult
	if !h.IsBinder() {
		// 未配置 bindVerb 的 extender 不会被选作 Binder，正常路径不会走到这里。
		return fmt.Errorf("unexpected empty bindVerb in extender")
	}
	req := &extenderv1.ExtenderBindingArgs{
		PodName:      binding.Name,
		PodNamespace: binding.Namespace,
		PodUID:       binding.UID,
		Node:         binding.Target.Name,
	}
	if err := h.send(h.bindVerb, req, &result); err != nil {
		return err
	}
	if result.Error != "" {
		return errors.New(result.Error)
	}
	return nil
}

// IsBinder 返回此 extender 是否配置了 Bind 动作。
func (h *HTTPExtender) IsBinder() bool {
	return h.bindVerb != ""
}

// IsPrioritizer 返回此 extender 是否配置了 Prioritize 动作。
func (h *HTTPExtender) IsPrioritizer() bool {
	return h.prioritizeVerb != ""
}

// IsFilter 返回此 extender 是否配置了 Filter 动作。
func (h *HTTPExtender) IsFilter() bool {
	return h.filterVerb != ""
}

// send 向 extender 发送 POST 请求，并把响应 JSON 解码到 result。
func (h *HTTPExtender) send(action string, args interface{}, result interface{}) error {
	out, err := json.Marshal(args)
	if err != nil {
		return err
	}

	url := strings.TrimRight(h.extenderURL, "/") + "/" + action

	req, err := http.NewRequest("POST", url, bytes.NewReader(out))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := h.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed %v with extender at URL %v, code %v", action, url, resp.StatusCode)
	}

	return json.NewDecoder(resp.Body).Decode(result)
}

// IsInterested 返回 Pod 请求的资源中是否至少有一种由该 extender 管理。
// 未配置托管资源时默认对所有 Pod 感兴趣。
func (h *HTTPExtender) IsInterested(pod *v1.Pod) bool {
	if h.managedResources.Len() == 0 {
		return true
	}
	if h.hasManagedResources(pod.Spec.Containers) {
		return true
	}
	if h.hasManagedResources(pod.Spec.InitContainers) {
		return true
	}
	return false
}

// hasManagedResources 检查容器 requests/limits 中是否出现 extender 托管的资源。
func (h *HTTPExtender) hasManagedResources(containers []v1.Container) bool {
	for i := range containers {
		container := &containers[i]
		for resourceName := range container.Resources.Requests {
			if h.managedResources.Has(string(resourceName)) {
				return true
			}
		}
		for resourceName := range container.Resources.Limits {
			if h.managedResources.Has(string(resourceName)) {
				return true
			}
		}
	}
	return false
}
