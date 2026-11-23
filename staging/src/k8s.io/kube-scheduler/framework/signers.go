/*
Copyright 2025 The Kubernetes Authors.

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

package framework

import (
	"encoding/json"
	"slices"
	"sort"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
)

// This file contains the names and implementations of various functions used
// to compute pod signatures for use in batching and other scheduling optimizations.
// See the definition of BatchablePlugin for more details.
//
// 签名用于把“对调度结果影响相同”的 Pod 归为一批，让插件只算一次就能复用结果。
// 所以这里的统一动作是排序和去重：字段顺序不同但语义相同的 Pod 必须给出同一签名，
// 否则批处理命中率为零，优化反而变成额外开销。

// Signer names
const (
	DynamicResourcesSignerName = "v1.Pod.Spec.DynamicResources"
	ImageNamesSignerName       = "v1.Pod.Spec.CanonicalImageNames()"
	LabelsSignerName           = "v1.Pod.Labels"
	NodeNameSignerName         = "v1.Pod.Spec.NodeName"
	NodeAffinitySignerName     = "v1.Pod.Spec.Affinity.NodeAffinity"
	NodeSelectorSignerName     = "v1.Pod.Spec.Affinity.NodeSelector"
	HostPortsSignerName        = "v1.Pod.Spec.HostPorts()"
	ResourcesSignerName        = "v1.Pod.Spec.ContainerRequestsAndOverheads()"
	SchedulerNameSignerName    = "v1.Pod.Spec.SchedulerName"
	TolerationsSignerName      = "v1.Pod.Spec.Tolerations"
	VolumesSignerName          = "v1.Pod.Spec.Volumes.NonSyntheticSources()"
	FeaturesSignerName         = "v1.Pod.Spec.RequiredFeatures()"
)

// Common signers. These are either generic or shared across plugins.

// HostPortsSigner 只关心真正占用宿主机端口的值：HostPort 为 0 只是声明容器端口，
// 不会引发端口冲突，计入反而会把本可同批的 Pod 拆开。
func HostPortsSigner(pod *v1.Pod) any {
	portSet := sets.New[int32]()
	containers := []v1.Container{}
	containers = append(containers, pod.Spec.Containers...)
	containers = append(containers, pod.Spec.InitContainers...)
	for _, container := range containers {
		for _, port := range container.Ports {
			if port.HostPort != 0 {
				portSet.Insert(port.HostPort)
			}
		}
	}
	ports := portSet.UnsortedList()
	slices.Sort(ports)
	return ports
}

// NodeSelectorRequirementsSigner 把每条需求转成字符串再整体排序。
// Values 的书写顺序不影响匹配语义，先排序再序列化才能避免同一条件生成不同签名。
func NodeSelectorRequirementsSigner(reqs []v1.NodeSelectorRequirement) ([]string, error) {
	ret := make([]string, len(reqs))
	for i, req := range reqs {
		t := req.DeepCopy()
		slices.Sort(t.Values)
		v, err := json.Marshal(t)
		if err != nil {
			return nil, err
		}
		ret[i] = string(v)
	}
	slices.Sort(ret)
	return ret, nil
}

type nodeSelTermSignResult struct {
	MatchExpressions []string
	MatchFields      []string
}

// NodeSelectorTermSigner 分别规范化 MatchExpressions 和 MatchFields。
// 两组条件是“且”的关系，但组内无序，所以各自独立排序即可。
func NodeSelectorTermSigner(t *v1.NodeSelectorTerm) (nodeSelTermSignResult, error) {
	exp, err := NodeSelectorRequirementsSigner(t.MatchExpressions)
	if err != nil {
		return nodeSelTermSignResult{}, err
	}
	fld, err := NodeSelectorRequirementsSigner(t.MatchFields)
	if err != nil {
		return nodeSelTermSignResult{}, err
	}
	return nodeSelTermSignResult{
		MatchExpressions: exp,
		MatchFields:      fld,
	}, nil
}

type prefSchedTermSignResult struct {
	Weight     int32
	Preference nodeSelTermSignResult
}

// PreferredSchedulingTermSigner 把权重和偏好一起编码：偏好相同但权重不同的两条
// 打分项会产生不同结果，必须算作不同签名。
func PreferredSchedulingTermSigner(terms []v1.PreferredSchedulingTerm) ([]string, error) {
	newTerms := make([]string, len(terms))
	for i, t := range terms {
		pref, err := NodeSelectorTermSigner(&t.Preference)
		if err != nil {
			return nil, err
		}
		termStr, err := json.Marshal(prefSchedTermSignResult{
			Weight:     t.Weight,
			Preference: pref,
		})
		if err != nil {
			return nil, err
		}
		newTerms[i] = string(termStr)
	}
	slices.Sort(newTerms)
	return newTerms, nil
}

// NodeSelectorTermsSigner 处理硬亲和性里的 term 列表。
// 这些 term 是“或”的关系，顺序不影响结果，整体排序后签名才稳定。
func NodeSelectorTermsSigner(terms []v1.NodeSelectorTerm) ([]string, error) {
	req := make([]string, len(terms))
	for i, t := range terms {
		nst, err := NodeSelectorTermSigner(&t)
		if err != nil {
			return nil, err
		}
		tStr, err := json.Marshal(nst)
		if err != nil {
			return nil, err
		}
		req[i] = string(tStr)
	}
	slices.Sort(req)

	return req, nil
}

type nodeAffinitySignerResult struct {
	Required  []string
	Preferred []string
}

// NodeAffinitySigner 把硬条件和软条件分开。
// 没有亲和性时返回 nil：这类 Pod 的签名里不该掺进一个空结构，
// 否则会和有亲和性但内容为空的 Pod 混在一起。
func NodeAffinitySigner(pod *v1.Pod) (any, error) {
	if pod.Spec.Affinity != nil {
		if pod.Spec.Affinity.NodeAffinity != nil {
			n := pod.Spec.Affinity.NodeAffinity
			pref := []string{}
			var err error
			if n.PreferredDuringSchedulingIgnoredDuringExecution != nil {
				pref, err = PreferredSchedulingTermSigner(n.PreferredDuringSchedulingIgnoredDuringExecution)
				if err != nil {
					return nil, err
				}
			}

			req := []string{}
			if n.RequiredDuringSchedulingIgnoredDuringExecution != nil {
				req, err = NodeSelectorTermsSigner(n.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms)
				if err != nil {
					return nil, err
				}
			}

			return nodeAffinitySignerResult{
				Required:  req,
				Preferred: pref,
			}, nil
		}
	}
	return nil, nil
}

// TolerationsSigner 按 Key 和 Value 排序，副本不去重：
// 重复容忍在语义上无害，但保留原样能让签名直接反映用户提交的 spec，
// 省掉对 Effect、Operator 等字段是否等价的判断。
func TolerationsSigner(pod *v1.Pod) any {
	ret := []v1.Toleration{}
	ret = append(ret, pod.Spec.Tolerations...)
	sort.Slice(ret, func(i, j int) bool {
		return ret[i].Key < ret[j].Key || (ret[i].Key == ret[j].Key && ret[i].Value < ret[j].Value)
	})
	return ret
}

// We special case volumes because config and secret volumes don't
// impact scheduling but are very specific to individual pods. If we
// don't exclude them no pods will have matching signatures.
//
// 其余卷类型影响调度（比如 PVC 的 topology），必须留在签名里；
// 排序是必要的，因为卷在 spec 里的书写顺序没有语义。
func VolumesSigner(pod *v1.Pod) any {
	ret := []string{}
	for _, vol := range pod.Spec.Volumes {
		if vol.VolumeSource.ConfigMap == nil && vol.VolumeSource.Secret == nil {
			volStr, err := json.Marshal(vol.VolumeSource)
			if err != nil {
				return nil
			}
			ret = append(ret, string(volStr))
		}
	}
	slices.Sort(ret)
	return ret
}
