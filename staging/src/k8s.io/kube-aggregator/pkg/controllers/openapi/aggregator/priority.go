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

package aggregator

import (
	"sort"

	apiregistrationv1 "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"
)

// byPriority can be used in sort.Sort to sort specs with their priorities.
//
// 中文补充：OpenAPI 合并时，后合并的规范会覆盖先合并的同名路径/定义。
// 排序因此决定了"谁的最后一句话说了算"：本地（delegate）放最后才能覆盖远端，
// 远端之间再按优先级，低优先级的先合并、高优先级的后合并。
type byPriority struct {
	apiServices     []*apiregistrationv1.APIService
	groupPriorities map[string]int32
}

func (a byPriority) Len() int { return len(a.apiServices) }
func (a byPriority) Swap(i, j int) {
	a.apiServices[i], a.apiServices[j] = a.apiServices[j], a.apiServices[i]
}
func (a byPriority) Less(i, j int) bool {
	// All local specs will come first
	// 中文补充：local 与远端分两段，是为了让本地规范排在序列末尾、最后参与合并，
	// 从而保住本地定义对聚合结果的最终话语权。
	if a.apiServices[i].Spec.Service == nil && a.apiServices[j].Spec.Service != nil {
		return true
	}
	if a.apiServices[i].Spec.Service != nil && a.apiServices[j].Spec.Service == nil {
		return false
	}
	// WARNING: This will result in not following priorities for local APIServices.
	if a.apiServices[i].Spec.Service == nil {
		// Sort local specs with their name. This is the order in the delegation chain (aggregator first).
		return a.apiServices[i].Name < a.apiServices[j].Name
	}
	var iPriority, jPriority int32
	if a.apiServices[i].Spec.Group == a.apiServices[j].Spec.Group {
		iPriority = a.apiServices[i].Spec.VersionPriority
		jPriority = a.apiServices[i].Spec.VersionPriority
	} else {
		iPriority = a.groupPriorities[a.apiServices[i].Spec.Group]
		jPriority = a.groupPriorities[a.apiServices[j].Spec.Group]
	}
	if iPriority != jPriority {
		// Sort by priority, higher first
		return iPriority > jPriority
	}
	// Sort by service name.
	return a.apiServices[i].Name < a.apiServices[j].Name
}

func sortByPriority(apiServices []*apiregistrationv1.APIService) {
	// 中文补充：组的代表优先级取组内最高值，这样同组的多个版本在组间比较时
	// 用的是同一把尺子，不会因为某个低优先级版本把整组排到后面。
	b := byPriority{
		apiServices:     apiServices,
		groupPriorities: map[string]int32{},
	}
	for _, apiService := range apiServices {
		if apiService.Spec.Service == nil {
			continue
		}
		if pr, found := b.groupPriorities[apiService.Spec.Group]; !found || apiService.Spec.GroupPriorityMinimum > pr {
			b.groupPriorities[apiService.Spec.Group] = apiService.Spec.GroupPriorityMinimum
		}
	}
	sort.Sort(b)
}
