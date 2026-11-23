/*
Copyright 2018 The Kubernetes Authors.

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

package custom_metrics

import (
	"fmt"
	"sync"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"

	cmint "k8s.io/metrics/pkg/apis/custom_metrics"
)

var (
	// metricVersionsToGV is the map of string group-versions
	// accepted by the converter to group-version objects (so
	// we don't have to re-parse)
	// 在 init 中一次性把已知版本字符串解析成 GroupVersion，避免每次转换重复解析。
	metricVersionsToGV map[string]schema.GroupVersion
)

func init() {
	metricVersionsToGV = make(map[string]schema.GroupVersion)
	for _, ver := range MetricVersions {
		metricVersionsToGV[ver.String()] = ver
	}
}

// NewAvailableAPIsGetter creates an AvailableAPIsGetter that checks discovery
// to find the available versions of the custom metrics api.
// 版本从服务端发现而非写死，集群升级后客户端能自动跟上；结果由 getter 自己缓存。
func NewAvailableAPIsGetter(client discovery.DiscoveryInterface) AvailableAPIsGetter {
	return &apiVersionsFromDiscovery{
		client: client,
	}
}

// apiVersionsFromDiscovery caches a preferred version of the custom metrics api.
// 发现请求代价不低，版本也很少变化；只需缓存最终选出的那一个版本。
type apiVersionsFromDiscovery struct {
	client discovery.DiscoveryInterface

	// just cache the group directly since the discovery interface doesn't yet allow
	// asking for a single API group's versions.
	// 接口目前只能列出全部 API 组，因此先保存整组信息再自行筛选。
	prefVersion *schema.GroupVersion
	mu          sync.RWMutex
}

// fetchVersions fetches the versions, but doesn't try to invalidate on cache misses.
// 刷新由 Invalidate 显式触发，避免每次读取都访问发现接口。
func (d *apiVersionsFromDiscovery) fetchVersions() (*metav1.APIGroup, error) {
	// TODO(directxman12): amend the discovery interface to ask for a particular group (/apis/foo)
	// 在接口支持按组查询之前，只能遍历 ServerGroups 的全量结果再筛选目标组。
	groups, err := d.client.ServerGroups()
	if err != nil {
		return nil, err
	}

	// Determine the preferred version on the server by first finding the custom metrics group
	// 先定位目标组再挑版本；组不存在时直接报错，不猜一个版本继续跑。
	var apiGroup *metav1.APIGroup
	for _, group := range groups.Groups {
		if group.Name == cmint.GroupName {
			apiGroup = &group
			break
		}
	}

	if apiGroup == nil {
		return nil, fmt.Errorf("no custom metrics API (%s) registered", cmint.GroupName)
	}

	return apiGroup, nil
}

// chooseVersion sets a preferred version of the custom metrics api based on available versions.
// 服务端首选版本若不在本客户端支持的集合里，就退到第一个已知版本，保证仍能工作。
func (d *apiVersionsFromDiscovery) chooseVersion(apiGroup *metav1.APIGroup) (schema.GroupVersion, error) {
	var preferredVersion *schema.GroupVersion
	if gv, present := metricVersionsToGV[apiGroup.PreferredVersion.GroupVersion]; present && len(apiGroup.PreferredVersion.GroupVersion) != 0 {
		preferredVersion = &gv
	} else {
		for _, version := range apiGroup.Versions {
			if gv, present := metricVersionsToGV[version.GroupVersion]; present {
				preferredVersion = &gv
				break
			}
		}
	}

	if preferredVersion == nil {
		return schema.GroupVersion{}, fmt.Errorf("no known available metric versions found")
	}
	return *preferredVersion, nil
}

// PreferredVersion returns the current preferred version of the custom metrics api.
// If none is specified, it will use the first known one.
// 用双重检查加锁：并发调用同时发现缓存为空时，只有一个会真正去查发现接口。
func (d *apiVersionsFromDiscovery) PreferredVersion() (schema.GroupVersion, error) {
	d.mu.RLock()
	if d.prefVersion != nil {
		// if we've already got one, proceed with that
		defer d.mu.RUnlock()
		return *d.prefVersion, nil
	}
	d.mu.RUnlock()

	d.mu.Lock()
	defer d.mu.Unlock()

	// double check, someone might have beaten us to it
	// 写锁内再判一次，免得别人已经填好缓存后重复发现。
	if d.prefVersion != nil {
		return *d.prefVersion, nil
	}

	// populate our cache
	// 只缓存成功结果；失败留到下次调用重试，避免把瞬时错误固化。
	groupInfo, err := d.fetchVersions()
	if err != nil {
		return schema.GroupVersion{}, err
	}
	prefVersion, err := d.chooseVersion(groupInfo)
	if err != nil {
		return schema.GroupVersion{}, err
	}

	d.prefVersion = &prefVersion
	return *d.prefVersion, nil
}

// Invalidate refreshes the preferred version information.
// 只清缓存，真正的重新发现推迟到下一次 PreferredVersion。
func (d *apiVersionsFromDiscovery) Invalidate() {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.prefVersion = nil
}
