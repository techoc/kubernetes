//go:build !linux

/*
Copyright The Kubernetes Authors.

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

package metrics

// 非 Linux 平台没有 nftables netfilter 计数器设施，这里提供空实现，
// 使 metrics.go 中的注册逻辑在所有平台上都能编译通过。
import "k8s.io/component-base/metrics"

// newNFAcctMetricCollector 在非 Linux 平台返回 nil，表示无需注册 nfacct 指标采集器。
func newNFAcctMetricCollector(counter string, description *metrics.Desc) metrics.StableCollector {
	return nil
}

// newNFTablesCounterMetricCollector 在非 Linux 平台返回 nil，表示无需注册 nftables 命名计数器采集器。
func newNFTablesCounterMetricCollector(counters map[string]string, description *metrics.Desc) metrics.StableCollector {
	return nil
}
