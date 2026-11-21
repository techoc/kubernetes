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

package images

import (
	"strconv"
	"strings"
	"sync"

	v1 "k8s.io/api/core/v1"
	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
	kubeletmetrics "k8s.io/kubernetes/pkg/kubelet/metrics"
)

var (
	// ensureImageRequestsCounter 按拉取策略、本地是否存在、是否需要拉取三个维度计数。
	ensureImageRequestsCounter = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      kubeletmetrics.KubeletSubsystem,
			Name:           "image_manager_ensure_image_requests_total",
			Help:           "Number of ensure-image requests processed by the kubelet.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"pull_policy", "present_locally", "pull_required"},
	)
)

var once sync.Once

// registerMetrics 确保指标只注册一次。
func registerMetrics() {
	once.Do(func() {
		legacyregistry.MustRegister(ensureImageRequestsCounter)
	})
}

// recordEnsureImageRequest 记录一次 ensure-image 请求；nil 参数记为 unknown。
func recordEnsureImageRequest(pullPolicy v1.PullPolicy, imagePresentLocally, imagePullRequired *bool) {
	presentLocally := "unknown"
	if imagePresentLocally != nil {
		presentLocally = strconv.FormatBool(*imagePresentLocally)
	}
	pullRequired := "unknown"
	if imagePullRequired != nil {
		pullRequired = strconv.FormatBool(*imagePullRequired)
	}

	ensureImageRequestsCounter.WithLabelValues(
		strings.ToLower(string(pullPolicy)),
		presentLocally,
		pullRequired,
	).Inc()
}
