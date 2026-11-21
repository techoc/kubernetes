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

package pullmanager

import (
	"context"
	"time"

	"k8s.io/klog/v2"
	kubeletconfiginternal "k8s.io/kubernetes/pkg/kubelet/apis/config"
)

var _ ImagePullManager = &NoopImagePullManager{}

// NoopImagePullManager 是不做任何拉取记录管理的空实现，用于未启用相关特性时。
type NoopImagePullManager struct{}

// RecordPullIntent 为空实现，不记录拉取意图。
func (m *NoopImagePullManager) RecordPullIntent(klog.Logger, string) error { return nil }

// RecordImagePulled 为空实现，不记录镜像拉取成功。
func (m *NoopImagePullManager) RecordImagePulled(context.Context, string, string, *kubeletconfiginternal.ImagePullCredentials) {
}

// RecordImagePullFailed 为空实现，不记录镜像拉取失败。
func (m *NoopImagePullManager) RecordImagePullFailed(context.Context, string) {}

// MustAttemptImagePull 始终返回 false，表示由调用方照常尝试拉取。
func (m *NoopImagePullManager) MustAttemptImagePull(context.Context, string, string, GetPodCredentials) (bool, error) {
	return false, nil
}

// PruneUnknownRecords 为空实现，不清理任何记录。
func (m *NoopImagePullManager) PruneUnknownRecords(context.Context, []string, time.Time) {}
