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

package kubelet

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/kubernetes/pkg/kubelet/podcertificate"
	"k8s.io/kubernetes/test/utils/ktesting"
)

// recordingPodCertificateManager 记录最近一次凭据请求的参数，用于验证参数顺序。
type recordingPodCertificateManager struct {
	// Namespace 是记录的命名空间。
	Namespace string
	// PodName 是记录的 Pod 名称。
	PodName string
	// PodUID 是记录的 Pod UID。
	PodUID string
	// VolumeName 是记录的卷名称。
	VolumeName string
	// SourceIndex 是记录的卷源索引。
	SourceIndex int
}

// GetPodCertificateCredentialBundle 记录参数并返回空结果。
func (f *recordingPodCertificateManager) GetPodCertificateCredentialBundle(ctx context.Context, namespace, podName, podUID, volumeName string, sourceIndex int) ([]byte, []byte, error) {
	f.Namespace = namespace
	f.PodName = podName
	f.PodUID = podUID
	f.VolumeName = volumeName
	f.SourceIndex = sourceIndex

	return nil, nil, nil
}

// TrackPod 是空实现。
func (f *recordingPodCertificateManager) TrackPod(ctx context.Context, pod *corev1.Pod) {}

// ForgetPod 是空实现。
func (f *recordingPodCertificateManager) ForgetPod(ctx context.Context, pod *corev1.Pod) {}

// MetricReport 返回空的指标报告。
func (f *recordingPodCertificateManager) MetricReport() *podcertificate.MetricReport {
	return &podcertificate.MetricReport{}
}

// 验证 GetPodCertificateCredentialBundle 按正确顺序转发参数。
// 看似多余，但此前确实出现过参数顺序写反的问题。
func TestGetPodCertificateCredentialBundle(t *testing.T) {
	tCtx := ktesting.Init(t)
	// 用记录参数的管理器验证参数传递顺序。
	recorder := &recordingPodCertificateManager{}

	kvh := &kubeletVolumeHost{
		podCertificateManager: recorder,
	}

	_, _, err := kvh.GetPodCertificateCredentialBundle(tCtx, "namespace", "pod-name", "pod-uid", "volume-name", 10)
	if err != nil {
		t.Fatalf("Unexpected error calling GetPodCertificateCredentialBundle: %v", err)
	}

	// 期望参数按 (namespace, podName, podUID, volumeName, sourceIndex) 顺序传入。
	want := &recordingPodCertificateManager{
		Namespace:   "namespace",
		PodName:     "pod-name",
		PodUID:      "pod-uid",
		VolumeName:  "volume-name",
		SourceIndex: 10,
	}

	if diff := cmp.Diff(recorder, want); diff != "" {
		t.Errorf("Wrong input to GetPodCertificateCredentialBundle; diff (-got +want)\n%s", diff)
	}
}
