/*
Copyright 2024 The Kubernetes Authors.

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

package utils

import (
	"context"
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/storage/names"
	"k8s.io/client-go/dynamic"
	"k8s.io/kubernetes/test/e2e/framework"
)

const (
	// VolumeGroupSnapshot 是组快照 API
	VolumeGroupSnapshotAPIGroup = "groupsnapshot.storage.k8s.io"
	// VolumeGroupSnapshotAPIVersion 是组快照 API 版本
	VolumeGroupSnapshotAPIVersion = "groupsnapshot.storage.k8s.io/v1"
)

var (

	// VolumeGroupSnapshotGVR 是 volumegroupsnapshots 的 GroupVersionResource
	VolumeGroupSnapshotGVR = schema.GroupVersionResource{Group: VolumeGroupSnapshotAPIGroup, Version: "v1", Resource: "volumegroupsnapshots"}
	// VolumeGroupSnapshotClassGVR 是 volumegroupsnapshotsclasses 的 GroupVersionResource
	VolumeGroupSnapshotClassGVR   = schema.GroupVersionResource{Group: VolumeGroupSnapshotAPIGroup, Version: "v1", Resource: "volumegroupsnapshotclasses"}
	VolumeGroupSnapshotContentGVR = schema.GroupVersionResource{Group: VolumeGroupSnapshotAPIGroup, Version: "v1", Resource: "volumegroupsnapshotcontents"}
)

// WaitForVolumeGroupSnapshotReady 等待 VolumeGroupSnapshot 可用或超时，以先发生者为准。
func WaitForVolumeGroupSnapshotReady(ctx context.Context, c dynamic.Interface, ns string, volumeGroupSnapshotName string, poll, timeout time.Duration) error {
	framework.Logf("Waiting up to %v for VolumeGroupSnapshot %s to become ready", timeout, volumeGroupSnapshotName)

	if successful := WaitUntil(poll, timeout, func() bool {
		volumeGroupSnapshot, err := c.Resource(VolumeGroupSnapshotGVR).Namespace(ns).Get(ctx, volumeGroupSnapshotName, metav1.GetOptions{})
		if err != nil {
			framework.Logf("Failed to get group snapshot %q, retrying in %v. Error: %v", volumeGroupSnapshotName, poll, err)
			return false
		}

		status := volumeGroupSnapshot.Object["status"]
		if status == nil {
			framework.Logf("VolumeGroupSnapshot %s found but is not ready.", volumeGroupSnapshotName)
			return false
		}
		value := status.(map[string]interface{})
		if value["readyToUse"] == true {
			framework.Logf("VolumeSnapshot %s found and is ready", volumeGroupSnapshotName)
			return true
		}

		framework.Logf("VolumeSnapshot %s found but is not ready.", volumeGroupSnapshotName)
		return false
	}); successful {
		return nil
	}

	return fmt.Errorf("VolumeSnapshot %s is not ready within %v", volumeGroupSnapshotName, timeout)
}

func GenerateVolumeGroupSnapshotClassSpec(
	snapshotter string,
	parameters map[string]string,
	ns string,
) *unstructured.Unstructured {
	deletionPolicy, ok := parameters["deletionPolicy"]
	if !ok {
		deletionPolicy = "Delete"
	}
	volumeGroupSnapshotClass := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"kind":       "VolumeGroupSnapshotClass",
			"apiVersion": VolumeGroupSnapshotAPIVersion,
			"metadata": map[string]interface{}{
				// 名称必须唯一，因此基于命名空间名称并使用 GenerateName
				"name": names.SimpleNameGenerator.GenerateName(ns),
			},
			"driver":         snapshotter,
			"parameters":     parameters,
			"deletionPolicy": deletionPolicy,
		},
	}

	return volumeGroupSnapshotClass
}
