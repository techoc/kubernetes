/*
Copyright 2020 The Kubernetes Authors.

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

	"github.com/onsi/ginkgo/v2"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/storage/names"
	"k8s.io/client-go/dynamic"
	"k8s.io/kubernetes/test/e2e/framework"
)

const (
	// SnapshotGroup 是 snapshot CRD 的 api group
	SnapshotGroup = "snapshot.storage.k8s.io"
	// SnapshotAPIVersion 是 snapshot CRD 的 api version
	SnapshotAPIVersion = "snapshot.storage.k8s.io/v1"
)

var (
	// SnapshotGVR 是 volumesnapshots 的 GroupVersionResource
	SnapshotGVR = schema.GroupVersionResource{Group: SnapshotGroup, Version: "v1", Resource: "volumesnapshots"}
	// SnapshotClassGVR 是 volumesnapshotclasses 的 GroupVersionResource
	SnapshotClassGVR = schema.GroupVersionResource{Group: SnapshotGroup, Version: "v1", Resource: "volumesnapshotclasses"}
	// SnapshotContentGVR 是 volumesnapshotcontents 的 GroupVersionResource
	SnapshotContentGVR = schema.GroupVersionResource{Group: SnapshotGroup, Version: "v1", Resource: "volumesnapshotcontents"}
)

// WaitForSnapshotReady 等待 VolumeSnapshot 可用或超时，以先发生者为准。
func WaitForSnapshotReady(ctx context.Context, c dynamic.Interface, ns string, snapshotName string, poll, timeout time.Duration) error {
	framework.Logf("Waiting up to %v for VolumeSnapshot %s to become ready", timeout, snapshotName)

	if successful := WaitUntil(poll, timeout, func() bool {
		snapshot, err := c.Resource(SnapshotGVR).Namespace(ns).Get(ctx, snapshotName, metav1.GetOptions{})
		if err != nil {
			framework.Logf("Failed to get snapshot %q, retrying in %v. Error: %v", snapshotName, poll, err)
			return false
		}

		status := snapshot.Object["status"]
		if status == nil {
			framework.Logf("VolumeSnapshot %s found but is not ready.", snapshotName)
			return false
		}
		value := status.(map[string]interface{})
		if value["readyToUse"] == true {
			framework.Logf("VolumeSnapshot %s found and is ready", snapshotName)
			return true
		}

		framework.Logf("VolumeSnapshot %s found but is not ready.", snapshotName)
		return false
	}); successful {
		return nil
	}

	return fmt.Errorf("VolumeSnapshot %s is not ready within %v", snapshotName, timeout)
}

// GetSnapshotContentFromSnapshot 返回绑定到给定 VolumeSnapshot 的
// VolumeSnapshotContent 对象
func GetSnapshotContentFromSnapshot(ctx context.Context, dc dynamic.Interface, snapshot *unstructured.Unstructured, timeout time.Duration) *unstructured.Unstructured {
	defer ginkgo.GinkgoRecover()
	err := WaitForSnapshotReady(ctx, dc, snapshot.GetNamespace(), snapshot.GetName(), framework.Poll, timeout)
	framework.ExpectNoError(err)

	vs, err := dc.Resource(SnapshotGVR).Namespace(snapshot.GetNamespace()).Get(ctx, snapshot.GetName(), metav1.GetOptions{})

	snapshotStatus := vs.Object["status"].(map[string]interface{})
	snapshotContentName := snapshotStatus["boundVolumeSnapshotContentName"].(string)
	framework.Logf("received snapshotStatus %v", snapshotStatus)
	framework.Logf("snapshotContentName %s", snapshotContentName)
	framework.ExpectNoError(err)

	vscontent, err := dc.Resource(SnapshotContentGVR).Get(ctx, snapshotContentName, metav1.GetOptions{})
	framework.ExpectNoError(err)

	return vscontent

}

// DeleteSnapshotWithoutWaiting 删除 VolumeSnapshot 后立即返回，不等待
func DeleteSnapshotWithoutWaiting(ctx context.Context, dc dynamic.Interface, ns string, snapshotName string) error {
	ginkgo.By("deleting the snapshot")
	err := dc.Resource(SnapshotGVR).Namespace(ns).Delete(ctx, snapshotName, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

// DeleteAndWaitSnapshot 删除 VolumeSnapshot 并等待其被删除或超时，以先发生者为准
func DeleteAndWaitSnapshot(ctx context.Context, dc dynamic.Interface, ns string, snapshotName string, poll, timeout time.Duration) error {
	var err error
	err = DeleteSnapshotWithoutWaiting(ctx, dc, ns, snapshotName)
	if err != nil {
		return err
	}

	ginkgo.By("checking the Snapshot has been deleted")
	err = WaitForNamespacedGVRDeletion(ctx, dc, SnapshotGVR, ns, snapshotName, poll, timeout)

	return err
}

// GenerateSnapshotClassSpec 构造新的 SnapshotClass spec，
// 使用基于 namespace + 后缀的唯一名称。
func GenerateSnapshotClassSpec(
	snapshotter string,
	parameters map[string]string,
	ns string,
) *unstructured.Unstructured {
	snapshotClass := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"kind":       "VolumeSnapshotClass",
			"apiVersion": SnapshotAPIVersion,
			"metadata": map[string]interface{}{
				// 名称必须唯一，因此基于命名空间名并用 GenerateName 生成
				"name": names.SimpleNameGenerator.GenerateName(ns),
			},
			"driver":         snapshotter,
			"parameters":     parameters,
			"deletionPolicy": "Delete",
		},
	}

	return snapshotClass
}
