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

package dra

import (
	"fmt"
	"strings"
	"time"

	"github.com/onsi/gomega"

	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/dynamic-resource-allocation/resourceslice"
	"k8s.io/kubernetes/test/utils/client-go/ktesting"
)

// TestCreateResourceSlices 用 ResourceSlice controller 创建 slice，既作为集成测试也作为 E2E 测试运行（slice 数量不同）
func TestCreateResourceSlices(tCtx ktesting.TContext, numSlices int) {
	// 构造期望的 resource slice
	namespace := tCtx.Namespace()
	driverName := namespace
	devicePrefix := "dev-"
	domainSuffix := ".example.com"
	poolName := "network-attached"
	domain := strings.Repeat("x", resourceapi.DeviceMaxDomainLength-len(domainSuffix)) + domainSuffix
	stringValue := strings.Repeat("v", resourceapi.DeviceAttributeMaxValueLength)
	pool := resourceslice.Pool{
		Slices:   make([]resourceslice.Slice, numSlices),
		AllNodes: true,
	}
	numDevices := 0
	for i := range numSlices {
		devices := make([]resourceapi.Device, resourceapi.ResourceSliceMaxDevices)
		for e := range resourceapi.ResourceSliceMaxDevices {
			device := resourceapi.Device{
				Name:       devicePrefix + strings.Repeat("x", validation.DNS1035LabelMaxLength-len(devicePrefix)-6) + fmt.Sprintf("%06d", numDevices),
				Attributes: make(map[resourceapi.QualifiedName]resourceapi.DeviceAttribute, resourceapi.ResourceSliceMaxAttributesAndCapacitiesPerDevice),
			}
			numDevices++
			for j := range resourceapi.ResourceSliceMaxAttributesAndCapacitiesPerDevice {
				name := resourceapi.QualifiedName(domain + "/" + strings.Repeat("x", resourceapi.DeviceMaxIDLength-4) + fmt.Sprintf("%04d", j))
				device.Attributes[name] = resourceapi.DeviceAttribute{
					StringValue: &stringValue,
				}
			}
			devices[e] = device
		}
		pool.Slices[i].Devices = devices
	}
	resources := &resourceslice.DriverResources{
		Pools: map[string]resourceslice.Pool{poolName: pool},
	}
	listSlices := func(tCtx ktesting.TContext) *resourceapi.ResourceSliceList {
		tCtx.Helper()
		// TODO: 用 ktesting 实现 framework.ListObjects/Get 等辅助方法
		slices, err := tCtx.Client().ResourceV1().ResourceSlices().List(tCtx, metav1.ListOptions{
			FieldSelector: resourceapi.ResourceSliceSelectorDriver + "=" + driverName,
		})
		tCtx.ExpectNoError(err, "list slices")
		return slices
	}

	tCtx.Log("Creating slices")
	mutationCacheTTL := 10 * time.Second
	controller, err := resourceslice.StartController(tCtx, resourceslice.Options{
		DriverName:       driverName,
		KubeClient:       tCtx.Client(),
		Resources:        resources,
		MutationCacheTTL: &mutationCacheTTL,
	})
	tCtx.ExpectNoError(err, "start controller")
	tCtx.CleanupCtx(func(tCtx ktesting.TContext) {
		controller.Stop()
		tCtx.Eventually(func(tCtx ktesting.TContext) *resourceapi.ResourceSliceList {
			err := tCtx.Client().ResourceV1().ResourceSlices().DeleteCollection(tCtx, metav1.DeleteOptions{}, metav1.ListOptions{
				FieldSelector: resourceapi.ResourceSliceSelectorDriver + "=" + driverName,
			})
			tCtx.ExpectNoError(err, "delete slices")
			return listSlices(tCtx)
		}).Should(gomega.HaveField("Items", gomega.BeEmpty()))
	})

	// 等待 controller 最终创建出全部期望的 slice
	tCtx.Eventually(listSlices).WithTimeout(3 * time.Minute).Should(gomega.HaveField("Items", gomega.HaveLen(numSlices)))

	// 校验 slice 体积和 controller 统计值
	expectSlices := listSlices(tCtx)
	tCtx.Assert(expectSlices.Items).ShouldNot(gomega.BeEmpty())
	tCtx.Logf("Protobuf size of one slice is %d bytes = %d KB.", expectSlices.Items[0].Size(), expectSlices.Items[0].Size()/1024)
	tCtx.Assert(expectSlices.Items[0].Size()).Should(gomega.BeNumerically(">=", 600*1024), "ResourceSlice size")
	tCtx.Assert(expectSlices.Items[0].Size()).Should(gomega.BeNumerically("<", 1024*1024), "ResourceSlice size")
	expectStats := resourceslice.Stats{NumCreates: int64(numSlices)}
	tCtx.Assert(controller.GetStats()).Should(gomega.Equal(expectStats))

	// 再观察一段时间，确认没有额外变更
	getStats := func(tCtx ktesting.TContext) resourceslice.Stats { return controller.GetStats() }
	tCtx.Consistently(getStats).WithTimeout(2 * mutationCacheTTL).Should(gomega.Equal(expectStats))

	// 更新期望资源，让 controller 只保留一个空 slice、其余删除
	tCtx.Log("Deleting slices")
	resources = resources.DeepCopy()
	resources.Pools[poolName] = resourceslice.Pool{Slices: []resourceslice.Slice{{}}, AllNodes: true}
	controller.Update(resources)

	// 期望一个 slice 被更新为空，其余被删除
	emptySlice := gomega.HaveField("Spec.Devices", gomega.BeEmpty())
	tCtx.Eventually(listSlices).WithTimeout(2 * time.Minute).Should(gomega.HaveField("Items", gomega.HaveExactElements(emptySlice)))
	expectStats = resourceslice.Stats{NumCreates: int64(numSlices), NumUpdates: 1, NumDeletes: int64(numSlices) - 1}

	// slice 已出现在 list 结果中但尚未计入 controller 统计之间存在时间窗口，
	// 主要是测试进程与 API server 的网络延迟。先等统计值收敛，再断言无后续变更
	tCtx.Eventually(getStats).WithTimeout(30 * time.Second).Should(gomega.Equal(expectStats))

	tCtx.Consistently(getStats).WithTimeout(2 * mutationCacheTTL).Should(gomega.Equal(expectStats))
}
