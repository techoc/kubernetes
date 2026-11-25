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

package dra

import (
	"bytes"
	"encoding/json"

	"github.com/stretchr/testify/require"

	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	resourceapiac "k8s.io/client-go/applyconfigurations/resource/v1"
	"k8s.io/kubernetes/test/utils/client-go/ktesting"
)

// testResourceClaimDeviceStatus 创建一个带有无效设备（未分配的设备）的 ResourceClaim，
// 并检查该对象是否未被校验（特性启用时），或在禁用时被接受但丢弃该字段。
//
// 启用时，它会尝试使用不同客户端执行 server-side-apply (SSA)，这是 DRA 驱动应当使用的方式。
func testResourceClaimDeviceStatus(tCtx ktesting.TContext, enabled bool) {
	tCtx.Parallel()

	// 创建一个唯一的命名空间，
	// 并从中派生出全局唯一的字符串。
	namespace := createTestNamespace(tCtx, nil)
	driverNamePrefix := "driver-" + namespace + "-"
	driverName1 := driverNamePrefix + "one"
	driverName2 := driverNamePrefix + "two"
	driverName3 := driverNamePrefix + "three"
	deviceClass := "class-" + namespace

	claim := &resourceapi.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: claimName,
		},
		Spec: resourceapi.ResourceClaimSpec{
			Devices: resourceapi.DeviceClaim{
				Requests: []resourceapi.DeviceRequest{
					{
						Name: "foo",
						Exactly: &resourceapi.ExactDeviceRequest{
							DeviceClassName: deviceClass,
						},
					},
				},
			},
		},
	}

	claim, err := tCtx.Client().ResourceV1().ResourceClaims(namespace).Create(tCtx, claim, metav1.CreateOptions{FieldValidation: "Strict"})
	tCtx.ExpectNoError(err, "create ResourceClaim")

	deviceStatus := []resourceapi.AllocatedDeviceStatus{{
		Driver: driverName1,
		Pool:   "global",
		Device: "my-device",
		Data: &runtime.RawExtension{
			Raw: []byte(`{"kind": "foo", "apiVersion": "dra.example.com/v1"}`),
		},
		NetworkData: &resourceapi.NetworkDeviceData{
			InterfaceName: "net-1",
			IPs: []string{
				"10.9.8.0/24",
				"2001:db8::/64",
			},
			HardwareAddress: "ea:9f:cb:40:b1:7b",
		},
	}}
	claim.Status.Devices = deviceStatus
	updatedClaim, err := tCtx.Client().ResourceV1().ResourceClaims(namespace).UpdateStatus(tCtx, claim, metav1.UpdateOptions{})
	if !enabled {
		tCtx.ExpectNoError(err, "updating the status with an invalid AllocatedDeviceStatus should have worked because the field should have been dropped")
		require.Empty(tCtx, updatedClaim.Status.Devices, "field should have been dropped")
		return
	}

	// 以下测试针对已启用的特性。

	if err == nil {
		tCtx.Fatal("updating the status with an invalid AllocatedDeviceStatus should have failed and didn't")
	}

	// 添加分配结果。
	claim.Status.Allocation = &resourceapi.AllocationResult{
		Devices: resourceapi.DeviceAllocationResult{
			Results: []resourceapi.DeviceRequestAllocationResult{
				{
					Request: "foo",
					Driver:  driverName1,
					Pool:    "global",
					Device:  "my-device",
				},
				{
					Request: "foo",
					Driver:  driverName2,
					Pool:    "global",
					Device:  "another-device",
				},
				{
					Request: "foo",
					Driver:  driverName3,
					Pool:    "global",
					Device:  "my-device",
				},
			},
		},
	}
	claim, err = tCtx.Client().ResourceV1().ResourceClaims(namespace).UpdateStatus(tCtx, claim, metav1.UpdateOptions{})
	tCtx.ExpectNoError(err, "add allocation result")

	// 现在添加设备状态应当可以成功。
	claim.Status.Devices = deviceStatus
	claim, err = tCtx.Client().ResourceV1().ResourceClaims(namespace).UpdateStatus(tCtx, claim, metav1.UpdateOptions{})
	tCtx.ExpectNoError(err, "add device status")
	require.Equal(tCtx, deviceStatus, claim.Status.Devices, "after adding device status")

	// 去除 RawExtension。SSA 会重新编码它，产生细微差异，仍会导致 assert.Equal 失败。
	claim.Status.Devices[0].Data = nil
	deviceStatus[0].Data = nil
	claim, err = tCtx.Client().ResourceV1().ResourceClaims(namespace).UpdateStatus(tCtx, claim, metav1.UpdateOptions{})
	tCtx.ExpectNoError(err, "add device status")
	require.Equal(tCtx, deviceStatus, claim.Status.Devices, "after stripping RawExtension")

	// 测试 SSA。
	deviceStatusAC := resourceapiac.AllocatedDeviceStatus().
		WithDriver(driverName2).
		WithPool("global").
		WithDevice("another-device").
		WithNetworkData(resourceapiac.NetworkDeviceData().WithInterfaceName("net-2"))
	deviceStatus = append(deviceStatus, resourceapi.AllocatedDeviceStatus{
		Driver: driverName2,
		Pool:   "global",
		Device: "another-device",
		NetworkData: &resourceapi.NetworkDeviceData{
			InterfaceName: "net-2",
		},
	})
	claimAC := resourceapiac.ResourceClaim(claim.Name, claim.Namespace).
		WithStatus(resourceapiac.ResourceClaimStatus().WithDevices(deviceStatusAC))
	claim, err = tCtx.Client().ResourceV1().ResourceClaims(namespace).ApplyStatus(tCtx, claimAC, metav1.ApplyOptions{
		Force:        true,
		FieldManager: "manager-1",
	})
	tCtx.ExpectNoError(err, "apply device status two")
	require.Equal(tCtx, deviceStatus, claim.Status.Devices, "after applying device status two")

	deviceStatusAC = resourceapiac.AllocatedDeviceStatus().
		WithDriver(driverName3).
		WithPool("global").
		WithDevice("my-device").
		WithNetworkData(resourceapiac.NetworkDeviceData().WithInterfaceName("net-3"))
	deviceStatus = append(deviceStatus, resourceapi.AllocatedDeviceStatus{
		Driver: driverName3,
		Pool:   "global",
		Device: "my-device",
		NetworkData: &resourceapi.NetworkDeviceData{
			InterfaceName: "net-3",
		},
	})
	claimAC = resourceapiac.ResourceClaim(claim.Name, claim.Namespace).
		WithStatus(resourceapiac.ResourceClaimStatus().WithDevices(deviceStatusAC))
	claim, err = tCtx.Client().ResourceV1().ResourceClaims(namespace).ApplyStatus(tCtx, claimAC, metav1.ApplyOptions{
		Force:        true,
		FieldManager: "manager-2",
	})
	tCtx.ExpectNoError(err, "apply device status three")
	require.Equal(tCtx, deviceStatus, claim.Status.Devices, "after applying device status three")
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetIndent("   ", "   ")
	tCtx.ExpectNoError(encoder.Encode(claim))
	tCtx.Logf("Final ResourceClaim:\n%s", buffer.String())

	// 更新一个条目，删除另一个。
	deviceStatusAC = resourceapiac.AllocatedDeviceStatus().
		WithDriver(driverName2).
		WithPool("global").
		WithDevice("another-device").
		WithNetworkData(resourceapiac.NetworkDeviceData().WithInterfaceName("yet-another-net"))
	deviceStatus[1].NetworkData.InterfaceName = "yet-another-net"
	claimAC = resourceapiac.ResourceClaim(claim.Name, claim.Namespace).
		WithStatus(resourceapiac.ResourceClaimStatus().WithDevices(deviceStatusAC))
	claim, err = tCtx.Client().ResourceV1().ResourceClaims(namespace).ApplyStatus(tCtx, claimAC, metav1.ApplyOptions{
		Force:        true,
		FieldManager: "manager-1",
	})
	tCtx.ExpectNoError(err, "update device status two")
	require.Equal(tCtx, deviceStatus, claim.Status.Devices, "after updating device status two")
	claimAC = resourceapiac.ResourceClaim(claim.Name, claim.Namespace)
	deviceStatus = deviceStatus[0:2]
	claim, err = tCtx.Client().ResourceV1().ResourceClaims(namespace).ApplyStatus(tCtx, claimAC, metav1.ApplyOptions{
		Force:        true,
		FieldManager: "manager-2",
	})
	tCtx.ExpectNoError(err, "remove device status three")
	require.Equal(tCtx, deviceStatus, claim.Status.Devices, "after removing device status three")
}
