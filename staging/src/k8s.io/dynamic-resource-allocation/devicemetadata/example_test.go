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

// Package devicemetadata_test 提供 devicemetadata 包的外部示例测试，
// 演示如何从 JSON 流解码元数据以及如何处理多版本元数据文件。
package devicemetadata_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"k8s.io/dynamic-resource-allocation/api/metadata"
	"k8s.io/dynamic-resource-allocation/devicemetadata"
)

// ExampleDecodeMetadataFromStream 演示把可能包含多个 API 版本的元数据文件当作 JSON 流来读取。
// 推荐解码到内部类型 metadata.DeviceMetadata，因为它转换次数更少，
// 且新增或移除 API 版本时无需改动源代码。
func ExampleDecodeMetadataFromStream() {
	// 单版本元数据文件内容：一个 v1alpha1 的 DeviceMetadata 对象。
	fileData := []byte(`{"apiVersion":"metadata.resource.k8s.io/v1alpha1","kind":"DeviceMetadata","metadata":{"name":"my-claim","namespace":"default","uid":"uid-1234","generation":1},"requests":[{"name":"gpu","devices":[{"driver":"gpu.example.com","pool":"worker-0","name":"gpu-0","attributes":{"model":{"string":"LATEST-GPU-MODEL"},"driverVersion":{"version":"1.0.0"},"type":{"string":"gpu"}}}]}]}`)

	// 解码到内部类型，出错则打印错误并提前返回。
	var md metadata.DeviceMetadata
	if err := devicemetadata.DecodeMetadataFromStream(json.NewDecoder(bytes.NewReader(fileData)), &md); err != nil {
		fmt.Printf("error: %v\n", err)
		return
	}
	// 依次打印 claim 标识、请求名与设备数量，以及首台设备的详情。
	fmt.Printf("claim: %s/%s\n", md.Namespace, md.Name)
	fmt.Printf("request: %s, devices: %d\n", md.Requests[0].Name, len(md.Requests[0].Devices))
	fmt.Printf("device: %s (driver: %s, pool: %s)\n", md.Requests[0].Devices[0].Name, md.Requests[0].Devices[0].Driver, md.Requests[0].Devices[0].Pool)
	// Output:
	// claim: default/my-claim
	// request: gpu, devices: 1
	// device: gpu-0 (driver: gpu.example.com, pool: worker-0)
}

// ExampleDecodeMetadataFromStream_multiVersion 演示读取用多个 API 版本编码同一份数据的元数据文件
// （新版本在前）。函数会跳过无法解码的版本，并把第一个兼容版本解析为内部类型。
func ExampleDecodeMetadataFromStream_multiVersion() {
	// 这里虚构的 v1000 使用了不同的字段名，应当被跳过。
	stream := `
{"apiVersion":"metadata.resource.k8s.io/v1000","kind":"DeviceMetadata","metadata":{"claimName":"my-claim","claimNamespace":"default","uid":"uid-1234","generation":1},"requests":[{"name":"gpu","devices":[{"driver":"gpu.example.com","pool":"worker-0","name":"gpu-0"}]}]}
{"apiVersion":"metadata.resource.k8s.io/v1alpha1","kind":"DeviceMetadata","metadata":{"name":"my-claim","namespace":"default","uid":"uid-1234","generation":1},"requests":[{"name":"gpu","devices":[{"driver":"gpu.example.com","pool":"worker-0","name":"gpu-0","attributes":{"model":{"string":"LATEST-GPU-MODEL"},"type":{"string":"gpu"}}}]}]}
`
	// 解码到内部类型：跳过 v1000，采用 v1alpha1，出错则打印并返回。
	var md metadata.DeviceMetadata
	if err := devicemetadata.DecodeMetadataFromStream(json.NewDecoder(strings.NewReader(stream)), &md); err != nil {
		fmt.Printf("error: %v\n", err)
		return
	}
	// 打印结果，预期与单版本示例一致。
	fmt.Printf("claim: %s/%s\n", md.Namespace, md.Name)
	fmt.Printf("request: %s, devices: %d\n", md.Requests[0].Name, len(md.Requests[0].Devices))
	fmt.Printf("device: %s (driver: %s, pool: %s)\n", md.Requests[0].Devices[0].Name, md.Requests[0].Devices[0].Driver, md.Requests[0].Devices[0].Pool)
	// Output:
	// claim: default/my-claim
	// request: gpu, devices: 1
	// device: gpu-0 (driver: gpu.example.com, pool: worker-0)
}
