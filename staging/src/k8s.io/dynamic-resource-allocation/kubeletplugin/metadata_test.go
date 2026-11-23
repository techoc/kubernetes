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

package kubeletplugin

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"

	cdispec "tags.cncf.io/container-device-interface/specs-go"

	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/dynamic-resource-allocation/api/metadata"
	"k8s.io/dynamic-resource-allocation/api/metadata/v1alpha1"
	"k8s.io/dynamic-resource-allocation/devicemetadata"
	"k8s.io/dynamic-resource-allocation/resourceclaim"
	"k8s.io/utils/ptr"
)

const (
	// 测试所用的驱动名、claim 名称与命名空间。
	testDriverName = "example.com"
	testClaimName  = "my-claim"
	testClaimNS    = "default"
	// Starts with a digit so CDI MinimumRequiredVersion yields 0.5.0,
	// matching real claim UIDs (UUIDs) that may start with a digit.
	// 以数字开头，使 CDI MinimumRequiredVersion 得到 0.5.0，匹配真实以数字开头的 claim UID。
	testClaimUID              = types.UID("1234-claim-uid")
	testRequest               = "gpu-request"
	testRequestWithSubrequest = "gpu-request/high-memory"
)

// metadataTypeMeta 是测试期望的 v1alpha1 DeviceMetadata TypeMeta。
var metadataTypeMeta = metav1.TypeMeta{
	APIVersion: v1alpha1.SchemeGroupVersion.String(),
	Kind:       "DeviceMetadata",
}

// testClaim 构造测试用的 ResourceClaim，带固定的名称、命名空间与 UID。
func testClaim() *resourceapi.ResourceClaim {
	// 只填充身份字段，其余留空。
	return &resourceapi.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testClaimName,
			Namespace: testClaimNS,
			UID:       testClaimUID,
		},
	}
}

// TestNewMetadataWriterVersionValidation 验证 newMetadataWriter 的版本校验行为：
// 全部受支持或部分受支持时成功，全部不受支持或空列表时报错。
// 输入：四种版本组合；断言是否返回错误。
func TestNewMetadataWriterVersionValidation(t *testing.T) {
	// 构造一个未注册的版本用于测试。
	bogus := schema.GroupVersion{Group: "bogus", Version: "v99"}

	testcases := map[string]struct {
		// versions 是请求的 API 版本列表；expectErr 表示是否期望返回错误。
		versions  []schema.GroupVersion
		expectErr bool
	}{
		// 仅传入受支持版本：应成功。
		"all supported": {
			versions: []schema.GroupVersion{v1alpha1.SchemeGroupVersion},
		},
		// 混合受支持与不支持版本：不支持的被跳过，仍应成功。
		"mix of supported and unsupported": {
			versions: []schema.GroupVersion{bogus, v1alpha1.SchemeGroupVersion},
		},
		// 全部不受支持：应报错。
		"all unsupported": {
			versions:  []schema.GroupVersion{bogus},
			expectErr: true,
		},
		// 空列表：应报错。
		"empty list": {
			versions:  []schema.GroupVersion{},
			expectErr: true,
		},
	}

	// 逐用例执行。
	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			// 调用被测试函数；其余参数使用临时目录与空文件操作。
			_, err := newMetadataWriter(testDriverName, t.TempDir(), t.TempDir(), tc.versions, defaultMetadataFileOperations(MetadataFileOperations{}))
			// 期望报错却返回 nil。
			if tc.expectErr && err == nil {
				t.Fatal("expected error but got nil")
			}
			// 不期望报错却返回错误。
			if !tc.expectErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

// newTestWriter 创建只启用 v1alpha1 的测试写入器，并返回插件目录与 CDI 目录。
func newTestWriter(t *testing.T) (*metadataWriter, string, string) {
	// 标记为测试辅助函数。
	t.Helper()
	// 为插件数据与 CDI 规范各创建一个临时目录。
	pluginDir := t.TempDir()
	cdiDir := t.TempDir()
	// 只启用 v1alpha1 版本。
	w, err := newMetadataWriter(testDriverName, pluginDir, cdiDir, []schema.GroupVersion{v1alpha1.SchemeGroupVersion}, defaultMetadataFileOperations(MetadataFileOperations{}))
	if err != nil {
		t.Fatalf("newMetadataWriter: %v", err)
	}
	return w, pluginDir, cdiDir
}

// readMetadataFile 从指定路径读取并按多版本流解码出 v1alpha1.DeviceMetadata。
func readMetadataFile(t *testing.T, path string) *v1alpha1.DeviceMetadata {
	// 标记为测试辅助函数。
	t.Helper()
	// 读取文件字节。
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read metadata file: %v", err)
	}
	// 准备解码目标。
	var dm v1alpha1.DeviceMetadata
	// 从流中解码第一个兼容版本。
	if err := devicemetadata.DecodeMetadataFromStream(json.NewDecoder(bytes.NewReader(data)), &dm); err != nil {
		t.Fatalf("decode metadata file: %v", err)
	}
	return &dm
}

// stringAttrs 把字符串映射转换为带 StringValue 的 DeviceMetadata 属性映射。
func stringAttrs(attrs map[string]string) *DeviceMetadata {
	// 预分配属性映射。
	da := make(map[string]resourceapi.DeviceAttribute, len(attrs))
	// 逐项转换。
	for k, v := range attrs {
		da[k] = resourceapi.DeviceAttribute{StringValue: &v}
	}
	// 返回包装结果。
	return &DeviceMetadata{Attributes: da}
}

// expectedV1Alpha1Attrs 生成 v1alpha1 的期望属性映射（QualifiedName -> DeviceAttribute）。
func expectedV1Alpha1Attrs(attrs map[string]string) map[resourceapi.QualifiedName]resourceapi.DeviceAttribute {
	da := make(map[resourceapi.QualifiedName]resourceapi.DeviceAttribute, len(attrs))
	for k, v := range attrs {
		da[resourceapi.QualifiedName(k)] = resourceapi.DeviceAttribute{StringValue: &v}
	}
	return da
}

// attrsToStrings 反向提取属性中的字符串值，忽略非字符串属性。
func attrsToStrings(attrs map[string]resourceapi.DeviceAttribute) map[string]string {
	// 预分配结果。
	out := make(map[string]string, len(attrs))
	// 遍历属性。
	for k, v := range attrs {
		// 只收集字符串值。
		if v.StringValue != nil {
			out[k] = *v.StringValue
		}
	}
	return out
}

// expectedRequestMetadata 根据给定请求名与设备列表构造期望的 v1alpha1 DeviceMetadata。
func expectedRequestMetadata(reqName string, devs []Device) *v1alpha1.DeviceMetadata {
	// 设备列表初始为空。
	var expectedDevs []v1alpha1.Device
	// 逐台设备构造期望值。
	for _, d := range devs {
		// 驱动名填测试驱动名，池名与设备名来自输入。
		ed := v1alpha1.Device{
			Driver: testDriverName,
			Pool:   d.PoolName,
			Name:   d.DeviceName,
		}
		// 有额外元数据时复制属性与网络数据。
		if d.Metadata != nil {
			// 属性通过字符串映射转换。
			if d.Metadata.Attributes != nil {
				ed.Attributes = expectedV1Alpha1Attrs(
					attrsToStrings(d.Metadata.Attributes))
			}
			ed.NetworkData = d.Metadata.NetworkData
		}
		expectedDevs = append(expectedDevs, ed)
	}
	// 组装期望元数据：generation 为 1，Requests 只含一个请求。
	return &v1alpha1.DeviceMetadata{
		TypeMeta: metadataTypeMeta,
		ObjectMeta: metav1.ObjectMeta{
			Name:       testClaimName,
			Namespace:  testClaimNS,
			UID:        testClaimUID,
			Generation: 1,
		},
		Requests: []v1alpha1.DeviceMetadataRequest{{
			Name:    reqName,
			Devices: expectedDevs,
		}},
	}
}

// expectedCDISpec returns the expected CDI spec for a single request.
// containerPath is the full expected path inside the container.
// expectedCDISpec 构造单个请求期望的 CDI 规范，containerPath 是容器内的完整挂载路径。
func expectedCDISpec(reqName, metadataHostPath, containerPath string) cdispec.Spec {
	// 一个设备，带一个只读绑定挂载。
	spec := cdispec.Spec{
		Kind: testDriverName + "/metadata",
		Devices: []cdispec.Device{{
			Name: string(testClaimUID) + "_" + reqName,
			ContainerEdits: cdispec.ContainerEdits{
				Mounts: []*cdispec.Mount{{
					HostPath:      metadataHostPath,
					ContainerPath: containerPath,
					Options:       []string{"ro", "bind"},
				}},
			},
		}},
	}
	// 计算所需的最低 CDI 版本。
	v, _ := cdispec.MinimumRequiredVersion(&spec)
	// 写入计算得到的版本。
	spec.Version = v
	return spec
}

// expectedRequestPaths 保存期望的宿主元数据路径后缀、CDI 规范文件名与容器内路径。
type expectedRequestPaths struct {
	// hostMetadataSuffix 是元数据文件相对插件目录的路径；cdiSpecFileName 是 CDI 规范文件名；
	// containerPath 是元数据文件在容器内的挂载路径。
	hostMetadataSuffix string
	cdiSpecFileName    string
	containerPath      string
}

// claimPaths 计算普通 ResourceClaim 的请求所期望的三类路径。
func claimPaths(requestRef string) expectedRequestPaths {
	// 子请求名使用基础请求名。
	baseReq := resourceclaim.BaseRequestRef(requestRef)
	// 普通声明的容器内路径位于 ResourceClaims 子目录。
	return expectedRequestPaths{
		hostMetadataSuffix: metadataSubDir + "/" + testClaimNS + "_" + testClaimName + "/" + baseReq + "/metadata.json",
		cdiSpecFileName:    testDriverName + "_metadata_" + string(testClaimUID) + "_" + baseReq + ".json",
		containerPath:      metadata.ContainerDir + "/" + metadata.ResourceClaimsSubDir + "/" + testClaimName + "/" + baseReq + "/" + metadata.MetadataFileName(testDriverName),
	}
}

// templateClaimPaths 计算来自 Pod 资源声明模板的请求所期望的路径，容器内位于 ResourceClaimTemplates 子目录。
func templateClaimPaths(requestRef, podClaimName string) expectedRequestPaths {
	// 子请求名使用基础请求名。
	baseReq := resourceclaim.BaseRequestRef(requestRef)
	// 模板声明的容器内路径位于 ResourceClaimTemplates 子目录。
	return expectedRequestPaths{
		hostMetadataSuffix: metadataSubDir + "/" + testClaimNS + "_" + testClaimName + "/" + baseReq + "/metadata.json",
		cdiSpecFileName:    testDriverName + "_metadata_" + string(testClaimUID) + "_" + baseReq + ".json",
		containerPath:      metadata.ContainerDir + "/" + metadata.ResourceClaimTemplatesSubDir + "/" + podClaimName + "/" + baseReq + "/" + metadata.MetadataFileName(testDriverName),
	}
}

// TestProcessPreparedClaim 验证 processPreparedClaim 对多种设备/请求组合写入的元数据文件、CDI 规范
// 以及返回的 CDI 设备 ID 均符合预期。
// 覆盖：带属性元数据、无元数据（延迟补充）、网络数据、多请求、子请求、带 Pod 声明名注解。
func TestProcessPreparedClaim(t *testing.T) {
	testcases := map[string]struct {
		// devices 是输入的已准备设备；claimAnnotations 是 claim 注解；expectPodClaimName 是期望的 Pod 声明名；
		// wantPaths 是各请求期望的路径。
		devices            []Device
		claimAnnotations   map[string]string
		expectPodClaimName *string
		wantPaths          map[string]expectedRequestPaths
	}{
		// 设备带字符串属性元数据。
		"with-metadata": {
			devices: []Device{{
				Requests:   []string{testRequest},
				PoolName:   "node-1",
				DeviceName: "gpu-0",
				Metadata:   stringAttrs(map[string]string{"pciBusID": "0000:03:00.0"}),
			}},
			wantPaths: map[string]expectedRequestPaths{
				testRequest: claimPaths(testRequest),
			},
		},
		// 设备无元数据，留待后续更新。
		"without-metadata-deferred": {
			devices: []Device{{
				Requests:   []string{testRequest},
				PoolName:   "node-1",
				DeviceName: "gpu-0",
			}},
			wantPaths: map[string]expectedRequestPaths{
				testRequest: claimPaths(testRequest),
			},
		},
		// 设备带网络数据。
		"network-device-data": {
			devices: []Device{{
				Requests:   []string{testRequest},
				PoolName:   "node-1",
				DeviceName: "vf-0",
				Metadata: &DeviceMetadata{
					NetworkData: &resourceapi.NetworkDeviceData{
						InterfaceName: "eth0",
						IPs:           []string{"10.0.0.5/24"},
					},
				},
			}},
			wantPaths: map[string]expectedRequestPaths{
				testRequest: claimPaths(testRequest),
			},
		},
		// 多个请求分别对应不同设备。
		"multiple-requests": {
			devices: []Device{
				{Requests: []string{"gpu-request"}, PoolName: "node-1", DeviceName: "gpu-0"},
				{Requests: []string{"nic-request"}, PoolName: "node-1", DeviceName: "nic-0"},
			},
			wantPaths: map[string]expectedRequestPaths{
				"gpu-request": claimPaths("gpu-request"),
				"nic-request": claimPaths("nic-request"),
			},
		},
		// 请求名带子请求后缀。
		"subrequest": {
			devices: []Device{{
				Requests:   []string{testRequestWithSubrequest},
				PoolName:   "node-1",
				DeviceName: "gpu-0",
				Metadata:   stringAttrs(map[string]string{"memoryGB": "80"}),
			}},
			wantPaths: map[string]expectedRequestPaths{
				testRequestWithSubrequest: claimPaths(testRequestWithSubrequest),
			},
		},
		// claim 带 Pod 资源声明注解，容器内路径走模板目录。
		"with-pod-claim-name-annotation": {
			devices: []Device{{
				Requests:   []string{testRequest},
				PoolName:   "node-1",
				DeviceName: "gpu-0",
				Metadata:   stringAttrs(map[string]string{"model": "LATEST-GPU-MODEL"}),
			}},
			claimAnnotations:   map[string]string{resourceapi.PodResourceClaimAnnotation: "my-gpu"},
			expectPodClaimName: ptr.To("my-gpu"), //nolint:modernize
			wantPaths: map[string]expectedRequestPaths{
				testRequest: templateClaimPaths(testRequest, "my-gpu"),
			},
		},
	}

	// 逐用例执行。
	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			// 创建测试写入器与临时目录。
			w, pluginDir, cdiDir := newTestWriter(t)
			// 构造 claim。
			claim := testClaim()

			// 按需设置 Pod 声明名注解。
			if tc.claimAnnotations != nil {
				claim.Annotations = tc.claimAnnotations
			}

			// 调用被测试函数。
			cdiIDs, err := w.processPreparedClaim(claim, tc.devices)
			// 写入失败直接终止用例。
			if err != nil {
				t.Fatalf("processPreparedClaim: %v", err)
			}

			// 按请求名重新分组，作为期望值的基础。
			devicesByRequest := map[string][]Device{}
			// 遍历设备。
			for _, d := range tc.devices {
				// 设备可能被多个请求引用。
				for _, req := range d.Requests {
					devicesByRequest[req] = append(devicesByRequest[req], d)
				}
			}

			// 构造期望的 CDI 设备 ID 映射。
			expectedCDIIDs := make(map[string]string, len(devicesByRequest))
			// 逐请求校验文件。
			for requestRef, devs := range devicesByRequest {
				// 基础请求名。
				baseReq := resourceclaim.BaseRequestRef(requestRef)
				// 期望的 CDI 设备 ID 格式。
				expectedCDIIDs[baseReq] = testDriverName + "/metadata=" + string(testClaimUID) + "_" + baseReq

				// 取出该请求期望的路径。
				wp := tc.wantPaths[requestRef]

				// 拼出宿主元数据文件路径。
				metadataPath := filepath.Join(pluginDir, wp.hostMetadataSuffix)
				// 读取并解码元数据。
				dm := readMetadataFile(t, metadataPath)
				// 构造期望元数据。
				expected := expectedRequestMetadata(requestRef, devs)
				// 设置期望的 Pod 声明名。
				expected.PodClaimName = tc.expectPodClaimName
				// 断言元数据内容一致。
				if diff := cmp.Diff(expected, dm); diff != "" {
					t.Errorf("metadata for request %q (-want +got):\n%s", requestRef, diff)
				}

				// 拼出 CDI 规范文件路径。
				cdiPath := filepath.Join(cdiDir, wp.cdiSpecFileName)
				// 读取 CDI 规范文件。
				cdiData, err := os.ReadFile(cdiPath)
				if err != nil {
					t.Fatalf("read CDI spec %s: %v", cdiPath, err)
				}
				// 准备反序列化目标。
				var gotCDISpec cdispec.Spec
				// 解析 CDI 规范。
				if err := json.Unmarshal(cdiData, &gotCDISpec); err != nil {
					t.Fatalf("unmarshal CDI spec: %v", err)
				}

				// 断言 CDI 规范内容一致。
				if diff := cmp.Diff(expectedCDISpec(baseReq, metadataPath, wp.containerPath), gotCDISpec); diff != "" {
					t.Errorf("CDI spec for request %q (-want +got):\n%s", requestRef, diff)
				}
			}
			// 断言返回的 CDI ID 映射一致。
			if diff := cmp.Diff(expectedCDIIDs, cdiIDs); diff != "" {
				t.Errorf("CDI IDs (-want +got):\n%s", diff)
			}
		})
	}
}

// TestCleanupClaim 验证 cleanupClaim 删除 claim 的整个元数据目录及所有 CDI 规范文件。
// 覆盖：实际删除、claim 不存在（无操作）、重复调用（幂等）、多请求、子请求。
// 断言：元数据目录与 CDI 规范文件均不再存在。
func TestCleanupClaim(t *testing.T) {
	// 测试属性字符串值。
	strVal := "0000:03:00.0"

	testcases := map[string]struct {
		// prepareDevices 是预先准备的设备；preCleanup 表示是否先清理一次；
		// claimNamespace、claimName、uid 标识要清理的 claim。
		prepareDevices []Device
		preCleanup     bool
		claimNamespace string
		claimName      string
		uid            types.UID
	}{
		// 准备文件后应被删除。
		"removes-files": {
			prepareDevices: []Device{{
				Requests:   []string{testRequest},
				PoolName:   "node-1",
				DeviceName: "gpu-0",
				Metadata: &DeviceMetadata{
					Attributes: map[string]resourceapi.DeviceAttribute{
						"pciBusID": {StringValue: &strVal},
					},
				},
			}},
			claimNamespace: testClaimNS,
			claimName:      testClaimName,
			uid:            testClaimUID,
		},
		// claim 不存在时是无操作。
		"nonexistent-claim-is-noop": {
			claimNamespace: "no-ns",
			claimName:      "no-claim",
			uid:            "unknown-uid",
		},
		// 先清理一次再清理，验证幂等性。
		"idempotent": {
			prepareDevices: []Device{{
				Requests:   []string{testRequest},
				PoolName:   "node-1",
				DeviceName: "gpu-0",
				Metadata: &DeviceMetadata{
					Attributes: map[string]resourceapi.DeviceAttribute{
						"pciBusID": {StringValue: &strVal},
					},
				},
			}},
			preCleanup:     true,
			claimNamespace: testClaimNS,
			claimName:      testClaimName,
			uid:            testClaimUID,
		},
		// 多请求对应的 CDI 规范都应被删除。
		"multiple-requests": {
			prepareDevices: []Device{
				{Requests: []string{"gpu-request"}, PoolName: "node-1", DeviceName: "gpu-0"},
				{Requests: []string{"nic-request"}, PoolName: "node-1", DeviceName: "nic-0"},
			},
			claimNamespace: testClaimNS,
			claimName:      testClaimName,
			uid:            testClaimUID,
		},
		// 子请求对应的文件也应被删除。
		"subrequest": {
			prepareDevices: []Device{{
				Requests:   []string{testRequestWithSubrequest},
				PoolName:   "node-1",
				DeviceName: "gpu-0",
			}},
			claimNamespace: testClaimNS,
			claimName:      testClaimName,
			uid:            testClaimUID,
		},
	}

	// 逐用例执行。
	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			// 创建测试环境。
			w, pluginDir, cdiDir := newTestWriter(t)

			// 先准备文件。
			if tc.prepareDevices != nil {
				if _, err := w.processPreparedClaim(testClaim(), tc.prepareDevices); err != nil {
					t.Fatalf("processPreparedClaim: %v", err)
				}
			}
			// 部分用例先清理一次以验证幂等性。
			if tc.preCleanup {
				if err := w.cleanupClaim(tc.claimNamespace, tc.claimName, tc.uid); err != nil {
					t.Fatalf("pre-cleanupClaim: %v", err)
				}
			}

			// 执行被测试函数。
			if err := w.cleanupClaim(tc.claimNamespace, tc.claimName, tc.uid); err != nil {
				t.Fatalf("cleanupClaim: %v", err)
			}

			// 计算元数据目录路径。
			metadataDir := filepath.Join(pluginDir, metadataSubDir, tc.claimNamespace+"_"+tc.claimName)
			// 断言元数据目录不再存在。
			if _, err := os.Stat(metadataDir); !os.IsNotExist(err) {
				t.Errorf("metadata directory %s still exists", metadataDir)
			}
			// 逐设备校验 CDI 规范已删除。
			for _, d := range tc.prepareDevices {
				// 逐请求计算 CDI 规范文件名。
				for _, reqName := range d.Requests {
					baseReq := resourceclaim.BaseRequestRef(reqName)
					cdiPath := filepath.Join(cdiDir,
						testDriverName+"_metadata_"+string(tc.uid)+"_"+baseReq+".json")
					if _, err := os.Stat(cdiPath); !os.IsNotExist(err) {
						t.Errorf("CDI spec file %s still exists", cdiPath)
					}
				}
			}
		})
	}
}

// TestUpdateRequestMetadata 验证 updateRequestMetadata 会覆写元数据文件并递增 generation。
// 覆盖：正常更新、claim 不存在、请求不存在、多次更新、子请求更新。
// 断言：generation 符合预期，或返回包含 "not found" 的错误。
func TestUpdateRequestMetadata(t *testing.T) {
	// 测试属性字符串值。
	strVal := "0000:03:00.0"
	// 带属性的设备，用于更新。
	devicesWithMetadata := []Device{{
		Requests:   []string{testRequest},
		PoolName:   "node-1",
		DeviceName: "gpu-0",
		Metadata: &DeviceMetadata{
			Attributes: map[string]resourceapi.DeviceAttribute{
				"pciBusID": {StringValue: &strVal},
			},
		},
	}}
	// 不带属性的设备，用于首次准备。
	devicesWithoutMetadata := []Device{{
		Requests:   []string{testRequest},
		PoolName:   "node-1",
		DeviceName: "gpu-0",
	}}

	// 子请求名。
	subreqRef := testRequest + "/high-memory"
	// 子请求：不带元数据的设备。
	subreqDevicesWithout := []Device{{
		Requests:   []string{subreqRef},
		PoolName:   "node-1",
		DeviceName: "gpu-0",
	}}
	// 子请求：带元数据的设备。
	subreqDevicesWith := []Device{{
		Requests:   []string{subreqRef},
		PoolName:   "node-1",
		DeviceName: "gpu-0",
		Metadata: &DeviceMetadata{
			Attributes: map[string]resourceapi.DeviceAttribute{
				"pciBusID": {StringValue: &strVal},
			},
		},
	}}

	testcases := map[string]struct {
		// prepareDevices 是初始准备的设备；preUpdates 是预先更新次数；claimUID 与 requestName 标识目标；
		// updateDevices 是更新用的设备；expectError 是期望的错误子串；expectedGeneration 是期望的 generation。
		prepareDevices     []Device
		preUpdates         int
		claimUID           types.UID
		requestName        string
		updateDevices      []Device
		expectError        string
		expectedGeneration int64
	}{
		// 正常更新：generation 从 1 变为 2。
		"updates-file-and-generation": {
			prepareDevices:     devicesWithoutMetadata,
			claimUID:           testClaimUID,
			requestName:        testRequest,
			updateDevices:      devicesWithMetadata,
			expectedGeneration: 2,
		},
		// claim 不存在：应报 not found。
		"unknown-claim": {
			claimUID:      "unknown-uid",
			requestName:   testRequest,
			updateDevices: devicesWithMetadata,
			expectError:   "not found",
		},
		// 请求不存在：应报 not found。
		"unknown-request": {
			prepareDevices: devicesWithMetadata,
			claimUID:       testClaimUID,
			requestName:    "nonexistent-request",
			updateDevices:  devicesWithMetadata,
			expectError:    "not found",
		},
		// 预先更新两次后 generation 累加到 4。
		"increments-generation-multiple-times": {
			prepareDevices:     devicesWithoutMetadata,
			preUpdates:         2,
			claimUID:           testClaimUID,
			requestName:        testRequest,
			updateDevices:      devicesWithMetadata,
			expectedGeneration: 4,
		},
		// 子请求更新同样递增 generation。
		"subrequest-update": {
			prepareDevices:     subreqDevicesWithout,
			claimUID:           testClaimUID,
			requestName:        subreqRef,
			updateDevices:      subreqDevicesWith,
			expectedGeneration: 2,
		},
	}

	// 逐用例执行。
	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			// 创建测试环境。
			w, pluginDir, _ := newTestWriter(t)

			// 先准备初始文件。
			if tc.prepareDevices != nil {
				if _, err := w.processPreparedClaim(testClaim(), tc.prepareDevices); err != nil {
					t.Fatalf("processPreparedClaim: %v", err)
				}
			}
			// 预先更新若干次，验证 generation 累加。
			for i := range tc.preUpdates {
				// 执行预更新。
				if err := w.updateRequestMetadata(
					claimRef{namespace: testClaimNS, name: testClaimName, uid: testClaimUID},
					tc.requestName, tc.updateDevices); err != nil {
					t.Fatalf("pre-updateRequestMetadata (%d): %v", i+1, err)
				}
			}

			// 执行被测试更新。
			err := w.updateRequestMetadata(
				claimRef{namespace: testClaimNS, name: testClaimName, uid: tc.claimUID},
				tc.requestName, tc.updateDevices)

			// 期望错误的用例。
			if tc.expectError != "" {
				// 应报错却返回 nil。
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tc.expectError)
				}
				// 断言错误信息包含预期子串。
				if !strings.Contains(err.Error(), tc.expectError) {
					t.Fatalf("expected error containing %q, got: %v", tc.expectError, err)
				}
				return
			}
			// 不期望错误却返回错误。
			if err != nil {
				t.Fatalf("updateRequestMetadata: %v", err)
			}

			// 构造期望元数据。
			expected := expectedRequestMetadata(tc.requestName, tc.updateDevices)
			// 设置期望的 generation。
			expected.ObjectMeta.Generation = tc.expectedGeneration

			// 基础请求名。
			baseReq := resourceclaim.BaseRequestRef(tc.requestName)
			// 元数据文件路径。
			metadataPath := filepath.Join(pluginDir, metadataSubDir,
				testClaimNS+"_"+testClaimName, baseReq, "metadata.json")
			// 读取更新后的文件。
			dm := readMetadataFile(t, metadataPath)
			// 断言文件内容与期望一致。
			if diff := cmp.Diff(expected, dm); diff != "" {
				t.Errorf("metadata mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
