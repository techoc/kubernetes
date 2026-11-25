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

package deviceattribute

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"k8s.io/utils/ptr"

	resourceapi "k8s.io/api/resource/v1"
)

// TestGetPCIeRootAttributePCIBusID 表驱动测试 GetPCIeRootAttributeByPCIBusID。
// 输入为 PCI BDF 地址及模拟的 sysfs 符号链接，覆盖合法链接、空/非法地址、不存在的设备、
// 链接目标前缀错误与后缀错误等场景；断言返回的字符串属性或错误信息符合预期。
func TestGetPCIeRootAttributePCIBusID(t *testing.T) {
	pcieRoot := "pci0000:00"
	intermediateBusID := "0000:01:00.0"
	pciBusID := "0000:02:00.0"
	// 合法输入下期望得到 pci0000:00 的字符串属性。
	expectedAttribute := DeviceAttribute{
		Name:  StandardDeviceAttributePCIeRoot,
		Value: resourceapi.DeviceAttribute{StringValue: ptr.To(pcieRoot)},
	}

	tests := map[string]struct {
		testMachineSetup  func(t *testing.T, testRootPath string)
		address           string
		expectedAttribute *DeviceAttribute
		expectsError      bool
		expectedErrMsg    string
	}{
		"valid": {
			testMachineSetup: func(t *testing.T, testRootPath string) {
				relDevicePath := filepath.Join("devices", pcieRoot, intermediateBusID, pciBusID)
				touchFile(t, filepath.Join(testRootPath, relDevicePath))
				busDir := filepath.Join(testRootPath, "bus", "pci", "devices")
				mkDirAll(t, busDir)
				createSymlink(t,
					filepath.Join("..", "..", "..", relDevicePath),
					filepath.Join(busDir, pciBusID),
				)
			},
			address:           pciBusID,
			expectedAttribute: &expectedAttribute,
			expectsError:      false,
		},
		"invalid empty PCI Bus ID": {
			address:           "",
			expectedAttribute: nil,
			expectsError:      true,
			expectedErrMsg:    "PCI Bus ID cannot be empty",
		},
		"invalid PCI Bus ID format": {
			address:           "invalid-pci-id",
			expectedAttribute: nil,
			expectsError:      true,
			expectedErrMsg:    "invalid PCI Bus ID format: invalid-pci-id",
		},
		"invalid no exist PCI Bus ID": {
			address:           pciBusID,
			expectedAttribute: nil,
			expectsError:      true,
			expectedErrMsg:    "no such file or directory",
		},
		"invalid symlink (invalid prefix)": {
			testMachineSetup: func(t *testing.T, testRootPath string) {
				relDevicePath := filepath.Join("devices", "invalid-pci-root", intermediateBusID, pciBusID)
				touchFile(t, filepath.Join(testRootPath, relDevicePath))
				busDir := filepath.Join(testRootPath, "bus", "pci", "devices")
				mkDirAll(t, busDir)
				createSymlink(t,
					filepath.Join("..", "..", "..", relDevicePath),
					filepath.Join(busDir, pciBusID),
				)
			},
			address:           pciBusID,
			expectedAttribute: nil,
			expectsError:      true,
			expectedErrMsg:    fmt.Sprintf("symlink target for PCI Bus ID %s is invalid: it must start with", pciBusID),
		},
		"invalid symlink (invalid suffix)": {
			testMachineSetup: func(t *testing.T, testRootPath string) {
				relDevicePath := filepath.Join("devices", pcieRoot, intermediateBusID, "0000:00:13.1") // 不同的 PCI 总线 ID
				touchFile(t, filepath.Join(testRootPath, relDevicePath))
				busDir := filepath.Join(testRootPath, "bus", "pci", "devices")
				mkDirAll(t, busDir)
				createSymlink(t,
					filepath.Join("..", "..", "..", relDevicePath),
					filepath.Join(busDir, pciBusID),
				)
			},
			address:           pciBusID,
			expectedAttribute: nil,
			expectsError:      true,
			expectedErrMsg:    fmt.Sprintf("symlink target for PCI Bus ID %s is invalid: it must end with", pciBusID),
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			// 每个用例使用独立临时目录，由 setup 构造设备文件与 bus/pci/devices 下的符号链接。
			testMachinePath := t.TempDir()
			if test.testMachineSetup != nil {
				test.testMachineSetup(t, testMachinePath)
			}
			// 把被测函数指向模拟的 sysfs 根目录。
			got, err := GetPCIeRootAttributeByPCIBusID(test.address, WithFSFromRoot(testMachinePath))
			if test.expectsError {
				// 错误路径：断言确实出错且错误信息包含预期子串。
				if err == nil {
					t.Errorf("Expected error but got none")
					return
				}
				if !strings.Contains(err.Error(), test.expectedErrMsg) {
					t.Errorf("Expected error message to contain %q, got %q", test.expectedErrMsg, err.Error())
					return
				}
				return
			}
			// 成功路径：断言无错误且返回属性与期望一致。
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if !reflect.DeepEqual(got, *test.expectedAttribute) {
				t.Errorf("Expected attribute %v, got %v", test.expectedAttribute, got)
			}
		})
	}
}

// TestGetPCIBusIDAttribute 表驱动测试 GetPCIBusIDAttribute：
// 该函数只做 BDF 格式校验并原样封装为字符串属性，故用例覆盖合法地址、空地址与非法格式；
// 断言成功时返回属性正确，失败时错误信息包含预期子串。
func TestGetPCIBusIDAttribute(t *testing.T) {
	pciBusID := "0000:02:00.0"
	// 合法输入下期望原样返回该 BDF 字符串属性。
	expectedAttribute := DeviceAttribute{
		Name:  StandardDeviceAttributePCIBusID,
		Value: resourceapi.DeviceAttribute{StringValue: ptr.To(pciBusID)},
	}

	tests := map[string]struct {
		address           string
		expectedAttribute *DeviceAttribute
		expectsError      bool
		expectedErrMsg    string
	}{
		"valid": {
			address:           pciBusID,
			expectedAttribute: &expectedAttribute,
			expectsError:      false,
		},
		"invalid empty PCI Bus ID": {
			address:           "",
			expectedAttribute: nil,
			expectsError:      true,
			expectedErrMsg:    "PCI Bus ID cannot be empty",
		},
		"invalid PCI Bus ID format": {
			address:           "invalid-pci-id",
			expectedAttribute: nil,
			expectsError:      true,
			expectedErrMsg:    "invalid PCI Bus ID format: invalid-pci-id",
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := GetPCIBusIDAttribute(test.address)
			if test.expectsError {
				// 错误路径：断言出错且错误信息包含预期子串。
				if err == nil {
					t.Errorf("Expected error but got none")
					return
				}
				if !strings.Contains(err.Error(), test.expectedErrMsg) {
					t.Errorf("Expected error message to contain %q, got %q", test.expectedErrMsg, err.Error())
					return
				}
				return
			}
			// 成功路径：断言返回的 BDF 字符串属性与期望一致。
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if !reflect.DeepEqual(got, *test.expectedAttribute) {
				t.Errorf("Expected attribute %v, got %v", test.expectedAttribute, got)
			}
		})
	}
}

// mkDirAll 是测试辅助函数：递归创建目录，失败即终止测试。
func mkDirAll(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(path, 0755); err != nil {
		t.Fatalf("Failed to create directory %s: %v", path, err)
	}
}

// touchFile 是测试辅助函数：先创建父目录，再创建一个空文件，失败即终止测试。
func touchFile(t *testing.T, path string) {
	t.Helper()
	mkDirAll(t, filepath.Dir(path))
	if _, err := os.Create(path); err != nil {
		t.Fatalf("Failed to create file %s: %v", path, err)
	}
}

// createSymlink 是测试辅助函数：创建从 link 指向 target 的符号链接，失败即终止测试。
func createSymlink(t *testing.T, target, link string) {
	t.Helper()
	if err := os.Symlink(target, link); err != nil {
		t.Fatalf("Failed to create symlink from %s to %s: %v", target, link, err)
	}
}
