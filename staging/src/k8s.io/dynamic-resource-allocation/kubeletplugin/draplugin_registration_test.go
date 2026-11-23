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
	"path"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/types"
)

// TestRollingUpdateRegistrarSocketFile_lengthBound 验证驱动名 + 完整 Pod UID 形式的注册 socket
// 文件名与注册表目录拼接后，完整路径不超过 AF_UNIX 路径长度上限 unixPathMax。
func TestRollingUpdateRegistrarSocketFile_lengthBound(t *testing.T) {
	// 典型的长驱动名与 UUID 形式的 Pod UID。
	driver := "gpu.dra-example-driver.sigs.k8s.io"
	podUID := types.UID("ad4af911-5bcd-47c5-a554-f35cea869472")
	// 生成候选文件名并拼接完整路径。
	name := RollingUpdateRegistrarSocketFile(KubeletRegistryDir, driver, podUID)
	full := path.Join(KubeletRegistryDir, name)
	// 断言路径长度在上限内，否则报告具体长度与路径。
	if len(full) > unixPathMax {
		t.Fatalf("registration socket path len %d exceeds %d: %q", len(full), unixPathMax, full)
	}
}

// TestRollingUpdateRegistrarSocketFile_prefersFullUID 验证驱动名较短时优先使用包含完整 Pod UID 的文件名。
func TestRollingUpdateRegistrarSocketFile_prefersFullUID(t *testing.T) {
	// 短驱动名。
	driver := "example.com/driver"
	podUID := types.UID("11111111-2222-3333-4444-555555555555")
	// 期望形式：<driver>-<完整 UID>-reg.sock。
	want := driver + "-" + string(podUID) + "-reg.sock"
	// 调用被测试函数。
	got := RollingUpdateRegistrarSocketFile(KubeletRegistryDir, driver, podUID)
	// 断言结果恰为完整 UID 形式。
	if got != want {
		t.Fatalf("expected full UID basename %q, got %q", want, got)
	}
}

// TestRollingUpdateRegistrarSocketFile_usesHashedUIDForLongDriverName 验证驱动名较长时退化为 UID 哈希形式：
// 保留驱动名前缀与 -reg.sock 后缀，但不再包含完整 UID。
func TestRollingUpdateRegistrarSocketFile_usesHashedUIDForLongDriverName(t *testing.T) {
	// 长驱动名与典型 UID。
	driver := "gpu.dra-example-driver.sigs.k8s.io"
	podUID := types.UID("ad4af911-5bcd-47c5-a554-f35cea869472")
	// 调用被测试函数。
	got := RollingUpdateRegistrarSocketFile(KubeletRegistryDir, driver, podUID)

	// 完整 UID 形式的候选名。
	fullUID := driver + "-" + string(podUID) + "-reg.sock"
	// 断言没有采用完整 UID 形式。
	if got == fullUID {
		t.Fatalf("expected hashed UID basename for long driver name, got full UID form %q", got)
	}
	// 断言仍保留驱动名前缀与 -reg.sock 后缀。
	if !strings.HasPrefix(got, driver+"-") || !strings.HasSuffix(got, "-reg.sock") {
		t.Fatalf("expected driver name prefix and -reg.sock suffix, got %q", got)
	}
	// 断言完整 UID 已被哈希替换。
	if strings.Contains(got, string(podUID)) {
		t.Fatalf("expected hashed UID, not full UID in %q", got)
	}
}

// TestRollingUpdateRegistrarSocketFile_usesShortestFormForVeryLongDriverName 验证驱动名极长时进一步退化为
// 最短形式：以 dra- 开头、-reg.sock 结尾，且完全省略驱动名。
func TestRollingUpdateRegistrarSocketFile_usesShortestFormForVeryLongDriverName(t *testing.T) {
	// 由 100 个 a 构成的极长驱动名。
	driver := strings.Repeat("a", 100)
	podUID := types.UID("ad4af911-5bcd-47c5-a554-f35cea869472")
	// 调用被测试函数。
	got := RollingUpdateRegistrarSocketFile(KubeletRegistryDir, driver, podUID)
	// 断言采用最短哈希形式的前后缀。
	if !strings.HasPrefix(got, "dra-") || !strings.HasSuffix(got, "-reg.sock") {
		t.Fatalf("expected shortest hashed basename, got %q", got)
	}
	// 断言结果中不再包含驱动名。
	if strings.Contains(got, driver) {
		t.Fatalf("expected driver name to be omitted from shortest form, got %q", got)
	}
}

// TestRollingUpdateRegistrarSocketFile_respectsCustomRegistryDir 验证更长的注册表目录会压缩驱动名可用空间：
// 完整 UID 形式超限时必须回退，且最终路径仍在上限内。
func TestRollingUpdateRegistrarSocketFile_respectsCustomRegistryDir(t *testing.T) {
	// 更长的注册表目录留给驱动名的空间更小。
	// A longer registry dir leaves less room for the driver name in the path.
	// 构造带长中间路径的注册表目录。
	registryDir := path.Join("/var/lib/kubelet", strings.Repeat("x", 20), "plugins_registry")
	// 短驱动名。
	driver := "example.com/driver"
	// 典型 UID。
	podUID := types.UID("11111111-2222-3333-4444-555555555555")

	// 完整 UID 形式的候选名。
	fullUID := driver + "-" + string(podUID) + "-reg.sock"
	// 测试前置条件：完整 UID 形式在该目录下确实超限。
	if len(path.Join(registryDir, fullUID)) <= unixPathMax {
		t.Fatalf("test setup: expected full UID form to exceed limit with custom registry dir")
	}

	// 调用被测试函数。
	got := RollingUpdateRegistrarSocketFile(registryDir, driver, podUID)
	// 断言最终路径不超过上限。
	if len(path.Join(registryDir, got)) > unixPathMax {
		t.Fatalf("expected path within limit, got len %d: %q", len(path.Join(registryDir, got)), got)
	}
	// 断言确实发生了回退。
	if got == fullUID {
		t.Fatalf("expected fallback from full UID form, got %q", got)
	}
}

// TestRollingUpdateRegistrarSocketFile_deterministic 验证相同输入产生稳定一致的文件名。
func TestRollingUpdateRegistrarSocketFile_deterministic(t *testing.T) {
	// 短驱动名与典型 UID。
	driver := "example.com/driver"
	podUID := types.UID("11111111-2222-3333-4444-555555555555")
	// 连续两次调用。
	a := RollingUpdateRegistrarSocketFile(KubeletRegistryDir, driver, podUID)
	b := RollingUpdateRegistrarSocketFile(KubeletRegistryDir, driver, podUID)
	// 断言两次结果相同。
	if a != b {
		t.Fatalf("expected stable name, got %q vs %q", a, b)
	}
}

// TestRollingUpdateRegistrarSocketFile_distinctInputs 验证不同驱动名或不同 Pod UID 不会产生相同文件名，
// 避免滚动更新时多个 Pod 争用同一个注册 socket。
func TestRollingUpdateRegistrarSocketFile_distinctInputs(t *testing.T) {
	// 固定 UID。
	uid := types.UID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
	// 驱动名不同。
	a := RollingUpdateRegistrarSocketFile(KubeletRegistryDir, "driver.one", uid)
	// 另一个驱动名。
	b := RollingUpdateRegistrarSocketFile(KubeletRegistryDir, "driver.two", uid)
	// 断言不同驱动名不冲突。
	if a == b {
		t.Fatalf("different driver names should not collide: %q", a)
	}
	// 驱动名相同但 Pod UID 不同。
	c := RollingUpdateRegistrarSocketFile(KubeletRegistryDir, "driver.one", "ffffffff-ffff-ffff-ffff-ffffffffffff")
	// 断言不同 UID 不冲突。
	if a == c {
		t.Fatalf("different pod UIDs should not collide: %q", a)
	}
}
