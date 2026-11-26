//go:build windows

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

package e2enodewindows

import (
	"fmt"
	"math/bits"
	"unsafe"

	"golang.org/x/sys/windows"

	v1 "k8s.io/api/core/v1"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

// 主机侧 job-object 校验直接读取 Win32 内核状态，
// 补充本套件其他地方使用的 CRI ContainerStatus 回读。CRI 校验的是 "containerd 记住了 kubelet
// 的请求"；这里的校验是 "内核真正应用了它"。
//
//
// 范围：仅限 WCOW 进程隔离容器。命名约定
// "\Container_<full-id>" 是 containerd/hcsshim 内部约定，这里
// 只作为诊断的事实来源，与 hcsshim 自己的
// 测试套件做法一致。

const jobObjectQueryAccess = 0x0004 // JOB_OBJECT_QUERY

var (
	modntdll            = windows.NewLazySystemDLL("ntdll.dll")
	procNtOpenJobObject = modntdll.NewProc("NtOpenJobObject")
)

// groupAffinity 对应 Win32 GROUP_AFFINITY（winnt.h）。KAFFINITY 是
// ULONG_PTR，在 64 位 Go 中是 uintptr。
type groupAffinity struct {
	Mask     uintptr
	Group    uint16
	Reserved [3]uint16
}

// openJobObject 按对象管理器名称（例如
// "\Container_<id>"）打开 container silo job object。它使用原生 NtOpenJobObject，而不是
// Win32 OpenJobObjectW：silo 位于 NT 对象命名空间的根目录，而 OpenJobObjectW 只解析
// BaseNamedObjects 下的名称，并拒绝包含反斜杠的名称（会返回
// ERROR_BAD_PATHNAME "the specified path is invalid"）。找不到该 job object 时返回
// NTStatus 为 windows.STATUS_OBJECT_NAME_NOT_FOUND 的错误。

func openJobObject(name string) (windows.Handle, error) {
	uname, err := windows.NewNTUnicodeString(name)
	if err != nil {
		return 0, err
	}
	oa := windows.OBJECT_ATTRIBUTES{
		ObjectName: uname,
		Attributes: windows.OBJ_CASE_INSENSITIVE,
	}
	oa.Length = uint32(unsafe.Sizeof(oa))

	var h windows.Handle
	// NTSTATUS NtOpenJobObject(PHANDLE, ACCESS_MASK, POBJECT_ATTRIBUTES)
	r, _, _ := procNtOpenJobObject.Call(
		uintptr(unsafe.Pointer(&h)),
		uintptr(jobObjectQueryAccess),
		uintptr(unsafe.Pointer(&oa)),
	)
	if status := windows.NTStatus(r); status != windows.STATUS_SUCCESS {
		return 0, fmt.Errorf("NtOpenJobObject(%s): %w", name, status)
	}
	return h, nil
}

// getHostJobAffinity 打开 containerd 为进程隔离容器创建的 job object，
// 读取其 JobObjectGroupInformationEx
// （Windows 内核当前应用的 GROUP_AFFINITY 数组）。
func getHostJobAffinity(pod *v1.Pod, ctnName string) ([]groupAffinity, error) {
	cntID, err := containerIDForContainer(pod, ctnName)
	if err != nil {
		return nil, err
	}

	// silo job object 只有 SYSTEM 可读（见 util_system_windows.go），
	// 因此要模拟 SYSTEM 来打开并查询。句柄的访问权限
	// 在打开时校验，因此在同一模拟上下文中读取
	// 才是这里的关键。
	var out []groupAffinity
	err = runAsSystem(func() error {
		h, err := openJobObject(`\Container_` + cntID)
		if err != nil {
			return err
		}
		defer windows.CloseHandle(h)

		// JobObjectGroupInformationEx 返回扁平的 GROUP_AFFINITY 数组。
		// 64 项覆盖 Windows 处理器组的架构上限，
		// 远超任何真实系统的规模。
		const maxGroups = 64
		entrySize := int(unsafe.Sizeof(groupAffinity{}))
		buf := make([]byte, maxGroups*entrySize)

		var returned uint32
		if err := windows.QueryInformationJobObject(
			h, windows.JobObjectGroupInformationEx,
			uintptr(unsafe.Pointer(&buf[0])), uint32(len(buf)), &returned,
		); err != nil {
			return fmt.Errorf("QueryInformationJobObject(JobObjectGroupInformationEx): %w", err)
		}

		count := int(returned) / entrySize
		out = make([]groupAffinity, count)
		for i := 0; i < count; i++ {
			out[i] = *(*groupAffinity)(unsafe.Pointer(&buf[i*entrySize]))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// countCPUsInHostJobAffinity 对 getHostJobAffinity 返回的所有 GROUP_AFFINITY
// 项中的置位求和。
func countCPUsInHostJobAffinity(affs []groupAffinity) int {
	total := 0
	for _, a := range affs {
		total += bits.OnesCount64(uint64(a.Mask))
	}
	return total
}

// 两个主机 job-object 亲和性至少共享一个 CPU（同一处理器组中同一位置置位）时，
// hostJobAffinitiesOverlap 返回 true。它是
// windowsAffinitiesOverlap（比较 CRI mask）在主机侧的对应实现。
func hostJobAffinitiesOverlap(a, b []groupAffinity) bool {
	maskA := map[uint16]uint64{}
	for _, x := range a {
		maskA[x.Group] |= uint64(x.Mask)
	}
	for _, x := range b {
		if maskA[x.Group]&uint64(x.Mask) != 0 {
			return true
		}
	}
	return false
}

// 主机 job-object 亲和性与 CRI 上报的亲和性在每个处理器组都逐位一致时，
// hostJobAffinityMatchesCRI 返回 nil。
// 逐位一致既证明运行时应用了
// kubelet 的请求，也证明 CRI 回读是忠实的。
func hostJobAffinityMatchesCRI(host []groupAffinity, cri []*runtimeapi.WindowsCpuGroupAffinity) error {
	hostByGroup := map[uint16]uint64{}
	for _, a := range host {
		hostByGroup[a.Group] |= uint64(a.Mask)
	}
	criByGroup := map[uint16]uint64{}
	for _, a := range cri {
		criByGroup[uint16(a.CpuGroup)] |= a.CpuMask
	}
	for g, hMask := range hostByGroup {
		if criByGroup[g] != hMask {
			return fmt.Errorf("group %d: host mask=0x%x, CRI mask=0x%x", g, hMask, criByGroup[g])
		}
	}
	for g, cMask := range criByGroup {
		if _, ok := hostByGroup[g]; !ok && cMask != 0 {
			return fmt.Errorf("group %d: CRI mask=0x%x, host has no affinity in this group", g, cMask)
		}
	}
	return nil
}

// validateHostJobAffinityProcessIsolated 把进程隔离容器的主机 job-object 亲和性
// 与 CRI 上报的亲和性做交叉校验。

func validateHostJobAffinityProcessIsolated(pod *v1.Pod, ctnName string, cri []*runtimeapi.WindowsCpuGroupAffinity) error {
	host, err := getHostJobAffinity(pod, ctnName)
	if err != nil {
		return err
	}
	if got, want := countCPUsInHostJobAffinity(host), countCPUsInAffinities(cri); got != want {
		return fmt.Errorf("CPU count disagreement: host job=%d, CRI=%d", got, want)
	}
	return hostJobAffinityMatchesCRI(host, cri)
}
