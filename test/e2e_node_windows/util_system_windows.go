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
	"runtime"
	"sort"
	"strings"
	"unsafe"

	"golang.org/x/sys/windows"
)

// SYSTEM 模拟。hcsshim/HCS 创建的容器 silo job object
// 的安全描述符只授权 NT AUTHORITY\SYSTEM 和
// 容器自身上下文访问，不授权 Administrators。因此一个已提升但非
// SYSTEM 的进程在打开时会被拒绝，无论请求何种访问
// 掩码（SeDebugPrivilege 也无济于事：它只对
// OpenProcess/OpenThread 绕过 DACL，对 job object 无效）。runAsSystem 短暂使用
// SYSTEM token，使宿主机侧的 affinity 读取能打开 silo。
//
// 要求测试套件以提升权限运行：Administrators 持有 SeDebugPrivilege
//（用于打开 SYSTEM 进程）和 SeImpersonatePrivilege（用于设置线程
// token) by default.

const systemSIDString = "S-1-5-18" // NT AUTHORITY\SYSTEM

// runAsSystem 在当前 OS 线程模拟 SYSTEM 时运行 fn，然后
// 恢复。期间 goroutine 锁定到其线程，因为
// SetThreadToken/RevertToSelf 是线程级的：锁定确保 fn() 在
// 模拟 token 下运行，且恢复的是被修改的同一线程。
func runAsSystem(fn func() error) (err error) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	if err := enablePrivilege(seDebugName); err != nil {
		return fmt.Errorf("enable SeDebugPrivilege: %w", err)
	}

	sysTok, err := duplicateSystemToken()
	if err != nil {
		return err
	}
	defer sysTok.Close()

	if err := windows.SetThreadToken(nil, sysTok); err != nil {
		return fmt.Errorf("SetThreadToken(SYSTEM): %w", err)
	}
	defer func() {
		if rerr := windows.RevertToSelf(); rerr != nil && err == nil {
			err = fmt.Errorf("RevertToSelf: %w", rerr)
		}
	}()

	return fn()
}

const seDebugName = "SeDebugPrivilege"

// enablePrivilege 在当前进程 token 上启用指定特权。如果
// 特权已启用，则相当于空操作。
func enablePrivilege(name string) error {
	var tok windows.Token
	if err := windows.OpenProcessToken(windows.CurrentProcess(),
		windows.TOKEN_ADJUST_PRIVILEGES|windows.TOKEN_QUERY, &tok); err != nil {
		return fmt.Errorf("OpenProcessToken: %w", err)
	}
	defer tok.Close()

	namePtr, err := windows.UTF16PtrFromString(name)
	if err != nil {
		return err
	}
	var luid windows.LUID
	if err := windows.LookupPrivilegeValue(nil, namePtr, &luid); err != nil {
		return fmt.Errorf("LookupPrivilegeValue(%s): %w", name, err)
	}
	tp := windows.Tokenprivileges{
		PrivilegeCount: 1,
		Privileges:     [1]windows.LUIDAndAttributes{{Luid: luid, Attributes: windows.SE_PRIVILEGE_ENABLED}},
	}
	// AdjustTokenPrivileges 即使未持有特权也会返回成功
	//（此时 GetLastError 为 ERROR_NOT_ALL_ASSIGNED）；这种情况下后续
	// 对 SYSTEM 进程的 OpenProcess 会失败，并在那里体现。
	if err := windows.AdjustTokenPrivileges(tok, false, &tp, 0, nil, nil); err != nil {
		return fmt.Errorf("AdjustTokenPrivileges(%s): %w", name, err)
	}
	return nil
}

// duplicateSystemToken 从 SYSTEM 进程复制一个模拟 token。
// 它优先选择已知的宿主机（session-0）SYSTEM 进程：从
// 容器 silo *内部*运行的进程复制的 token 只在 silo 内有效，无法
// 打开另一个容器的 silo job object（STATUS_ACCESS_DENIED），即使
// 其用户是 S-1-5-18。services.exe 等宿主机进程产生的 token
// 具有宿主机范围，可打开任意 silo。
func duplicateSystemToken() (windows.Token, error) {
	systemSID, err := windows.StringToSid(systemSIDString)
	if err != nil {
		return 0, err
	}

	snap, err := windows.CreateToolhelp32Snapshot(windows.TH32CS_SNAPPROCESS, 0)
	if err != nil {
		return 0, fmt.Errorf("CreateToolhelp32Snapshot: %w", err)
	}
	defer windows.CloseHandle(snap)

	type proc struct {
		pid  uint32
		name string
	}
	var procs []proc
	var pe windows.ProcessEntry32
	pe.Size = uint32(unsafe.Sizeof(pe))
	for err = windows.Process32First(snap, &pe); err == nil; err = windows.Process32Next(snap, &pe) {
		if pe.ProcessID == 0 {
			continue
		}
		procs = append(procs, proc{pe.ProcessID, strings.ToLower(windows.UTF16ToString(pe.ExeFile[:]))})
	}

	// 优先使用宿主机 SYSTEM 进程，其他作为回退。
	preferred := map[string]int{"services.exe": 0, "winlogon.exe": 1, "wininit.exe": 2, "lsass.exe": 3}
	sort.SliceStable(procs, func(i, j int) bool {
		pi, oki := preferred[procs[i].name]
		pj, okj := preferred[procs[j].name]
		if oki != okj {
			return oki // 优先项排在前面
		}
		return pi < pj
	})

	for _, p := range procs {
		if dup, ok := trySystemTokenFromPID(p.pid, systemSID); ok {
			return dup, nil
		}
	}
	return 0, fmt.Errorf("could not duplicate a SYSTEM token from any process " +
		"(is the suite running elevated with SeDebugPrivilege?)")
}

// 当且仅当 pid 进程以 systemSID 运行时，trySystemTokenFromPID 从该进程
// 复制模拟 token。任何失败都返回 ok=false（无错误），
// 以便调用方尝试下一个进程：某些受保护的 SYSTEM 进程
// （如 csrss.exe）即使有 SeDebugPrivilege 也拒绝 TOKEN_DUPLICATE。
func trySystemTokenFromPID(pid uint32, systemSID *windows.SID) (windows.Token, bool) {
	ph, err := windows.OpenProcess(windows.PROCESS_QUERY_LIMITED_INFORMATION, false, pid)
	if err != nil {
		return 0, false
	}
	defer windows.CloseHandle(ph)

	var procTok windows.Token
	if err := windows.OpenProcessToken(ph, windows.TOKEN_DUPLICATE|windows.TOKEN_QUERY, &procTok); err != nil {
		return 0, false
	}
	defer procTok.Close()

	tu, err := procTok.GetTokenUser()
	if err != nil || !tu.User.Sid.Equals(systemSID) {
		return 0, false
	}

	var dup windows.Token
	if err := windows.DuplicateTokenEx(procTok,
		windows.TOKEN_QUERY|windows.TOKEN_IMPERSONATE|windows.TOKEN_DUPLICATE,
		nil, windows.SecurityImpersonation, windows.TokenImpersonation, &dup); err != nil {
		return 0, false
	}
	return dup, true
}
