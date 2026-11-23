/*
Copyright 2014 The Kubernetes Authors.

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

package mount

import (
	"fmt"
	"runtime"
	"strings"
	"testing"

	"k8s.io/utils/exec"
	testingexec "k8s.io/utils/exec/testing"
)

// ErrorMounter 按调用顺序返回预设错误，用来模拟挂载在不同阶段失败。
// errIndex 指向下一次挂载应返回的错误；err 可短于调用次数，其余情况走 FakeMounter 的默认行为。
type ErrorMounter struct {
	*FakeMounter
	errIndex int
	err      []error
}

// Mount 保持与 MountSensitive 相同的失败序列，便于两种入口共用用例。
func (mounter *ErrorMounter) Mount(source string, target string, fstype string, options []string) error {
	return mounter.MountSensitive(source, target, fstype, options, nil /* sensitiveOptions */)
}

// MountSensitive 先消费预设错误再回退到 FakeMounter，避免污染真实挂载状态。
func (mounter *ErrorMounter) MountSensitive(source string, target string, fstype string, options []string, sensitiveOptions []string) error {
	i := mounter.errIndex
	mounter.errIndex++
	if mounter.err != nil && mounter.err[i] != nil {
		return mounter.err[i]
	}
	return mounter.FakeMounter.Mount(source, target, fstype, options)
}

// ExecArgs 描述一次预期命令及其输出，用于构造 FakeExec 的脚本。
// command 为预期执行的命令名，args 为预期参数且顺序必须与实际调用一致；
// output 作为命令的标准输出返回，err 模拟命令失败，例如 blkid 的退出码。
type ExecArgs struct {
	command string
	args    []string
	output  string
	err     error
}

// TestSafeFormatAndMount 用假命令覆盖探测、格式化和挂载全流程，同时校验敏感选项脱敏。
func TestSafeFormatAndMount(t *testing.T) {
	if runtime.GOOS == "darwin" || runtime.GOOS == "windows" {
		t.Skipf("not supported on GOOS=%s", runtime.GOOS)
	}
	tests := []struct {
		description           string
		fstype                string
		mountOptions          []string
		sensitiveMountOptions []string
		formatOptions         []string
		execScripts           []ExecArgs
		mountErrs             []error
		expErrorType          MountErrorType
	}{
		{
			description:  "Test a read only mount of an already formatted device",
			fstype:       "ext4",
			mountOptions: []string{"ro"},
			execScripts: []ExecArgs{
				{"blkid", []string{"-p", "-s", "TYPE", "-s", "PTTYPE", "-o", "export", "/dev/foo"}, "DEVNAME=/dev/foo\nTYPE=ext4\n", nil},
			},
		},
		{
			description: "Test a normal mount of an already formatted device",
			fstype:      "ext4",
			execScripts: []ExecArgs{
				{"blkid", []string{"-p", "-s", "TYPE", "-s", "PTTYPE", "-o", "export", "/dev/foo"}, "DEVNAME=/dev/foo\nTYPE=ext4\n", nil},
				{"fsck", []string{"-a", "/dev/foo"}, "", nil},
			},
		},
		{
			description:  "Test a read only mount of unformatted device",
			fstype:       "ext4",
			mountOptions: []string{"ro"},
			execScripts: []ExecArgs{
				{"blkid", []string{"-p", "-s", "TYPE", "-s", "PTTYPE", "-o", "export", "/dev/foo"}, "", &testingexec.FakeExitError{Status: 2}},
			},
			expErrorType: UnformattedReadOnly,
		},
		{
			description: "Test a normal mount of unformatted device",
			fstype:      "ext4",
			execScripts: []ExecArgs{
				{"blkid", []string{"-p", "-s", "TYPE", "-s", "PTTYPE", "-o", "export", "/dev/foo"}, "", &testingexec.FakeExitError{Status: 2}},
				{"mkfs.ext4", []string{"-F", "-m0", "/dev/foo"}, "", nil},
			},
		},
		{
			description: "Test 'fsck' fails with exit status 4",
			fstype:      "ext4",
			execScripts: []ExecArgs{
				{"blkid", []string{"-p", "-s", "TYPE", "-s", "PTTYPE", "-o", "export", "/dev/foo"}, "DEVNAME=/dev/foo\nTYPE=ext4\n", nil},
				{"fsck", []string{"-a", "/dev/foo"}, "", &testingexec.FakeExitError{Status: 4}},
			},
			expErrorType: HasFilesystemErrors,
		},
		{
			description: "Test 'fsck' fails with exit status 1 (errors found and corrected)",
			fstype:      "ext4",
			execScripts: []ExecArgs{
				{"blkid", []string{"-p", "-s", "TYPE", "-s", "PTTYPE", "-o", "export", "/dev/foo"}, "DEVNAME=/dev/foo\nTYPE=ext4\n", nil},
				{"fsck", []string{"-a", "/dev/foo"}, "", &testingexec.FakeExitError{Status: 1}},
			},
		},
		{
			description: "Test 'fsck' fails with exit status other than 1 and 4 (likely unformatted device)",
			fstype:      "ext4",
			execScripts: []ExecArgs{
				{"blkid", []string{"-p", "-s", "TYPE", "-s", "PTTYPE", "-o", "export", "/dev/foo"}, "DEVNAME=/dev/foo\nTYPE=ext4\n", nil},
				{"fsck", []string{"-a", "/dev/foo"}, "", &testingexec.FakeExitError{Status: 8}},
			},
		},
		{
			description: "Test that 'blkid' is called and fails",
			fstype:      "ext4",
			mountErrs:   []error{fmt.Errorf("unknown filesystem type '(null)'")},
			execScripts: []ExecArgs{
				{"blkid", []string{"-p", "-s", "TYPE", "-s", "PTTYPE", "-o", "export", "/dev/foo"}, "DEVNAME=/dev/foo\nPTTYPE=dos\n", nil},
				{"fsck", []string{"-a", "/dev/foo"}, "", nil},
			},
			expErrorType: FilesystemMismatch,
		},
		{
			description: "Test that 'blkid' is called and confirms unformatted disk, format fails",
			fstype:      "ext4",
			mountErrs:   []error{fmt.Errorf("unknown filesystem type '(null)'")},
			execScripts: []ExecArgs{
				{"blkid", []string{"-p", "-s", "TYPE", "-s", "PTTYPE", "-o", "export", "/dev/foo"}, "", &testingexec.FakeExitError{Status: 2}},
				{"mkfs.ext4", []string{"-F", "-m0", "/dev/foo"}, "", fmt.Errorf("formatting failed")},
			},
			expErrorType: FormatFailed,
		},
		{
			description: "Test that 'blkid' is called and confirms unformatted disk, format passes, second mount fails",
			fstype:      "ext4",
			mountErrs:   []error{fmt.Errorf("unknown filesystem type '(null)'")},
			execScripts: []ExecArgs{
				{"blkid", []string{"-p", "-s", "TYPE", "-s", "PTTYPE", "-o", "export", "/dev/foo"}, "", &testingexec.FakeExitError{Status: 2}},
				{"mkfs.ext4", []string{"-F", "-m0", "/dev/foo"}, "", nil},
			},
			expErrorType: UnknownMountError,
		},
		{
			description: "Test that 'blkid' is called and confirms unformatted disk, format passes, mount passes",
			fstype:      "ext4",
			execScripts: []ExecArgs{
				{"blkid", []string{"-p", "-s", "TYPE", "-s", "PTTYPE", "-o", "export", "/dev/foo"}, "", &testingexec.FakeExitError{Status: 2}},
				{"mkfs.ext4", []string{"-F", "-m0", "/dev/foo"}, "", nil},
			},
		},
		{
			description: "Test that 'blkid' is called and confirms unformatted disk, format passes, mount passes with ext3",
			fstype:      "ext3",
			execScripts: []ExecArgs{
				{"blkid", []string{"-p", "-s", "TYPE", "-s", "PTTYPE", "-o", "export", "/dev/foo"}, "", &testingexec.FakeExitError{Status: 2}},
				{"mkfs.ext3", []string{"-F", "-m0", "/dev/foo"}, "", nil},
			},
		},
		{
			description: "test that none ext4 fs does not get called with ext4 options.",
			fstype:      "xfs",
			execScripts: []ExecArgs{
				{"blkid", []string{"-p", "-s", "TYPE", "-s", "PTTYPE", "-o", "export", "/dev/foo"}, "", &testingexec.FakeExitError{Status: 2}},
				{"mkfs.xfs", []string{"-f", "/dev/foo"}, "", nil},
			},
		},
		{
			description: "Test that 'blkid' is called and reports ext4 partition",
			fstype:      "ext4",
			execScripts: []ExecArgs{
				{"blkid", []string{"-p", "-s", "TYPE", "-s", "PTTYPE", "-o", "export", "/dev/foo"}, "DEVNAME=/dev/foo\nTYPE=ext4\n", nil},
				{"fsck", []string{"-a", "/dev/foo"}, "", nil},
			},
		},
		{
			description: "Test that 'blkid' is called but has some usage or other errors (an exit code of 4 is returned)",
			fstype:      "xfs",
			mountErrs:   []error{fmt.Errorf("unknown filesystem type '(null)'"), nil},
			execScripts: []ExecArgs{
				{"blkid", []string{"-p", "-s", "TYPE", "-s", "PTTYPE", "-o", "export", "/dev/foo"}, "", &testingexec.FakeExitError{Status: 4}},
				{"mkfs.xfs", []string{"-f", "/dev/foo"}, "", nil},
			},
			expErrorType: GetDiskFormatFailed,
		},
		{
			description:           "Test that 'blkid' is called and confirms unformatted disk, format fails with sensitive options",
			fstype:                "ext4",
			sensitiveMountOptions: []string{"mySecret"},
			mountErrs:             []error{fmt.Errorf("unknown filesystem type '(null)'")},
			execScripts: []ExecArgs{
				{"blkid", []string{"-p", "-s", "TYPE", "-s", "PTTYPE", "-o", "export", "/dev/foo"}, "", &testingexec.FakeExitError{Status: 2}},
				{"mkfs.ext4", []string{"-F", "-m0", "/dev/foo"}, "", fmt.Errorf("formatting failed")},
			},
			expErrorType: FormatFailed,
		},
		{
			description:   "Test that 'blkid' is called and confirms unformatted disk, format passes, mount passes (with format options)",
			fstype:        "ext4",
			formatOptions: []string{"-b", "1024"},
			execScripts: []ExecArgs{
				{"blkid", []string{"-p", "-s", "TYPE", "-s", "PTTYPE", "-o", "export", "/dev/foo"}, "", &testingexec.FakeExitError{Status: 2}},
				{"mkfs.ext4", []string{"-b", "1024", "-F", "-m0", "/dev/foo"}, "", nil},
			},
		},
	}

	for _, test := range tests {
		fakeMounter := ErrorMounter{NewFakeMounter(nil), 0, test.mountErrs}
		fakeExec := &testingexec.FakeExec{ExactOrder: true}
		for _, script := range test.execScripts {
			fakeCmd := &testingexec.FakeCmd{}
			cmdAction := makeFakeCmd(fakeCmd, script.command, script.args...)
			outputAction := makeFakeOutput(script.output, script.err)
			fakeCmd.CombinedOutputScript = append(fakeCmd.CombinedOutputScript, outputAction)
			fakeExec.CommandScript = append(fakeExec.CommandScript, cmdAction)
		}
		mounter := SafeFormatAndMount{
			Interface: &fakeMounter,
			Exec:      fakeExec,
		}

		device := "/dev/foo"
		dest := t.TempDir()
		var err error
		if len(test.formatOptions) > 0 {
			err = mounter.FormatAndMountSensitiveWithFormatOptions(device, dest, test.fstype, test.mountOptions, test.sensitiveMountOptions, test.formatOptions)
		} else if len(test.sensitiveMountOptions) == 0 {
			err = mounter.FormatAndMount(device, dest, test.fstype, test.mountOptions)
		} else {
			err = mounter.FormatAndMountSensitive(device, dest, test.fstype, test.mountOptions, test.sensitiveMountOptions)
		}
		if len(test.expErrorType) == 0 {
			if err != nil {
				t.Errorf("test \"%s\" unexpected non-error: %v", test.description, err)
			}

			// Check that something was mounted on the directory
			isNotMountPoint, err := fakeMounter.IsLikelyNotMountPoint(dest)
			if err != nil || isNotMountPoint {
				t.Errorf("test \"%s\" the directory was not mounted", test.description)
			}

			// check that the correct device was mounted
			mountedDevice, _, err := GetDeviceNameFromMount(fakeMounter.FakeMounter, dest)
			if err != nil || mountedDevice != device {
				t.Errorf("test \"%s\" the correct device was not mounted", test.description)
			}
		} else {
			mntErr, ok := err.(MountError)
			if !ok {
				t.Errorf("mount error not of mount error type: %v", err)
			}
			if mntErr.Type != test.expErrorType {
				t.Errorf("test \"%s\" unexpected error: \n          [%v]. \nExpecting err type[%v]", test.description, err, test.expErrorType)
			}
			if len(test.sensitiveMountOptions) == 0 {
				if strings.Contains(mntErr.Error(), sensitiveOptionsRemoved) {
					t.Errorf("test \"%s\" returned an error unexpectedly containing the string %q: %v", test.description, sensitiveOptionsRemoved, err)
				}
			} else {
				if !strings.Contains(err.Error(), sensitiveOptionsRemoved) {
					t.Errorf("test \"%s\" returned an error without the string %q: %v", test.description, sensitiveOptionsRemoved, err)
				}
				for _, sensitiveOption := range test.sensitiveMountOptions {
					if strings.Contains(err.Error(), sensitiveOption) {
						t.Errorf("test \"%s\" returned an error with a sensitive string (%q): %v", test.description, sensitiveOption, err)
					}
				}
			}
		}
	}
}

// makeFakeCmd 把预期命令与参数闭包起来，忽略调用方实际传入的值以免误判。
func makeFakeCmd(fakeCmd *testingexec.FakeCmd, cmd string, args ...string) testingexec.FakeCommandAction {
	c := cmd
	a := args
	return func(cmd string, args ...string) exec.Cmd {
		command := testingexec.InitFakeCmd(fakeCmd, c, a...)
		return command
	}
}

// makeFakeOutput 固定返回预设的输出和错误，模拟命令执行结果。
func makeFakeOutput(output string, err error) testingexec.FakeAction {
	o := output
	return func() ([]byte, []byte, error) {
		return []byte(o), nil, err
	}
}
