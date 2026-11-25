//go:build linux

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
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"golang.org/x/sys/unix"
	utilexec "k8s.io/utils/exec"
	testexec "k8s.io/utils/exec/testing"
	"k8s.io/utils/ptr"
)

// 覆盖 /proc/mounts 解析的正常路径与格式错误分支。
func TestReadProcMountsFrom(t *testing.T) {
	successCase := `/dev/0 /path/to/0 type0 flags 0 0
/dev/1    /path/to/1   type1	flags 1 1
/dev/2 /path/to/2 type2 flags,1,2=3 2 2
`
	// 注意：readProcMountsFrom 已改用 fnv.New32a()。
	mounts, err := parseProcMounts([]byte(successCase))
	if err != nil {
		t.Errorf("expected success, got %v", err)
	}
	if len(mounts) != 3 {
		t.Fatalf("expected 3 mounts, got %d", len(mounts))
	}
	mp := MountPoint{"/dev/0", "/path/to/0", "type0", []string{"flags"}, 0, 0}
	if !mountPointsEqual(&mounts[0], &mp) {
		t.Errorf("got unexpected MountPoint[0]: %#v", mounts[0])
	}
	mp = MountPoint{"/dev/1", "/path/to/1", "type1", []string{"flags"}, 1, 1}
	if !mountPointsEqual(&mounts[1], &mp) {
		t.Errorf("got unexpected MountPoint[1]: %#v", mounts[1])
	}
	mp = MountPoint{"/dev/2", "/path/to/2", "type2", []string{"flags", "1", "2=3"}, 2, 2}
	if !mountPointsEqual(&mounts[2], &mp) {
		t.Errorf("got unexpected MountPoint[2]: %#v", mounts[2])
	}

	errorCases := []string{
		"/dev/0 /path/to/mount\n",
		"/dev/1 /path/to/mount type flags a 0\n",
		"/dev/2 /path/to/mount type flags 0 b\n",
	}
	for _, ec := range errorCases {
		_, err := parseProcMounts([]byte(ec))
		if err == nil {
			t.Errorf("expected error")
		}
	}
}

// 逐字段比较，避免依赖结构体可能新增的字段导致误判。
func mountPointsEqual(a, b *MountPoint) bool {
	if a.Device != b.Device || a.Path != b.Path || a.Type != b.Type || !reflect.DeepEqual(a.Opts, b.Opts) || a.Pass != b.Pass || a.Freq != b.Freq {
		return false
	}
	return true
}

// 验证按设备号找出同一块盘的所有引用点。
func TestGetMountRefs(t *testing.T) {
	fm := NewFakeMounter(
		[]MountPoint{
			{Device: "/dev/sdb", Path: "/var/lib/kubelet/plugins/kubernetes.io/gce-pd/mounts/gce-pd"},
			{Device: "/dev/sdb", Path: "/var/lib/kubelet/pods/some-pod/volumes/kubernetes.io~gce-pd/gce-pd-in-pod"},
			{Device: "/dev/sdc", Path: "/var/lib/kubelet/plugins/kubernetes.io/gce-pd/mounts/gce-pd2"},
			{Device: "/dev/sdc", Path: "/var/lib/kubelet/pods/some-pod/volumes/kubernetes.io~gce-pd/gce-pd2-in-pod1"},
			{Device: "/dev/sdc", Path: "/var/lib/kubelet/pods/some-pod/volumes/kubernetes.io~gce-pd/gce-pd2-in-pod2"},
		})

	tests := []struct {
		mountPath    string
		expectedRefs []string
	}{
		{
			"/var/lib/kubelet/pods/some-pod/volumes/kubernetes.io~gce-pd/gce-pd-in-pod",
			[]string{
				"/var/lib/kubelet/plugins/kubernetes.io/gce-pd/mounts/gce-pd",
			},
		},
		{
			"/var/lib/kubelet/pods/some-pod/volumes/kubernetes.io~gce-pd/gce-pd2-in-pod1",
			[]string{
				"/var/lib/kubelet/pods/some-pod/volumes/kubernetes.io~gce-pd/gce-pd2-in-pod2",
				"/var/lib/kubelet/plugins/kubernetes.io/gce-pd/mounts/gce-pd2",
			},
		},
		{
			"/var/fake/directory/that/doesnt/exist",
			[]string{},
		},
	}

	for i, test := range tests {
		if refs, err := fm.GetMountRefs(test.mountPath); err != nil || !setEquivalent(test.expectedRefs, refs) {
			t.Errorf("%d. getMountRefs(%q) = %v, %v; expected %v, nil", i, test.mountPath, refs, err, test.expectedRefs)
		}
	}
}

// 把切片当集合比较，因为挂载引用的返回顺序并不保证稳定。
func setEquivalent(set1, set2 []string) bool {
	map1 := make(map[string]bool)
	map2 := make(map[string]bool)
	for _, s := range set1 {
		map1[s] = true
	}
	for _, s := range set2 {
		map2[s] = true
	}

	for s := range map1 {
		if !map2[s] {
			return false
		}
	}
	for s := range map2 {
		if !map1[s] {
			return false
		}
	}
	return true
}

// 确认同一设备多处挂载时，引用计数与设备名都能正确返回。
func TestGetDeviceNameFromMount(t *testing.T) {
	fm := NewFakeMounter(
		[]MountPoint{
			{
				Device: "/dev/disk/by-path/prefix-lun-1",
				Path:   "/mnt/111",
			},
			{
				Device: "/dev/disk/by-path/prefix-lun-1",
				Path:   "/mnt/222",
			},
		})

	tests := []struct {
		mountPath      string
		expectedDevice string
		expectedRefs   int
	}{
		{
			"/mnt/222",
			"/dev/disk/by-path/prefix-lun-1",
			2,
		},
	}

	for i, test := range tests {
		if device, refs, err := GetDeviceNameFromMount(fm, test.mountPath); err != nil || test.expectedRefs != refs || test.expectedDevice != device {
			t.Errorf("%d. GetDeviceNameFromMount(%s) = (%s, %d), %v; expected (%s,%d), nil", i, test.mountPath, device, refs, err, test.expectedDevice, test.expectedRefs)
		}
	}
}

// 从具体挂载点反查同一设备上的其它挂载点。
func TestGetMountRefsByDev(t *testing.T) {
	fm := NewFakeMounter(
		[]MountPoint{
			{Device: "/dev/sdb", Path: "/var/lib/kubelet/plugins/kubernetes.io/gce-pd/mounts/gce-pd"},
			{Device: "/dev/sdb", Path: "/var/lib/kubelet/pods/some-pod/volumes/kubernetes.io~gce-pd/gce-pd-in-pod"},
			{Device: "/dev/sdc", Path: "/var/lib/kubelet/plugins/kubernetes.io/gce-pd/mounts/gce-pd2"},
			{Device: "/dev/sdc", Path: "/var/lib/kubelet/pods/some-pod/volumes/kubernetes.io~gce-pd/gce-pd2-in-pod1"},
			{Device: "/dev/sdc", Path: "/var/lib/kubelet/pods/some-pod/volumes/kubernetes.io~gce-pd/gce-pd2-in-pod2"},
		})

	tests := []struct {
		mountPath    string
		expectedRefs []string
	}{
		{
			"/var/lib/kubelet/plugins/kubernetes.io/gce-pd/mounts/gce-pd",
			[]string{
				"/var/lib/kubelet/pods/some-pod/volumes/kubernetes.io~gce-pd/gce-pd-in-pod",
			},
		},
		{
			"/var/lib/kubelet/plugins/kubernetes.io/gce-pd/mounts/gce-pd2",
			[]string{
				"/var/lib/kubelet/pods/some-pod/volumes/kubernetes.io~gce-pd/gce-pd2-in-pod1",
				"/var/lib/kubelet/pods/some-pod/volumes/kubernetes.io~gce-pd/gce-pd2-in-pod2",
			},
		},
	}

	for i, test := range tests {
		if refs, err := getMountRefsByDev(fm, test.mountPath); err != nil || !setEquivalent(test.expectedRefs, refs) {
			t.Errorf("%d. getMountRefsByDev(%q) = %v, %v; expected %v, nil", i, test.mountPath, refs, err, test.expectedRefs)
		}
	}
}

// 覆盖父子路径、前缀误判及 .. 回退等边界，防止路径逃逸被漏检。
func TestPathWithinBase(t *testing.T) {
	tests := []struct {
		name     string
		fullPath string
		basePath string
		expected bool
	}{
		{
			name:     "good subpath",
			fullPath: "/a/b/c",
			basePath: "/a",
			expected: true,
		},
		{
			name:     "good subpath 2",
			fullPath: "/a/b/c",
			basePath: "/a/b",
			expected: true,
		},
		{
			name:     "good subpath end slash",
			fullPath: "/a/b/c/",
			basePath: "/a/b",
			expected: true,
		},
		{
			name:     "good subpath backticks",
			fullPath: "/a/b/../c",
			basePath: "/a",
			expected: true,
		},
		{
			name:     "good subpath equal",
			fullPath: "/a/b/c",
			basePath: "/a/b/c",
			expected: true,
		},
		{
			name:     "good subpath equal 2",
			fullPath: "/a/b/c/",
			basePath: "/a/b/c",
			expected: true,
		},
		{
			name:     "good subpath root",
			fullPath: "/a",
			basePath: "/",
			expected: true,
		},
		{
			name:     "bad subpath parent",
			fullPath: "/a/b/c",
			basePath: "/a/b/c/d",
			expected: false,
		},
		{
			name:     "bad subpath outside",
			fullPath: "/b/c",
			basePath: "/a/b/c",
			expected: false,
		},
		{
			name:     "bad subpath prefix",
			fullPath: "/a/b/cd",
			basePath: "/a/b/c",
			expected: false,
		},
		{
			name:     "bad subpath backticks",
			fullPath: "/a/../b",
			basePath: "/a",
			expected: false,
		},
		{
			name:     "configmap subpath",
			fullPath: "/var/lib/kubelet/pods/uuid/volumes/kubernetes.io~configmap/config/..timestamp/file.txt",
			basePath: "/var/lib/kubelet/pods/uuid/volumes/kubernetes.io~configmap/config",
			expected: true,
		},
	}
	for _, test := range tests {
		if PathWithinBase(test.fullPath, test.basePath) != test.expected {
			t.Errorf("test %q failed: expected %v", test.name, test.expected)
		}
	}
}

// 用固定的 findmnt 输出验证各类共享挂载场景下的引用搜索。
func TestSearchMountPoints(t *testing.T) {
	base := `
19 25 0:18 / /sys rw,nosuid,nodev,noexec,relatime shared:7 - sysfs sysfs rw
20 25 0:4 / /proc rw,nosuid,nodev,noexec,relatime shared:12 - proc proc rw
21 25 0:6 / /dev rw,nosuid,relatime shared:2 - devtmpfs udev rw,size=4058156k,nr_inodes=1014539,mode=755
22 21 0:14 / /dev/pts rw,nosuid,noexec,relatime shared:3 - devpts devpts rw,gid=5,mode=620,ptmxmode=000
23 25 0:19 / /run rw,nosuid,noexec,relatime shared:5 - tmpfs tmpfs rw,size=815692k,mode=755
25 0 252:0 / / rw,relatime shared:1 - ext4 /dev/mapper/ubuntu--vg-root rw,errors=remount-ro,data=ordered
26 19 0:12 / /sys/kernel/security rw,nosuid,nodev,noexec,relatime shared:8 - securityfs securityfs rw
27 21 0:21 / /dev/shm rw,nosuid,nodev shared:4 - tmpfs tmpfs rw
28 23 0:22 / /run/lock rw,nosuid,nodev,noexec,relatime shared:6 - tmpfs tmpfs rw,size=5120k
29 19 0:23 / /sys/fs/cgroup ro,nosuid,nodev,noexec shared:9 - tmpfs tmpfs ro,mode=755
30 29 0:24 / /sys/fs/cgroup/systemd rw,nosuid,nodev,noexec,relatime shared:10 - cgroup cgroup rw,xattr,release_agent=/lib/systemd/systemd-cgroups-agent,name=systemd
31 19 0:25 / /sys/fs/pstore rw,nosuid,nodev,noexec,relatime shared:11 - pstore pstore rw
32 29 0:26 / /sys/fs/cgroup/devices rw,nosuid,nodev,noexec,relatime shared:13 - cgroup cgroup rw,devices
33 29 0:27 / /sys/fs/cgroup/freezer rw,nosuid,nodev,noexec,relatime shared:14 - cgroup cgroup rw,freezer
34 29 0:28 / /sys/fs/cgroup/pids rw,nosuid,nodev,noexec,relatime shared:15 - cgroup cgroup rw,pids
35 29 0:29 / /sys/fs/cgroup/blkio rw,nosuid,nodev,noexec,relatime shared:16 - cgroup cgroup rw,blkio
36 29 0:30 / /sys/fs/cgroup/memory rw,nosuid,nodev,noexec,relatime shared:17 - cgroup cgroup rw,memory
37 29 0:31 / /sys/fs/cgroup/perf_event rw,nosuid,nodev,noexec,relatime shared:18 - cgroup cgroup rw,perf_event
38 29 0:32 / /sys/fs/cgroup/hugetlb rw,nosuid,nodev,noexec,relatime shared:19 - cgroup cgroup rw,hugetlb
39 29 0:33 / /sys/fs/cgroup/cpu,cpuacct rw,nosuid,nodev,noexec,relatime shared:20 - cgroup cgroup rw,cpu,cpuacct
40 29 0:34 / /sys/fs/cgroup/cpuset rw,nosuid,nodev,noexec,relatime shared:21 - cgroup cgroup rw,cpuset
41 29 0:35 / /sys/fs/cgroup/net_cls,net_prio rw,nosuid,nodev,noexec,relatime shared:22 - cgroup cgroup rw,net_cls,net_prio
58 25 7:1 / /mnt/disks/blkvol1 rw,relatime shared:38 - ext4 /dev/loop1 rw,data=ordere
`

	testcases := []struct {
		name         string
		source       string
		mountInfos   string
		expectedRefs []string
		expectedErr  error
	}{
		{
			"dir",
			"/mnt/disks/vol1",
			base,
			nil,
			nil,
		},
		{
			"dir-used",
			"/mnt/disks/vol1",
			base + `
56 25 252:0 /mnt/disks/vol1 /var/lib/kubelet/pods/1890aef5-5a60-11e8-962f-000c29bb0377/volumes/kubernetes.io~local-volume/local-pv-test rw,relatime shared:1 - ext4 /dev/mapper/ubuntu--vg-root rw,errors=remount-ro,data=ordered
57 25 0:45 / /mnt/disks/vol rw,relatime shared:36 - tmpfs tmpfs rw
`,
			[]string{"/var/lib/kubelet/pods/1890aef5-5a60-11e8-962f-000c29bb0377/volumes/kubernetes.io~local-volume/local-pv-test"},
			nil,
		},
		{
			"tmpfs-vol",
			"/mnt/disks/vol1",
			base + `120 25 0:76 / /mnt/disks/vol1 rw,relatime shared:41 - tmpfs vol1 rw,size=10000k
`,
			nil,
			nil,
		},
		{
			"tmpfs-vol-used-by-two-pods",
			"/mnt/disks/vol1",
			base + `120 25 0:76 / /mnt/disks/vol1 rw,relatime shared:41 - tmpfs vol1 rw,size=10000k
196 25 0:76 / /var/lib/kubelet/pods/ade3ac21-5a5b-11e8-8559-000c29bb0377/volumes/kubernetes.io~local-volume/local-pv-8f263585 rw,relatime shared:41 - tmpfs vol1 rw,size=10000k
228 25 0:76 / /var/lib/kubelet/pods/ac60532d-5a5b-11e8-8559-000c29bb0377/volumes/kubernetes.io~local-volume/local-pv-8f263585 rw,relatime shared:41 - tmpfs vol1 rw,size=10000k
`,
			[]string{
				"/var/lib/kubelet/pods/ade3ac21-5a5b-11e8-8559-000c29bb0377/volumes/kubernetes.io~local-volume/local-pv-8f263585",
				"/var/lib/kubelet/pods/ac60532d-5a5b-11e8-8559-000c29bb0377/volumes/kubernetes.io~local-volume/local-pv-8f263585",
			},
			nil,
		},
		{
			"tmpfs-subdir-used-indirectly-via-bindmount-dir-by-one-pod",
			"/mnt/vol1/foo",
			base + `177 25 0:46 / /mnt/data rw,relatime shared:37 - tmpfs data rw
190 25 0:46 /vol1 /mnt/vol1 rw,relatime shared:37 - tmpfs data rw
191 25 0:46 /vol2 /mnt/vol2 rw,relatime shared:37 - tmpfs data rw
62 25 0:46 /vol1/foo /var/lib/kubelet/pods/e25f2f01-5b06-11e8-8694-000c29bb0377/volumes/kubernetes.io~local-volume/local-pv-test rw,relatime shared:37 - tmpfs data rw
`,
			[]string{"/var/lib/kubelet/pods/e25f2f01-5b06-11e8-8694-000c29bb0377/volumes/kubernetes.io~local-volume/local-pv-test"},
			nil,
		},
		{
			"dir-bindmounted",
			"/mnt/disks/vol2",
			base + `342 25 252:0 /mnt/disks/vol2 /mnt/disks/vol2 rw,relatime shared:1 - ext4 /dev/mapper/ubuntu--vg-root rw,errors=remount-ro,data=ordered
`,
			nil,
			nil,
		},
		{
			"dir-bindmounted-used-by-one-pod",
			"/mnt/disks/vol2",
			base + `342 25 252:0 /mnt/disks/vol2 /mnt/disks/vol2 rw,relatime shared:1 - ext4 /dev/mapper/ubuntu--vg-root rw,errors=remount-ro,data=ordered
77 25 252:0 /mnt/disks/vol2 /var/lib/kubelet/pods/f30dc360-5a5d-11e8-962f-000c29bb0377/volumes/kubernetes.io~local-volume/local-pv-1fb30a1c rw,relatime shared:1 - ext4 /dev/mapper/ubuntu--vg-root rw,errors=remount-ro,data=ordered
`,
			[]string{"/var/lib/kubelet/pods/f30dc360-5a5d-11e8-962f-000c29bb0377/volumes/kubernetes.io~local-volume/local-pv-1fb30a1c"},
			nil,
		},
		{
			"blockfs",
			"/mnt/disks/blkvol1",
			base + `58 25 7:1 / /mnt/disks/blkvol1 rw,relatime shared:38 - ext4 /dev/loop1 rw,data=ordered
`,
			nil,
			nil,
		},
		{
			"blockfs-used-by-one-pod",
			"/mnt/disks/blkvol1",
			base + `58 25 7:1 / /mnt/disks/blkvol1 rw,relatime shared:38 - ext4 /dev/loop1 rw,data=ordered
62 25 7:1 / /var/lib/kubelet/pods/f19fe4e2-5a63-11e8-962f-000c29bb0377/volumes/kubernetes.io~local-volume/local-pv-test rw,relatime shared:38 - ext4 /dev/loop1 rw,data=ordered
`,
			[]string{"/var/lib/kubelet/pods/f19fe4e2-5a63-11e8-962f-000c29bb0377/volumes/kubernetes.io~local-volume/local-pv-test"},
			nil,
		},
		{
			"blockfs-used-by-two-pods",
			"/mnt/disks/blkvol1",
			base + `58 25 7:1 / /mnt/disks/blkvol1 rw,relatime shared:38 - ext4 /dev/loop1 rw,data=ordered
62 25 7:1 / /var/lib/kubelet/pods/f19fe4e2-5a63-11e8-962f-000c29bb0377/volumes/kubernetes.io~local-volume/local-pv-test rw,relatime shared:38 - ext4 /dev/loop1 rw,data=ordered
95 25 7:1 / /var/lib/kubelet/pods/4854a48b-5a64-11e8-962f-000c29bb0377/volumes/kubernetes.io~local-volume/local-pv-test rw,relatime shared:38 - ext4 /dev/loop1 rw,data=ordered
`,
			[]string{
				"/var/lib/kubelet/pods/f19fe4e2-5a63-11e8-962f-000c29bb0377/volumes/kubernetes.io~local-volume/local-pv-test",
				"/var/lib/kubelet/pods/4854a48b-5a64-11e8-962f-000c29bb0377/volumes/kubernetes.io~local-volume/local-pv-test",
			},
			nil,
		},
	}
	tmpFile, err := os.CreateTemp("", "test-get-filetype")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()
	for _, v := range testcases {
		assert.NoError(t, tmpFile.Truncate(0))
		_, err := tmpFile.Seek(0, 0)
		assert.NoError(t, err)
		_, err = tmpFile.WriteString(v.mountInfos)
		assert.NoError(t, err)
		assert.NoError(t, tmpFile.Sync())
		refs, err := SearchMountPoints(v.source, tmpFile.Name())
		if !reflect.DeepEqual(refs, v.expectedRefs) {
			t.Errorf("test %q: expected Refs: %#v, got %#v", v.name, v.expectedRefs, refs)
		}
		if err != v.expectedErr {
			t.Errorf("test %q: expected err: %v, got %v", v.name, v.expectedErr, err)
		}
	}
}

// 验证挂载参数中敏感选项被遮蔽，同时辅助标志与普通选项完整保留。
func TestSensitiveMountOptions(t *testing.T) {
	// 准备测试数据。
	testcases := []struct {
		source           string
		target           string
		fstype           string
		options          []string
		sensitiveOptions []string
		mountFlags       []string
		expectedLogStr   string
	}{
		{
			source:           "mySrc",
			target:           "myTarget",
			fstype:           "myFS",
			options:          []string{"o1", "o2"},
			sensitiveOptions: []string{"s1", "s2"},
			mountFlags:       []string{},
			expectedLogStr:   " -t myFS -o o1,o2,<masked>,<masked> mySrc myTarget",
		},
		{
			source:           "mySrc",
			target:           "myTarget",
			fstype:           "myFS",
			options:          []string{},
			sensitiveOptions: []string{"s1", "s2"},
			mountFlags:       []string{},
			expectedLogStr:   " -t myFS -o <masked>,<masked> mySrc myTarget",
		},
		{
			source:           "mySrc",
			target:           "myTarget",
			fstype:           "myFS",
			options:          []string{"o1", "o2"},
			sensitiveOptions: []string{},
			mountFlags:       []string{},
			expectedLogStr:   " -t myFS -o o1,o2 mySrc myTarget",
		},
		{
			source:           "mySrc",
			target:           "myTarget",
			fstype:           "myFS",
			options:          []string{"o1", "o2"},
			sensitiveOptions: []string{"s1", "s2"},
			mountFlags:       []string{"--no-canonicalize"},
			expectedLogStr:   "--no-canonicalize -t myFS -o o1,o2,<masked>,<masked> mySrc myTarget",
		},
	}

	for _, v := range testcases {
		// 执行被测逻辑。
		mountArgs, mountArgsLogStr := MakeMountArgsSensitiveWithMountFlags(v.source, v.target, v.fstype, v.options, v.sensitiveOptions, v.mountFlags)

		// 校验结果。
		t.Logf("\r\nmountArgs =%q\r\nmountArgsLogStr=%q", mountArgs, mountArgsLogStr)
		for _, mountFlag := range v.mountFlags {
			if found := mountArgsContainString(t, mountArgs, mountFlag); !found {
				t.Errorf("Expected mountFlag (%q) to exist in returned mountArgs (%q), but it does not", mountFlag, mountArgs)
			}
			if !strings.Contains(mountArgsLogStr, mountFlag) {
				t.Errorf("Expected mountFlag (%q) to exist in returned mountArgsLogStr (%q), but it does", mountFlag, mountArgsLogStr)
			}
		}
		for _, option := range v.options {
			if found := mountArgsContainOption(t, mountArgs, option); !found {
				t.Errorf("Expected option (%q) to exist in returned mountArgs (%q), but it does not", option, mountArgs)
			}
			if !strings.Contains(mountArgsLogStr, option) {
				t.Errorf("Expected option (%q) to exist in returned mountArgsLogStr (%q), but it does", option, mountArgsLogStr)
			}
		}
		for _, sensitiveOption := range v.sensitiveOptions {
			if found := mountArgsContainOption(t, mountArgs, sensitiveOption); !found {
				t.Errorf("Expected sensitiveOption (%q) to exist in returned mountArgs (%q), but it does not", sensitiveOption, mountArgs)
			}
			if strings.Contains(mountArgsLogStr, sensitiveOption) {
				t.Errorf("Expected sensitiveOption (%q) to not exist in returned mountArgsLogStr (%q), but it does", sensitiveOption, mountArgsLogStr)
			}
		}
		if v.expectedLogStr != "" && mountArgsLogStr != v.expectedLogStr {
			t.Errorf("Expected mountArgsLogStr to be %q, got %q", v.expectedLogStr, mountArgsLogStr)
		}
	}
}

// 确认 systemd 探测在初始化后留下可用的判定结果。
func TestHasSystemd(t *testing.T) {
	mounter := &Mounter{}
	_ = mounter.hasSystemd()
	if mounter.withSystemd == nil {
		t.Error("Failed to run detectSystemd()")
	}
}

// 精确匹配单个参数项，避免子串误判。
func mountArgsContainString(t *testing.T, mountArgs []string, wanted string) bool {
	for _, mountArg := range mountArgs {
		if mountArg == wanted {
			return true
		}
	}
	return false
}

// 只在 -o 之后的参数中查找选项，防止命中源路径等同名字符串。
func mountArgsContainOption(t *testing.T, mountArgs []string, option string) bool {
	optionsIndex := -1
	for i, s := range mountArgs {
		if s == "-o" {
			optionsIndex = i + 1
			break
		}
	}

	if optionsIndex < 0 || optionsIndex >= len(mountArgs) {
		return false
	}

	return strings.Contains(mountArgs[optionsIndex], option)
}

// 用伪造的 umount 输出验证“未挂载”安全行为的判定，避免依赖宿主机实际 umount 版本。
func TestDetectSafeNotMountedBehavior(t *testing.T) {
	// util-linux 2.30.2 中 umount 的输出示例。
	notMountedOutput := "umount: /foo: not mounted."

	testcases := []struct {
		fakeCommandAction testexec.FakeCommandAction
		expectedSafe      bool
	}{
		{
			fakeCommandAction: makeFakeCommandAction(notMountedOutput, errors.New("any error"), nil),
			expectedSafe:      true,
		},
		{
			fakeCommandAction: makeFakeCommandAction(notMountedOutput, nil, nil),
			expectedSafe:      false,
		},
		{
			fakeCommandAction: makeFakeCommandAction("any output", nil, nil),
			expectedSafe:      false,
		},
		{
			fakeCommandAction: makeFakeCommandAction("any output", errors.New("any error"), nil),
			expectedSafe:      false,
		},
	}

	for _, v := range testcases {
		fakeexec := &testexec.FakeExec{
			LookPathFunc: func(s string) (string, error) {
				return "fake-umount", nil
			},
			CommandScript: []testexec.FakeCommandAction{v.fakeCommandAction},
		}

		if detectSafeNotMountedBehaviorWithExec(fakeexec) != v.expectedSafe {
			var adj string
			if v.expectedSafe {
				adj = "safe"
			} else {
				adj = "unsafe"
			}
			t.Errorf("Expected to detect %s umount behavior, but did not", adj)
		}
	}
}

// 覆盖卸载时的几类典型错误，确认只有预期情形被当作可忽略。
func TestCheckUmountError(t *testing.T) {
	target := "/test/path"
	withSafeNotMountedBehavior := true
	command := exec.Command("uname", "-r") // 仅用于取得状态为 0 的虚拟命令。

	if err := command.Run(); err != nil {
		t.Errorf("Faild to exec dummy command. err: %s", err)
	}

	testcases := []struct {
		output   []byte
		err      error
		expected bool
	}{
		{
			err:      errors.New("wait: no child processes"),
			expected: true,
		},
		{
			output:   []byte("umount: /test/path: not mounted."),
			err:      errors.New("exit status 1"),
			expected: true,
		},
		{
			output:   []byte("umount: /test/path: No such file or directory"),
			err:      errors.New("exit status 1"),
			expected: false,
		},
	}

	for _, v := range testcases {
		if err := checkUmountError(target, command, v.output, v.err, withSafeNotMountedBehavior); (err == nil) != v.expected {
			if v.expected {
				t.Errorf("Expected to return nil, but did not. err: %s", err)
			} else {
				t.Errorf("Expected to return error, but did not.")
			}
		}
	}
}

// TODO：测试设计讨论见 https://github.com/kubernetes/kubernetes/pull/117539#discussion_r1181873355
// 用受控的假命令验证格式化并发上限：多余请求应排队等待而非同时执行。
func TestFormatConcurrency(t *testing.T) {
	const (
		formatCount    = 5
		fstype         = "ext4"
		output         = "complete"
		defaultTimeout = 1 * time.Minute
	)

	tests := []struct {
		desc    string
		max     int
		timeout time.Duration
	}{
		{
			max: 2,
		},
		{
			max: 3,
		},
		{
			max: 4,
		},
	}

	for _, tc := range tests {
		t.Run(fmt.Sprintf("max=%d,timeout=%s", tc.max, tc.timeout.String()), func(t *testing.T) {
			if tc.timeout == 0 {
				tc.timeout = defaultTimeout
			}

			var concurrent int
			var mu sync.Mutex
			witness := make(chan struct{})

			exec := &testexec.FakeExec{}
			for i := 0; i < formatCount; i++ {
				exec.CommandScript = append(exec.CommandScript, makeFakeCommandAction(output, nil, func() {
					mu.Lock()
					concurrent++
					mu.Unlock()

					<-witness

					mu.Lock()
					concurrent--
					mu.Unlock()
				}))
			}
			mounter := NewSafeFormatAndMount(nil, exec, WithMaxConcurrentFormat(tc.max, tc.timeout))

			// 运行 max+1 个 goroutine 并阻塞命令执行；只有 max 个应同时运行，多余的一个必须等待某个令牌释放。
			for i := 0; i < tc.max+1; i++ {
				go func() {
					_, err := mounter.format(fstype, nil)
					if err != nil {
						t.Errorf("format(%q): %v", fstype, err)
					}
				}()
			}

			// 等待所有 goroutine 都被调度。
			time.Sleep(100 * time.Millisecond)

			mu.Lock()
			if concurrent != tc.max {
				t.Errorf("SafeFormatAndMount.format() got concurrency: %d, want: %d", concurrent, tc.max)
			}
			mu.Unlock()

			// 通知正在执行的命令结束，让排队的命令得以执行。
			for i := 0; i < tc.max; i++ {
				witness <- struct{}{}
			}

			// 等待所有 goroutine 获取锁并递减计数器。
			time.Sleep(100 * time.Millisecond)

			mu.Lock()
			if concurrent != 1 {
				t.Errorf("SafeFormatAndMount.format() got concurrency: %d, want: 1", concurrent)
			}
			mu.Unlock()

			// 通知剩余命令结束，此后不应再有命令运行。
			close(witness)

			// 稍等片刻，让最后一个 goroutine 获取锁并将计数器递减到零。
			time.Sleep(10 * time.Millisecond)

			mu.Lock()
			if concurrent != 0 {
				t.Errorf("SafeFormatAndMount.format() got concurrency: %d, want: 0", concurrent)
			}
			mu.Unlock()
		})
	}
}

// TODO：测试设计讨论见 https://github.com/kubernetes/kubernetes/pull/117539#discussion_r1181873355
// 验证等待超时的排队请求最终会被放行，避免格式化请求永久挂起。
func TestFormatTimeout(t *testing.T) {
	const (
		formatCount    = 5
		fstype         = "ext4"
		output         = "complete"
		maxConcurrency = 4
		timeout        = 200 * time.Millisecond
	)

	var concurrent int
	var mu sync.Mutex
	witness := make(chan struct{})

	exec := &testexec.FakeExec{}
	for i := 0; i < formatCount; i++ {
		exec.CommandScript = append(exec.CommandScript, makeFakeCommandAction(output, nil, func() {
			mu.Lock()
			concurrent++
			mu.Unlock()

			<-witness

			mu.Lock()
			concurrent--
			mu.Unlock()
		}))
	}
	mounter := NewSafeFormatAndMount(nil, exec, WithMaxConcurrentFormat(maxConcurrency, timeout))

	for i := 0; i < maxConcurrency+1; i++ {
		go func() {
			_, err := mounter.format(fstype, nil)
			if err != nil {
				t.Errorf("format(%q): %v", fstype, err)
			}
		}()
	}

	// 等待时间略长于配置的超时。
	time.Sleep(timeout + 100*time.Millisecond)

	mu.Lock()
	if concurrent != maxConcurrency+1 {
		t.Errorf("SafeFormatAndMount.format() got concurrency: %d, want: %d", concurrent, maxConcurrency+1)
	}
	mu.Unlock()

	// 通知剩余命令结束，并等待所有 goroutine 获取锁且递减计数器。
	close(witness)
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	if concurrent != 0 {
		t.Errorf("SafeFormatAndMount.format() got concurrency: %d, want: 0", concurrent)
	}
	mu.Unlock()
}

// Signed 复制 "golang.org/x/exp/constraints" 中的部分类型，避免依赖该包。
// 只在测试里用到的有符号整数约束，避免为一个简单辅助函数引入额外依赖。
type Signed interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64
}

// Unsigned 统一覆盖无符号整数，兼容各平台不同的 Statfs_t.Flags 底层类型。
type Unsigned interface {
	~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~uintptr
}

// 合并有符号与无符号约束，供进制转换辅助函数复用。
type Integer interface {
	Signed | Unsigned
}

// 部分平台以不同方式定义 unix.Statfs_t.Flags；这里只测试少量标志，因此可以进行激进的类型转换。
// 泛型转换让同一份测试代码能在不同体系结构的 Flags 类型上编译。
func mkStatfsFlags[T1 Integer, T2 Integer](orig T1, add T2) T1 {
	return orig | T1(add)
}

// 验证从 Statfs 标志位推导出的绑定挂载选项，注意结果需排序后比较。
func TestGetBindMountOptions(t *testing.T) {
	var testCases = map[string]struct {
		flags        int32 // 所关注平台中最小的底层类型。
		mountoptions string
	}{
		"ro":            {flags: unix.MS_RDONLY, mountoptions: "ro"},
		"nodev":         {flags: unix.MS_NODEV, mountoptions: "nodev"},
		"noexec":        {flags: unix.MS_NOEXEC, mountoptions: "noexec"},
		"nosuid":        {flags: unix.MS_NOSUID, mountoptions: "nosuid"},
		"noatime":       {flags: unix.MS_NOATIME, mountoptions: "noatime"},
		"relatime":      {flags: unix.MS_RELATIME, mountoptions: "relatime"},
		"nodiratime":    {flags: unix.MS_NODIRATIME, mountoptions: "nodiratime"},
		"ronodev":       {flags: unix.MS_RDONLY | unix.MS_NODEV, mountoptions: "nodev,ro"},
		"ronodevnoexec": {flags: unix.MS_RDONLY | unix.MS_NODEV | unix.MS_NOEXEC, mountoptions: "nodev,noexec,ro"},
	}

	statfsMock := func(path string, buf *unix.Statfs_t) (err error) {
		*buf = unix.Statfs_t{}
		buf.Flags = mkStatfsFlags(buf.Flags, testCases[path].flags)
		return nil
	}

	testGetBindMountOptionsSingleCase := func(t *testing.T) {
		path := strings.Split(t.Name(), "/")[1]
		options, _ := getBindMountOptions(path, statfsMock)
		sort.Strings(options)
		optionString := strings.Join(options, ",")
		mountOptions := testCases[path].mountoptions
		if optionString != mountOptions {
			t.Fatalf(`Mountoptions differ. Wanted: %s, returned: %s`, mountOptions, optionString)
		}
	}

	for k := range testCases {
		t.Run(k, testGetBindMountOptionsSingleCase)
	}
}

// 构造可注入输出、错误与副作用的假命令动作，供并发与错误路径测试复用。
func makeFakeCommandAction(stdout string, err error, cmdFn func()) testexec.FakeCommandAction {
	c := testexec.FakeCmd{
		CombinedOutputScript: []testexec.FakeAction{
			func() ([]byte, []byte, error) {
				if cmdFn != nil {
					cmdFn()
				}
				return []byte(stdout), nil, err
			},
		},
	}
	return func(cmd string, args ...string) utilexec.Cmd {
		return testexec.InitFakeCmd(&c, cmd, args...)
	}
}

// 覆盖普通目录、符号链接及悬空链接等情形，同时用绝对路径与相对路径各跑一遍。
func TestIsLikelyNotMountPoint(t *testing.T) {
	mounter := Mounter{"fake/path", ptr.To(true), true, true}

	tests := []struct {
		fileName       string
		targetLinkName string
		setUp          func(base, fileName, targetLinkName string) error
		cleanUp        func(base, fileName, targetLinkName string) error
		expectedResult bool
		expectError    bool
	}{
		{
			"Dir",
			"",
			func(base, fileName, targetLinkName string) error {
				return os.Mkdir(filepath.Join(base, fileName), 0o750)
			},
			func(base, fileName, targetLinkName string) error {
				return os.Remove(filepath.Join(base, fileName))
			},
			true,
			false,
		},
		{
			"InvalidDir",
			"",
			func(base, fileName, targetLinkName string) error {
				return nil
			},
			func(base, fileName, targetLinkName string) error {
				return nil
			},
			true,
			true,
		},
		{
			"ValidSymLink",
			"targetSymLink",
			func(base, fileName, targetLinkName string) error {
				targeLinkPath := filepath.Join(base, targetLinkName)
				if err := os.Mkdir(targeLinkPath, 0o750); err != nil {
					return err
				}

				filePath := filepath.Join(base, fileName)
				if err := os.Symlink(targeLinkPath, filePath); err != nil {
					return err
				}
				return nil
			},
			func(base, fileName, targetLinkName string) error {
				if err := os.Remove(filepath.Join(base, fileName)); err != nil {
					return err
				}
				return os.Remove(filepath.Join(base, targetLinkName))
			},
			true,
			false,
		},
		{
			"InvalidSymLink",
			"targetSymLink2",
			func(base, fileName, targetLinkName string) error {
				targeLinkPath := filepath.Join(base, targetLinkName)
				if err := os.Mkdir(targeLinkPath, 0o750); err != nil {
					return err
				}

				filePath := filepath.Join(base, fileName)
				if err := os.Symlink(targeLinkPath, filePath); err != nil {
					return err
				}
				return os.Remove(targeLinkPath)
			},
			func(base, fileName, targetLinkName string) error {
				return os.Remove(filepath.Join(base, fileName))
			},
			true,
			true,
		},
	}

	for _, test := range tests {
		// 分别使用绝对路径和相对路径测试。
		baseList := []string{t.TempDir(), "./"}
		for _, base := range baseList {
			if err := test.setUp(base, test.fileName, test.targetLinkName); err != nil {
				t.Fatalf("unexpected error in setUp(%s, %s): %v", test.fileName, test.targetLinkName, err)
			}

			filePath := filepath.Join(base, test.fileName)
			result, err := mounter.IsLikelyNotMountPoint(filePath)
			if result != test.expectedResult {
				t.Errorf("Expect result not equal with IsLikelyNotMountPoint(%s) return: %t, expected: %t", filePath, result, test.expectedResult)
			}

			if base == "./" {
				if err := test.cleanUp(base, test.fileName, test.targetLinkName); err != nil {
					t.Fatalf("unexpected error in cleanUp(%s, %s): %v", test.fileName, test.targetLinkName, err)
				}
			}

			if (err != nil) != test.expectError {
				if test.expectError {
					t.Errorf("Expect error during IsLikelyNotMountPoint(%s)", filePath)
				} else {
					t.Errorf("Expect error is nil during IsLikelyNotMountPoint(%s): %v", filePath, err)
				}
			}
		}
	}
}
