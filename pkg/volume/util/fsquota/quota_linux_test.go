//go:build linux

/*
Copyright 2018 The Kubernetes Authors.

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

package fsquota

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"k8s.io/mount-utils"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/volume/util/fsquota/common"
)

// dummyMountData 是模拟的 /proc/self/mounts 内容，供挂载解析测试使用。
const dummyMountData = `sysfs /sys sysfs rw,nosuid,nodev,noexec,relatime 0 0
proc /proc proc rw,nosuid,nodev,noexec,relatime 0 0
devtmpfs /dev devtmpfs rw,nosuid,size=6133536k,nr_inodes=1533384,mode=755 0 0
tmpfs /tmp tmpfs rw,nosuid,nodev 0 0
/dev/sda1 /boot ext4 rw,relatime 0 0
/dev/mapper/fedora-root / ext4 rw,noatime 0 0
/dev/mapper/fedora-home /home ext4 rw,noatime 0 0
/dev/sdb1 /virt xfs rw,noatime,attr2,inode64,usrquota,prjquota 0 0
`

// dummyFakeMount1 返回带若干典型挂载点的假挂载器，用于挂载点探测测试。
func dummyFakeMount1() mount.Interface {
	return mount.NewFakeMounter(
		[]mount.MountPoint{
			{
				Device: "tmpfs",
				Path:   "/tmp",
				Type:   "tmpfs",
				Opts:   []string{"rw", "nosuid", "nodev"},
			},
			{
				Device: "/dev/sda1",
				Path:   "/boot",
				Type:   "ext4",
				Opts:   []string{"rw", "relatime"},
			},
			{
				Device: "/dev/mapper/fedora-root",
				Path:   "/",
				Type:   "ext4",
				Opts:   []string{"rw", "relatime"},
			},
			{
				Device: "/dev/mapper/fedora-home",
				Path:   "/home",
				Type:   "ext4",
				Opts:   []string{"rw", "relatime"},
			},
			{
				Device: "/dev/sdb1",
				Path:   "/mnt/virt",
				Type:   "xfs",
				Opts:   []string{"rw", "relatime", "attr2", "inode64", "usrquota", "prjquota"},
			},
		})
}

// backingDevTest 描述一次块设备探测的用例：给定路径和 mounts 内容，期望的结果或失败。
type backingDevTest struct {
	path           string
	mountdata      string
	expectedResult string
	expectFailure  bool
}

// mountpointTest 描述一次挂载点探测的用例：给定路径和挂载器，期望的结果或失败。
type mountpointTest struct {
	path           string
	mounter        mount.Interface
	expectedResult string
	expectFailure  bool
}

// testBackingDev1 执行一次块设备探测用例，将 mounts 内容写入临时文件再解析。
func testBackingDev1(testcase backingDevTest) error {
	tmpfile, err := os.CreateTemp("", "backingdev")
	if err != nil {
		return err
	}
	defer os.Remove(tmpfile.Name())
	if _, err = tmpfile.WriteString(testcase.mountdata); err != nil {
		return err
	}

	backingDev, err := detectBackingDevInternal(testcase.path, tmpfile.Name())
	if err != nil {
		if testcase.expectFailure {
			return nil
		}
		return err
	}
	if testcase.expectFailure {
		return fmt.Errorf("path %s expected to fail; succeeded and got %s", testcase.path, backingDev)
	}
	if backingDev == testcase.expectedResult {
		return nil
	}
	return fmt.Errorf("mismatch: path %s expects mountpoint %s got %s", testcase.path, testcase.expectedResult, backingDev)
}

// TestBackingDev 验证从 mounts 数据中为各种路径找到正确块设备，错误路径应失败。
func TestBackingDev(t *testing.T) {
	testcasesBackingDev := map[string]backingDevTest{
		"Root": {
			"/",
			dummyMountData,
			"/dev/mapper/fedora-root",
			false,
		},
		"tmpfs": {
			"/tmp",
			dummyMountData,
			"tmpfs",
			false,
		},
		"user filesystem": {
			"/virt",
			dummyMountData,
			"/dev/sdb1",
			false,
		},
		"empty mountpoint": {
			"",
			dummyMountData,
			"",
			true,
		},
		"bad mountpoint": {
			"/kiusf",
			dummyMountData,
			"",
			true,
		},
	}
	for name, testcase := range testcasesBackingDev {
		err := testBackingDev1(testcase)
		if err != nil {
			t.Errorf("%s failed: %s", name, err.Error())
		}
	}
}

// TestDetectMountPoint 验证沿路径向上查找挂载点的行为，包括根目录与空路径。
func TestDetectMountPoint(t *testing.T) {
	testcasesMount := map[string]mountpointTest{
		"Root": {
			"/",
			dummyFakeMount1(),
			"/",
			false,
		},
		"(empty)": {
			"",
			dummyFakeMount1(),
			"/",
			false,
		},
		"(invalid)": {
			"",
			dummyFakeMount1(),
			"/",
			false,
		},
		"/usr": {
			"/usr",
			dummyFakeMount1(),
			"/",
			false,
		},
		"/var/tmp": {
			"/var/tmp",
			dummyFakeMount1(),
			"/",
			false,
		},
	}
	for name, testcase := range testcasesMount {
		mountpoint, err := detectMountpointInternal(testcase.mounter, testcase.path)
		if err == nil && testcase.expectFailure {
			t.Errorf("Case %s expected failure, but succeeded, returning mountpoint %s", name, mountpoint)
		} else if err != nil {
			t.Errorf("Case %s failed: %s", name, err.Error())
		} else if mountpoint != testcase.expectedResult {
			t.Errorf("Case %s got mountpoint %s, expected %s", name, mountpoint, testcase.expectedResult)
		}
	}
}

// dummyMountPoints 模拟带 prjquota 与不带配额选项的挂载，供配额测试使用。
var dummyMountPoints = []mount.MountPoint{
	{
		Device: "/dev/sda2",
		Path:   "/quota1",
		Type:   "ext4",
		Opts:   []string{"rw", "relatime", "prjquota"},
	},
	{
		Device: "/dev/sda3",
		Path:   "/quota2",
		Type:   "ext4",
		Opts:   []string{"rw", "relatime", "prjquota"},
	},
	{
		Device: "/dev/sda3",
		Path:   "/noquota",
		Type:   "ext4",
		Opts:   []string{"rw", "relatime"},
	},
	{
		Device: "/dev/sda1",
		Path:   "/",
		Type:   "ext4",
		Opts:   []string{"rw", "relatime"},
	},
}

// dummyQuotaTest 返回基于 dummyMountPoints 的假挂载器。
func dummyQuotaTest() mount.Interface {
	return mount.NewFakeMounter(dummyMountPoints)
}

// dummySetFSInfo 预填路径的挂载点与块设备缓存，绕过真实文件系统探测。
func dummySetFSInfo(path string) {
	if enabledQuotasForMonitoring() {
		for _, mount := range dummyMountPoints {
			// 取最长前缀对应挂载；dummyMountPoints 中更具体的路径在前。
			if strings.HasPrefix(path, mount.Path) {
				mountpointMap[path] = mount.Path
				backingDevMap[path] = mount.Device
				return
			}
		}
	}
}

// VolumeProvider1 是为测试提供 /quota1 挂载点施加器的虚拟配额提供者。
type VolumeProvider1 struct {
}

// VolumeProvider2 是为测试提供 /quota2 挂载点施加器的虚拟配额提供者。
type VolumeProvider2 struct {
}

// testVolumeQuota 是用内存映射模拟配额施加行为的虚拟施加器。
type testVolumeQuota struct {
}

// logAllMaps 在用例失败时打印所有内部映射，便于定位状态偏差。
func logAllMaps(where string) {
	fmt.Printf("Maps at %s\n", where)
	fmt.Printf("    Map podQuotaMap contents:\n")
	for key, val := range podQuotaMap {
		fmt.Printf("        %v -> %v\n", key, val)
	}
	fmt.Printf("    Map dirQuotaMap contents:\n")
	for key, val := range dirQuotaMap {
		fmt.Printf("        %v -> %v\n", key, val)
	}
	fmt.Printf("    Map quotaPodMap contents:\n")
	for key, val := range quotaPodMap {
		fmt.Printf("        %v -> %v\n", key, val)
	}
	fmt.Printf("    Map dirPodMap contents:\n")
	for key, val := range dirPodMap {
		fmt.Printf("        %v -> %v\n", key, val)
	}
	fmt.Printf("    Map devApplierMap contents:\n")
	for key, val := range devApplierMap {
		fmt.Printf("        %v -> %v\n", key, val)
	}
	fmt.Printf("    Map dirApplierMap contents:\n")
	for key, val := range dirApplierMap {
		fmt.Printf("        %v -> %v\n", key, val)
	}
	fmt.Printf("    Map podDirCountMap contents:\n")
	for key, val := range podDirCountMap {
		fmt.Printf("        %v -> %v\n", key, val)
	}
	fmt.Printf("    Map quotaSizeMap contents:\n")
	for key, val := range quotaSizeMap {
		fmt.Printf("        %v -> %v\n", key, val)
	}
	fmt.Printf("    Map supportsQuotasMap contents:\n")
	for key, val := range supportsQuotasMap {
		fmt.Printf("        %v -> %v\n", key, val)
	}
	fmt.Printf("    Map backingDevMap contents:\n")
	for key, val := range backingDevMap {
		fmt.Printf("        %v -> %v\n", key, val)
	}
	fmt.Printf("    Map mountpointMap contents:\n")
	for key, val := range mountpointMap {
		fmt.Printf("        %v -> %v\n", key, val)
	}
	fmt.Printf("End maps %s\n", where)
}

// testIDQuotaMap 记录测试用配额 ID 到目录的映射。
var testIDQuotaMap = make(map[common.QuotaID]string)

// testQuotaIDMap 记录测试用目录到配额 ID 的映射。
var testQuotaIDMap = make(map[string]common.QuotaID)

// GetQuotaApplier 为 /quota1 挂载点返回虚拟施加器，其他挂载点返回 nil。
func (*VolumeProvider1) GetQuotaApplier(mountpoint string, backingDev string) common.LinuxVolumeQuotaApplier {
	if strings.HasPrefix(mountpoint, "/quota1") {
		return testVolumeQuota{}
	}
	return nil
}

// GetQuotaApplier 为 /quota2 挂载点返回虚拟施加器，其他挂载点返回 nil。
func (*VolumeProvider2) GetQuotaApplier(mountpoint string, backingDev string) common.LinuxVolumeQuotaApplier {
	if strings.HasPrefix(mountpoint, "/quota2") {
		return testVolumeQuota{}
	}
	return nil
}

// SetQuotaOnDir 在内存映射中设置目录配额，拒绝同一 ID 或目录被重复占用。
func (v testVolumeQuota) SetQuotaOnDir(dir string, id common.QuotaID, _ int64) error {
	odir, ok := testIDQuotaMap[id]
	if ok && dir != odir {
		return fmt.Errorf("ID %v is already in use", id)
	}
	oid, ok := testQuotaIDMap[dir]
	if ok && id != oid {
		return fmt.Errorf("directory %s already has a quota applied", dir)
	}
	testQuotaIDMap[dir] = id
	testIDQuotaMap[id] = dir
	return nil
}

// GetQuotaOnDir 返回目录上设置的配额 ID，未设置时返回 BadQuotaID 和错误。
func (v testVolumeQuota) GetQuotaOnDir(path string) (common.QuotaID, error) {
	id, ok := testQuotaIDMap[path]
	if ok {
		return id, nil
	}
	return common.BadQuotaID, fmt.Errorf("no quota available for %s", path)
}

// QuotaIDIsInUse 判断配额 ID 是否已被测试映射占用。
func (v testVolumeQuota) QuotaIDIsInUse(id common.QuotaID) (bool, error) {
	if _, ok := testIDQuotaMap[id]; ok {
		return true, nil
	}
	// 人为拒绝部分 ID，以覆盖未占用但不可用的情况。
	if id%3 == 0 {
		return false, nil
	}
	return false, nil
}

// GetConsumption 返回固定的模拟已用字节数。
func (v testVolumeQuota) GetConsumption(_ string, _ common.QuotaID) (int64, error) {
	return 4096, nil
}

// GetInodes 返回固定的模拟已用 inode 数。
func (v testVolumeQuota) GetInodes(_ string, _ common.QuotaID) (int64, error) {
	return 1, nil
}

// fakeSupportsQuotas 用假挂载器预填文件系统信息后检查路径是否支持配额。
func fakeSupportsQuotas(path string, userNamespacesEnabled bool) (bool, error) {
	dummySetFSInfo(path)
	return SupportsQuotas(dummyQuotaTest(), path, userNamespacesEnabled)
}

// fakeAssignQuota 用假挂载器为路径分配指定大小的配额。
func fakeAssignQuota(path string, poduid types.UID, bytes int64, userNamespacesEnabled bool) error {
	dummySetFSInfo(path)
	return AssignQuota(dummyQuotaTest(), path, poduid, resource.NewQuantity(bytes, resource.DecimalSI), userNamespacesEnabled)
}

// fakeClearQuota 用假挂载器清除路径上的配额。
func fakeClearQuota(path string, userNamespacesEnabled bool) error {
	dummySetFSInfo(path)
	return ClearQuota(dummyQuotaTest(), path, userNamespacesEnabled)
}

// quotaTestCase 是配额分配/清理用例：包含操作参数、期望的系统文件内容及各内部映射的数量变化。
type quotaTestCase struct {
	name                             string
	path                             string
	poduid                           types.UID
	bytes                            int64
	op                               string
	expectedProjects                 string
	expectedProjid                   string
	supportsQuota                    bool
	expectsSetQuota                  bool
	userNamespacesEnabled            bool
	deltaExpectedPodQuotaCount       int
	deltaExpectedDirQuotaCount       int
	deltaExpectedQuotaPodCount       int
	deltaExpectedDirPodCount         int
	deltaExpectedDevApplierCount     int
	deltaExpectedDirApplierCount     int
	deltaExpectedPodDirCountCount    int
	deltaExpectedQuotaSizeCount      int
	deltaExpectedSupportsQuotasCount int
	deltaExpectedBackingDevCount     int
	deltaExpectedMountpointCount     int
}

// 以下是系统配额文件 /etc/projects 与 /etc/projid 的期望内容片段。
const (
	projectsHeader = `# This is a /etc/projects header
1048578:/quota/d
`
	projects1 = `1048577:/quota1/a
`
	projects2 = `1048577:/quota1/a
1048580:/quota1/b
`
	projects3 = `1048577:/quota1/a
1048580:/quota1/b
1048581:/quota2/b
`
	projects4 = `1048577:/quota1/a
1048581:/quota2/b
`
	projects5 = `1048581:/quota2/b
`

	projidHeader = `# This is a /etc/projid header
xxxxxx:1048579
`
	projid1 = `volume1048577:1048577
`
	projid2 = `volume1048577:1048577
volume1048580:1048580
`
	projid3 = `volume1048577:1048577
volume1048580:1048580
volume1048581:1048581
`
	projid4 = `volume1048577:1048577
volume1048581:1048581
`
	projid5 = `volume1048581:1048581
`
)

// quotaTestCases 按执行顺序排列：依次分配、重复分配、清理，并校验每步后的映射状态。
var quotaTestCases = []quotaTestCase{
	{
		"SupportsQuotaOnQuotaVolumeWithUserNamespace",
		"/quota1/a", "", 1024, "Supports", "", "",
		true, true, true, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 1,
	},
	{
		"SupportsQuotaOnQuotaVolumeWithoutUserNamespace",
		"/quota1/a", "", 1024, "Supports", "", "",
		true, true, false, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	},
	{
		"AssignQuotaFirstTime",
		"/quota1/a", "", 1024, "Set", projects1, projid1,
		true, true, true, 1, 1, 1, 1, 0, 0, 1, 1, 0, 0, 0,
	},
	{
		"AssignQuotaFirstTime",
		"/quota1/b", "x", 1024, "Set", projects2, projid2,
		true, true, true, 1, 1, 1, 1, 0, 1, 1, 1, 1, 1, 1,
	},
	{
		"AssignQuotaFirstTime",
		"/quota2/b", "x", 1024, "Set", projects3, projid3,
		true, true, true, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	},
	{
		"AssignQuotaSecondTimeWithSameSize",
		"/quota1/b", "x", 1024, "Set", projects3, projid3,
		true, true, true, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	},
	{
		"AssignQuotaSecondTimeWithDifferentSize",
		"/quota2/b", "x", 2048, "Set", projects3, projid3,
		true, false, true, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	},
	{
		"ClearQuotaFirstTime",
		"/quota1/b", "", 1024, "Clear", projects4, projid4,
		true, true, true, -1, -1, -1, -1, 0, -1, -1, -1, -1, -1, -1,
	},
	{
		"SupportsQuotaOnNonQuotaVolume",
		"/noquota/a", "", 1024, "Supports", projects4, projid4,
		false, false, true, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	},
	{
		"ClearQuotaFirstTime",
		"/quota1/a", "", 1024, "Clear", projects5, projid5,
		true, true, true, -1, -1, -1, -1, 0, -1, -1, -1, -1, -1, -1,
	},
	{
		"ClearQuotaSecondTime",
		"/quota1/a", "", 1024, "Clear", projects5, projid5,
		true, false, true, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	},
	{
		"ClearQuotaFirstTime",
		"/quota2/b", "", 1024, "Clear", "", "",
		true, true, true, -1, -1, -1, -1, 0, -1, -1, -1, -1, -1, -1,
	},
}

// compareProjectsFiles 校验配额系统文件内容：特性关闭时期望只剩文件头。
func compareProjectsFiles(t *testing.T, testcase quotaTestCase, projectsFile string, projidFile string, enabled bool) {
	bytes, err := os.ReadFile(projectsFile)
	if err != nil {
		t.Error(err.Error())
	} else {
		s := string(bytes)
		p := projectsHeader
		if enabled {
			p += testcase.expectedProjects
		}
		if s != p {
			t.Errorf("Case %v /etc/projects miscompare: expected\n`%s`\ngot\n`%s`\n", testcase.path, p, s)
		}
	}
	bytes, err = os.ReadFile(projidFile)
	if err != nil {
		t.Error(err.Error())
	} else {
		s := string(bytes)
		p := projidHeader
		if enabled {
			p += testcase.expectedProjid
		}
		if s != p {
			t.Errorf("Case %v /etc/projid miscompare: expected\n`%s`\ngot\n`%s`\n", testcase.path, p, s)
		}
	}
}

// runCaseEnabled 在配额特性开启时执行单个用例并返回是否失败。
func runCaseEnabled(t *testing.T, testcase quotaTestCase, seq int) bool {
	fail := false
	var err error
	switch testcase.op {
	case "Supports":
		supports, err := fakeSupportsQuotas(testcase.path, testcase.userNamespacesEnabled)
		if err != nil {
			fail = true
			t.Errorf("Case %v (%s, %s, %v) Got error in fakeSupportsQuotas: %v", seq, testcase.name, testcase.path, true, err)
		}
		expectedSupport := testcase.supportsQuota && testcase.userNamespacesEnabled
		if supports != expectedSupport {
			fail = true
			t.Errorf("Case %v (%s, %s, userNamespacesEnabled: %v) fakeSupportsQuotas got %v, expect %v", seq, testcase.name, testcase.path, testcase.userNamespacesEnabled, supports, expectedSupport)
		}
		return fail
	case "Set":
		err = fakeAssignQuota(testcase.path, testcase.poduid, testcase.bytes, testcase.userNamespacesEnabled)
	case "Clear":
		err = fakeClearQuota(testcase.path, testcase.userNamespacesEnabled)
	case "GetConsumption":
		_, err = GetConsumption(testcase.path)
	case "GetInodes":
		_, err = GetInodes(testcase.path)
	default:
		t.Errorf("Case %v (%s, %s, %v) unknown operation %s", seq, testcase.name, testcase.path, true, testcase.op)
		return true
	}
	if err != nil && testcase.expectsSetQuota {
		fail = true
		t.Errorf("Case %v (%s, %s, %v) %s expected to clear quota but failed %v", seq, testcase.name, testcase.path, true, testcase.op, err)
	} else if err == nil && !testcase.expectsSetQuota {
		fail = true
		t.Errorf("Case %v (%s, %s, %v) %s expected not to clear quota but succeeded", seq, testcase.name, testcase.path, true, testcase.op)
	}
	return fail
}

// runCaseDisabled 在配额特性关闭时执行用例：所有操作都应报错，且不支持配额。
func runCaseDisabled(t *testing.T, testcase quotaTestCase, seq int) bool {
	var err error
	var supports bool
	switch testcase.op {
	case "Supports":
		if supports, _ = fakeSupportsQuotas(testcase.path, testcase.userNamespacesEnabled); supports {
			t.Errorf("Case %v (%s, %s, %v) supports quotas but shouldn't", seq, testcase.name, testcase.path, false)
			return true
		}
		return false
	case "Set":
		err = fakeAssignQuota(testcase.path, testcase.poduid, testcase.bytes, testcase.userNamespacesEnabled)
	case "Clear":
		err = fakeClearQuota(testcase.path, testcase.userNamespacesEnabled)
	case "GetConsumption":
		_, err = GetConsumption(testcase.path)
	case "GetInodes":
		_, err = GetInodes(testcase.path)
	default:
		t.Errorf("Case %v (%s, %s, %v) unknown operation %s", seq, testcase.name, testcase.path, false, testcase.op)
		return true
	}
	if err == nil {
		t.Errorf("Case %v (%s, %s, %v) %s: supports quotas but shouldn't", seq, testcase.name, testcase.path, false, testcase.op)
		return true
	}
	return false
}

// testAddRemoveQuotas 在给定特性开关下顺序执行全部配额用例，逐步校验文件内容与映射计数。
func testAddRemoveQuotas(t *testing.T, enabled bool) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.LocalStorageCapacityIsolationFSQuotaMonitoring, enabled)
	// 用临时文件模拟系统配额文件，避免触碰真实 /etc/projects 与 /etc/projid。
	tmpProjectsFile, err := os.CreateTemp("", "projects")
	if err == nil {
		_, err = tmpProjectsFile.WriteString(projectsHeader)
	}
	if err != nil {
		t.Errorf("Unable to create fake projects file")
	}
	projectsFile = tmpProjectsFile.Name()
	tmpProjectsFile.Close()
	tmpProjidFile, err := os.CreateTemp("", "projid")
	if err == nil {
		_, err = tmpProjidFile.WriteString(projidHeader)
	}
	if err != nil {
		t.Errorf("Unable to create fake projid file")
	}
	projidFile = tmpProjidFile.Name()
	tmpProjidFile.Close()
	providers = []common.LinuxVolumeQuotaProvider{
		&VolumeProvider1{},
		&VolumeProvider2{},
	}
	for k := range podQuotaMap {
		delete(podQuotaMap, k)
	}
	for k := range dirQuotaMap {
		delete(dirQuotaMap, k)
	}
	for k := range quotaPodMap {
		delete(quotaPodMap, k)
	}
	for k := range dirPodMap {
		delete(dirPodMap, k)
	}
	for k := range devApplierMap {
		delete(devApplierMap, k)
	}
	for k := range dirApplierMap {
		delete(dirApplierMap, k)
	}
	for k := range podDirCountMap {
		delete(podDirCountMap, k)
	}
	for k := range quotaSizeMap {
		delete(quotaSizeMap, k)
	}
	for k := range supportsQuotasMap {
		delete(supportsQuotasMap, k)
	}
	for k := range backingDevMap {
		delete(backingDevMap, k)
	}
	for k := range mountpointMap {
		delete(mountpointMap, k)
	}
	for k := range testIDQuotaMap {
		delete(testIDQuotaMap, k)
	}
	for k := range testQuotaIDMap {
		delete(testQuotaIDMap, k)
	}
	expectedPodQuotaCount := 0
	expectedDirQuotaCount := 0
	expectedQuotaPodCount := 0
	expectedDirPodCount := 0
	expectedDevApplierCount := 0
	expectedDirApplierCount := 0
	expectedPodDirCountCount := 0
	expectedQuotaSizeCount := 0
	expectedSupportsQuotasCount := 0
	expectedBackingDevCount := 0
	expectedMountpointCount := 0
	for seq, testcase := range quotaTestCases {
		if enabled {
			expectedPodQuotaCount += testcase.deltaExpectedPodQuotaCount
			expectedDirQuotaCount += testcase.deltaExpectedDirQuotaCount
			expectedQuotaPodCount += testcase.deltaExpectedQuotaPodCount
			expectedDirPodCount += testcase.deltaExpectedDirPodCount
			expectedDevApplierCount += testcase.deltaExpectedDevApplierCount
			expectedDirApplierCount += testcase.deltaExpectedDirApplierCount
			expectedPodDirCountCount += testcase.deltaExpectedPodDirCountCount
			expectedQuotaSizeCount += testcase.deltaExpectedQuotaSizeCount
			expectedSupportsQuotasCount += testcase.deltaExpectedSupportsQuotasCount
			expectedBackingDevCount += testcase.deltaExpectedBackingDevCount
			expectedMountpointCount += testcase.deltaExpectedMountpointCount
		}
		fail := false
		if enabled {
			fail = runCaseEnabled(t, testcase, seq)
		} else {
			fail = runCaseDisabled(t, testcase, seq)
		}

		compareProjectsFiles(t, testcase, projectsFile, projidFile, enabled)
		if len(podQuotaMap) != expectedPodQuotaCount {
			fail = true
			t.Errorf("Case %v (%s, %s, %v) podQuotaCount mismatch: got %v, expect %v", seq, testcase.name, testcase.path, enabled, len(podQuotaMap), expectedPodQuotaCount)
		}
		if len(dirQuotaMap) != expectedDirQuotaCount {
			fail = true
			t.Errorf("Case %v (%s, %s, %v) dirQuotaCount mismatch: got %v, expect %v", seq, testcase.name, testcase.path, enabled, len(dirQuotaMap), expectedDirQuotaCount)
		}
		if len(quotaPodMap) != expectedQuotaPodCount {
			fail = true
			t.Errorf("Case %v (%s, %s, %v) quotaPodCount mismatch: got %v, expect %v", seq, testcase.name, testcase.path, enabled, len(quotaPodMap), expectedQuotaPodCount)
		}
		if len(dirPodMap) != expectedDirPodCount {
			fail = true
			t.Errorf("Case %v (%s, %s, %v) dirPodCount mismatch: got %v, expect %v", seq, testcase.name, testcase.path, enabled, len(dirPodMap), expectedDirPodCount)
		}
		if len(devApplierMap) != expectedDevApplierCount {
			fail = true
			t.Errorf("Case %v (%s, %s, %v) devApplierCount mismatch: got %v, expect %v", seq, testcase.name, testcase.path, enabled, len(devApplierMap), expectedDevApplierCount)
		}
		if len(dirApplierMap) != expectedDirApplierCount {
			fail = true
			t.Errorf("Case %v (%s, %s, %v) dirApplierCount mismatch: got %v, expect %v", seq, testcase.name, testcase.path, enabled, len(dirApplierMap), expectedDirApplierCount)
		}
		if len(podDirCountMap) != expectedPodDirCountCount {
			fail = true
			t.Errorf("Case %v (%s, %s, %v) podDirCountCount mismatch: got %v, expect %v", seq, testcase.name, testcase.path, enabled, len(podDirCountMap), expectedPodDirCountCount)
		}
		if len(quotaSizeMap) != expectedQuotaSizeCount {
			fail = true
			t.Errorf("Case %v (%s, %s, %v) quotaSizeCount mismatch: got %v, expect %v", seq, testcase.name, testcase.path, enabled, len(quotaSizeMap), expectedQuotaSizeCount)
		}
		if len(supportsQuotasMap) != expectedSupportsQuotasCount {
			fail = true
			t.Errorf("Case %v (%s, %s, %v) supportsQuotasCount mismatch: got %v, expect %v", seq, testcase.name, testcase.path, enabled, len(supportsQuotasMap), expectedSupportsQuotasCount)
		}
		if len(backingDevMap) != expectedBackingDevCount {
			fail = true
			t.Errorf("Case %v (%s, %s, %v) BackingDevCount mismatch: got %v, expect %v", seq, testcase.name, testcase.path, enabled, len(backingDevMap), expectedBackingDevCount)
		}
		if len(mountpointMap) != expectedMountpointCount {
			fail = true
			t.Errorf("Case %v (%s, %s, %v) MountpointCount mismatch: got %v, expect %v", seq, testcase.name, testcase.path, enabled, len(mountpointMap), expectedMountpointCount)
		}
		if fail {
			logAllMaps(fmt.Sprintf("%v %s %s", seq, testcase.name, testcase.path))
		}
	}
	os.Remove(projectsFile)
	os.Remove(projidFile)
}

// TestAddRemoveQuotasEnabled 验证特性开启时配额的分配与清理行为。
func TestAddRemoveQuotasEnabled(t *testing.T) {
	testAddRemoveQuotas(t, true)
}

// TestAddRemoveQuotasDisabled 验证特性关闭时配额操作全部被拒绝。
func TestAddRemoveQuotasDisabled(t *testing.T) {
	testAddRemoveQuotas(t, false)
}
