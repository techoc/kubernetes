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

// Package fsquota 管理卷目录上的项目配额，包括配额支持检测、分配、查询与清理。
package fsquota

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"k8s.io/klog/v2"
	"k8s.io/mount-utils"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/kubernetes/pkg/volume/util/fsquota/common"
)

// podUidMap 记录内部 Pod UID 到外部（真实）Pod UID 的映射。
var podUidMap = make(map[types.UID]types.UID)

// podQuotaMap 记录内部 Pod UID 到配额 ID 的映射。
var podQuotaMap = make(map[types.UID]common.QuotaID)

// dirQuotaMap 记录目录到配额 ID 的映射，便于按目录直接查 ID。
var dirQuotaMap = make(map[string]common.QuotaID)

// quotaPodMap 记录配额 ID 到内部 Pod UID 的反向映射。
var quotaPodMap = make(map[common.QuotaID]types.UID)

// dirPodMap 记录目录到内部 Pod UID 的映射。
var dirPodMap = make(map[string]types.UID)

// devApplierMap 记录块设备到配额施加器的映射。
// 此表有意不清理：设备数量有限，保留映射可避免反复探测。
var devApplierMap = make(map[string]common.LinuxVolumeQuotaApplier)

// dirApplierMap 记录目录到配额施加器的映射；dirApplierLock 保护其并发访问。
var dirApplierMap = make(map[string]common.LinuxVolumeQuotaApplier)
var dirApplierLock sync.RWMutex

// podDirCountMap 记录内部 Pod UID 对应的目录引用计数。
var podDirCountMap = make(map[types.UID]int)

// quotaSizeMap 记录配额 ID 对应的字节上限；quotaLock 保护配额相关映射的并发访问。
var quotaSizeMap = make(map[common.QuotaID]int64)
var quotaLock sync.RWMutex

// supportsQuotasMap 缓存各路径是否支持配额；supportsQuotasLock 保护其并发访问。
var supportsQuotasMap = make(map[string]bool)
var supportsQuotasLock sync.RWMutex

// backingDevMap 记录目录到其所在块设备的映射；backingDevLock 保护其并发访问。
var backingDevMap = make(map[string]string)
var backingDevLock sync.RWMutex

// mountpointMap 记录目录到其挂载点的映射；mountpointLock 保护其并发访问。
var mountpointMap = make(map[string]string)
var mountpointLock sync.RWMutex

// providers 是探测配额支持时依次尝试的配额提供者列表。
var providers = []common.LinuxVolumeQuotaProvider{
	&common.VolumeProvider{},
}

// detectBackingDevInternal 从给定的 mounts 文件内容中查找挂载点对应的块设备。
// 拆出内部函数是为了便于测试，不必依赖真实的 /proc/self/mounts。
func detectBackingDevInternal(mountpoint string, mounts string) (string, error) {
	file, err := os.Open(mounts)
	if err != nil {
		return "", err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		match := common.MountParseRegexp.FindStringSubmatch(scanner.Text())
		if match != nil {
			device := match[1]
			mount := match[2]
			if mount == mountpoint {
				return device, nil
			}
		}
	}
	return "", fmt.Errorf("couldn't find backing device for %s", mountpoint)
}

// detectBackingDev 查询挂载点对应的块设备，假定传入的挂载点有效。
func detectBackingDev(_ mount.Interface, mountpoint string) (string, error) {
	return detectBackingDevInternal(mountpoint, common.MountsFile)
}

// clearBackingDev 清除路径的块设备缓存。
func clearBackingDev(path string) {
	backingDevLock.Lock()
	defer backingDevLock.Unlock()
	delete(backingDevMap, path)
}

// detectMountpointInternal 沿路径逐级向上查找真实的挂载点，假定路径已完全规范化。
// 拆出内部函数是为了便于测试。
func detectMountpointInternal(m mount.Interface, path string) (string, error) {
	for path != "" && path != "/" {
		// 按 k8s.io/mount-utils/mount_linux 的说法，除挂载之间的 bind mount 外，
		// 此方法能覆盖其余情况；这里只需拿到"真实"挂载点，足够用。
		//
		// 不用 IsNotMountPoint：它会扫描全部挂载，在挂载/卸载频繁时
		// 拿不到一致的 /proc/self/mounts 视图，导致超时和误报。
		isNotMount, err := m.IsLikelyNotMountPoint(path)
		if err != nil {
			return "/", err
		}
		if !isNotMount {
			return path, nil
		}
		path = filepath.Dir(path)
	}
	return "/", nil
}

// detectMountpoint 将路径转为绝对路径并解析符号链接后，再查找其挂载点。
func detectMountpoint(m mount.Interface, path string) (string, error) {
	xpath, err := filepath.Abs(path)
	if err != nil {
		return "/", err
	}
	xpath, err = filepath.EvalSymlinks(xpath)
	if err != nil {
		return "/", err
	}
	if xpath, err = detectMountpointInternal(m, xpath); err == nil {
		return xpath, nil
	}
	return "/", err
}

// clearMountpoint 清除路径的挂载点缓存。
func clearMountpoint(path string) {
	mountpointLock.Lock()
	defer mountpointLock.Unlock()
	delete(mountpointMap, path)
}

// clearSupportsQuotas 清除路径的配额支持缓存。
func clearSupportsQuotas(path string) {
	supportsQuotasLock.Lock()
	defer supportsQuotasLock.Unlock()
	delete(supportsQuotasMap, path)
}

// getFSInfo 返回路径的挂载点和块设备，并缓存结果，避免重复探测文件系统。
func getFSInfo(m mount.Interface, path string) (string, string, error) {
	mountpointLock.Lock()
	defer mountpointLock.Unlock()

	backingDevLock.Lock()
	defer backingDevLock.Unlock()

	var err error

	mountpoint, okMountpoint := mountpointMap[path]
	if !okMountpoint {
		mountpoint, err = detectMountpoint(m, path)
		if err != nil {
			return "", "", fmt.Errorf("cannot determine mountpoint for %s: %v", path, err)
		}
	}

	backingDev, okBackingDev := backingDevMap[path]
	if !okBackingDev {
		backingDev, err = detectBackingDev(m, mountpoint)
		if err != nil {
			return "", "", fmt.Errorf("cannot determine backing device for %s: %v", path, err)
		}
	}
	mountpointMap[path] = mountpoint
	backingDevMap[path] = backingDev
	return mountpoint, backingDev, nil
}

// clearFSInfo 清除路径的挂载点和块设备缓存。
func clearFSInfo(path string) {
	clearMountpoint(path)
	clearBackingDev(path)
}

// getApplier 返回目录对应的配额施加器；未设置配额时不返回施加器。
func getApplier(path string) common.LinuxVolumeQuotaApplier {
	dirApplierLock.Lock()
	defer dirApplierLock.Unlock()
	return dirApplierMap[path]
}

// setApplier 为目录设置配额施加器。
func setApplier(path string, applier common.LinuxVolumeQuotaApplier) {
	dirApplierLock.Lock()
	defer dirApplierLock.Unlock()
	dirApplierMap[path] = applier
}

// clearApplier 清除目录的配额施加器。
func clearApplier(path string) {
	dirApplierLock.Lock()
	defer dirApplierLock.Unlock()
	delete(dirApplierMap, path)
}

// setQuotaOnDir 通过目录对应的施加器设置配额；调用方需确保施加器可用。
func setQuotaOnDir(path string, id common.QuotaID, bytes int64) error {
	return getApplier(path).SetQuotaOnDir(path, id, bytes)
}

// GetQuotaOnDir 返回目录上已设置的配额 ID；出错时返回 BadQuotaID。
func GetQuotaOnDir(m mount.Interface, path string) (common.QuotaID, error) {
	_, _, err := getFSInfo(m, path)
	if err != nil {
		return common.BadQuotaID, err
	}
	return getApplier(path).GetQuotaOnDir(path)
}

// clearQuotaOnDir 清除目录上的配额及对应项目 ID，失败时记录日志并继续清理。
func clearQuotaOnDir(m mount.Interface, path string, userNamespacesEnabled bool) error {
	// 调用时路径可能不在缓存中，因此需显式检查，不能假定配额一定存在。
	klog.V(4).Infof("clearQuotaOnDir %s", path)
	supportsQuotas, err := SupportsQuotas(m, path, userNamespacesEnabled)
	if err != nil {
		// 出于尚未明确的向后兼容考虑，暂时只记录日志而不返回错误（后续再修订）。
		klog.V(3).Infof("Attempt to check for quota support failed: %v", err)
	}
	if !supportsQuotas {
		return nil
	}
	projid, err := GetQuotaOnDir(m, path)
	if err == nil && projid != common.BadQuotaID {
		// 目录上有配额却清不掉，属于异常，需要记录后续失败。
		err = setQuotaOnDir(path, projid, 0)
		if err != nil {
			klog.V(3).Infof("Attempt to clear quota failed: %v", err)
		}
		// 即使清配额失败，也要尝试移除项目 ID，否则会留下悬挂记录。
		err1 := removeProjectID(path, projid)
		if err1 != nil {
			klog.V(3).Infof("Attempt to remove quota ID from system files failed: %v", err1)
		}
		clearFSInfo(path)
		if err != nil {
			return err
		}
		return err1
	}
	// 查不到配额也无妨：可能本来就没设置过，且无从区分这种情况。
	klog.V(3).Infof("clearQuotaOnDir fails %v", err)
	return nil
}

// SupportsQuotas 判断路径是否支持配额。
// 仅缓存支持配额的路径的施加器；不支持的结果不缓存，因为没有地方清理它。
// 但设备到施加器的映射可以缓存，因为设备数量有限。
// 用户命名空间会阻止修改文件系统上的项目 ID，以确保 xfs-quota 指标可靠，
// 因此需要检查 userNamespacesEnabled。
func SupportsQuotas(m mount.Interface, path string, userNamespacesEnabled bool) (bool, error) {
	if !enabledQuotasForMonitoring() {
		klog.V(3).Info("SupportsQuotas called, but quotas disabled")
		return false, nil
	}
	if !userNamespacesEnabled {
		klog.V(3).Info("SupportQuotas called and LocalStorageCapacityIsolationFSQuotaMonitoring enabled, but pod is not in a user namespace")
		return false, nil
	}
	supportsQuotasLock.Lock()
	defer supportsQuotasLock.Unlock()
	if supportsQuotas, ok := supportsQuotasMap[path]; ok {
		return supportsQuotas, nil
	}
	mount, dev, err := getFSInfo(m, path)
	if err != nil {
		return false, err
	}
	// 该设备是否已经探测过施加器？缓存命中就跳过探测。
	applier, ok := devApplierMap[mount]
	if !ok {
		for _, provider := range providers {
			if applier = provider.GetQuotaApplier(mount, dev); applier != nil {
				devApplierMap[mount] = applier
				break
			}
		}
	}
	if applier != nil {
		supportsQuotasMap[path] = true
		setApplier(path, applier)
		return true, nil
	}
	delete(backingDevMap, path)
	delete(mountpointMap, path)
	return false, nil
}

// AssignQuota 为指定目录分配配额。
// 配额 ID 依据 Pod UID 和路径选定；若 Pod UID 与已知 pod 相同，
// 理论上（目前尚未实现）可与同 pod 的其他卷共用配额 ID。
func AssignQuota(m mount.Interface, path string, poduid types.UID, bytes *resource.Quantity, userNamespacesEnabled bool) error { //nolint:staticcheck
	if bytes == nil {
		return fmt.Errorf("attempting to assign null quota to %s", path)
	}
	ibytes := bytes.Value()
	if ok, err := SupportsQuotas(m, path, userNamespacesEnabled); !ok {
		return fmt.Errorf("quotas not supported on %s: %v", path, err)
	}
	quotaLock.Lock()
	defer quotaLock.Unlock()
	// 当前策略是每个卷单独设配额：为每个新卷生成随机 UUID 作为内部 Pod UID。
	// 在 fsquota 看来，每个卷都属于一个独一无二的 pod。
	// 若以后改为一个 pod 的所有卷共用一个配额，直接用 poduid 参数即可；
	// 若确定永久采用每卷一配额，则这套 pod 相关代码都可以删掉。
	externalPodUid := poduid
	internalPodUid, ok := dirPodMap[path]
	if ok {
		if podUidMap[internalPodUid] != externalPodUid {
			return fmt.Errorf("requesting quota on existing directory %s but different pod %s %s", path, podUidMap[internalPodUid], externalPodUid)
		}
	} else {
		internalPodUid = types.UID(uuid.NewUUID())
	}
	oid, ok := podQuotaMap[internalPodUid]
	if ok {
		if quotaSizeMap[oid] != ibytes {
			return fmt.Errorf("requesting quota of different size: old %v new %v", quotaSizeMap[oid], bytes)
		}
		if _, ok := dirPodMap[path]; ok {
			return nil
		}
	} else {
		oid = common.BadQuotaID
	}
	id, err := createProjectID(path, oid)
	if err == nil {
		if oid != common.BadQuotaID && oid != id {
			return fmt.Errorf("attempt to reassign quota %v to %v", oid, id)
		}
		// 若启用强制配额，这里还要附加"强制配额已禁用"的条件。
		fsbytes := ibytes
		if fsbytes > 0 {
			fsbytes = -1
		}
		if err = setQuotaOnDir(path, id, fsbytes); err == nil {
			quotaPodMap[id] = internalPodUid
			quotaSizeMap[id] = ibytes
			podQuotaMap[internalPodUid] = id
			dirQuotaMap[path] = id
			dirPodMap[path] = internalPodUid
			podUidMap[internalPodUid] = externalPodUid
			podDirCountMap[internalPodUid]++
			klog.V(4).Infof("Assigning quota ID %d (request limit %d, actual limit %d) to %s", id, ibytes, fsbytes, path)
			return nil
		}
		removeProjectID(path, id)
	}
	return fmt.Errorf("assign quota FAILED %v", err)
}

// GetConsumption 返回目录已用字节数；目录不受配额管理时返回 (nil, nil)。
func GetConsumption(path string) (*resource.Quantity, error) {
	// 锁必须至少持有到配额命令执行完毕，防止期间目录配额被回收。
	quotaLock.Lock()
	defer quotaLock.Unlock()
	applier := getApplier(path)
	// 没有施加器说明该目录不受配额管理。
	if applier == nil {
		return nil, nil
	}
	ibytes, err := applier.GetConsumption(path, dirQuotaMap[path])
	if err != nil {
		return nil, err
	}
	return resource.NewQuantity(ibytes, resource.DecimalSI), nil
}

// GetInodes 返回目录下已用的 inode 数；目录不受配额管理时返回 (nil, nil)。
func GetInodes(path string) (*resource.Quantity, error) {
	// 锁必须至少持有到配额命令执行完毕，防止期间目录配额被回收。
	quotaLock.Lock()
	defer quotaLock.Unlock()
	applier := getApplier(path)
	// 没有施加器说明该目录不受配额管理。
	if applier == nil {
		return nil, nil
	}
	inodes, err := applier.GetInodes(path, dirQuotaMap[path])
	if err != nil {
		return nil, err
	}
	return resource.NewQuantity(inodes, resource.DecimalSI), nil
}

// ClearQuota 移除目录上分配的配额，并清理相关缓存与项目 ID。
func ClearQuota(m mount.Interface, path string, userNamespacesEnabled bool) error {
	klog.V(3).Infof("ClearQuota %s", path)
	if !enabledQuotasForMonitoring() {
		return fmt.Errorf("clearQuota called, but quotas disabled")
	}
	quotaLock.Lock()
	defer quotaLock.Unlock()
	poduid, ok := dirPodMap[path]
	if !ok {
		// 缓存里没有该目录，可能是原本就没有配额，也可能是在清理陈旧目录；
		// 因此若真发现配额，直接清掉即可。清配额需要施加器，而它本身也要清理，
		// 故用 defer 保证路径缓存和施加器一并清除。
		defer clearSupportsQuotas(path)
		defer clearApplier(path)
		return clearQuotaOnDir(m, path, userNamespacesEnabled)
	}
	_, ok = podQuotaMap[poduid]
	if !ok {
		return fmt.Errorf("clearQuota: No quota available for %s", path)
	}
	projid, err := GetQuotaOnDir(m, path)
	if err != nil {
		// 出于尚未明确的向后兼容考虑，暂时只记录日志而不返回错误（后续再修订）。
		klog.V(3).Infof("Attempt to check quota ID %v on dir %s failed: %v", dirQuotaMap[path], path, err)
	}
	if projid != dirQuotaMap[path] {
		return fmt.Errorf("expected quota ID %v on dir %s does not match actual %v", dirQuotaMap[path], path, projid)
	}
	count, ok := podDirCountMap[poduid]
	if count <= 1 || !ok {
		err = clearQuotaOnDir(m, path, userNamespacesEnabled)
		// 此错误仍需记录，但后续清理照常进行。
		if err != nil {
			klog.V(3).Infof("Unable to clear quota %v %s: %v", dirQuotaMap[path], path, err)
		}
		delete(quotaSizeMap, podQuotaMap[poduid])
		delete(quotaPodMap, podQuotaMap[poduid])
		delete(podDirCountMap, poduid)
		delete(podQuotaMap, poduid)
		delete(podUidMap, poduid)
	} else {
		err = removeProjectID(path, projid)
		podDirCountMap[poduid]--
		klog.V(4).Infof("Not clearing quota for pod %s; still %v dirs outstanding", poduid, podDirCountMap[poduid])
	}
	delete(dirPodMap, path)
	delete(dirQuotaMap, path)
	clearSupportsQuotas(path)
	clearApplier(path)
	if err != nil {
		return fmt.Errorf("unable to clear quota for %s: %v", path, err)
	}
	return nil
}
