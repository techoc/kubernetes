/*
Copyright 2015 The Kubernetes Authors.

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

package iscsi

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"k8s.io/klog/v2"
	"k8s.io/mount-utils"
	utilexec "k8s.io/utils/exec"

	v1 "k8s.io/api/core/v1"
	"k8s.io/kubernetes/pkg/kubelet/kubeletconfig"
	"k8s.io/kubernetes/pkg/volume"
	volumeutil "k8s.io/kubernetes/pkg/volume/util"
	"k8s.io/kubernetes/pkg/volume/util/types"
)

const (
	// 请求多路径卷时，卷插件认为足够的最少路径数。
	minMultipathCount = 2

	// 附加多路径卷所有路径的最少尝试次数。达到该次数后，
	// 若已有至少 minMultipathCount 条路径可用，就继续挂载卷。
	minAttachAttempts = 2

	// 尝试附加至少 minMultipathCount 条路径的总次数上限。若始终不足，
	// 插件至少会尝试这么多次；超过后即使只有一条路径也会挂载。
	maxAttachAttempts = 5

	// 至少有两条路径可用时，等待多路径设备出现的秒数。
	multipathDeviceTimeout = 10

	// 放弃前等待设备/路径出现的秒数。
	deviceDiscoveryTimeout = 30

	// 'iscsiadm' 中表示会话已登录的错误码。
	// 参见 https://github.com/open-iscsi/open-iscsi/blob/7d121d12ad6ba7783308c25ffd338a9fa0cc402b/include/iscsi_err.h#L37-L38
	iscsiadmErrorSessExists = 15

	// iscsiadm 退出码："找不到会话"。
	exit_ISCSI_ERR_SESS_NOT_FOUND = 2
	// iscsiadm 退出码："没有可操作的记录/目标/会话/portal"。
	exit_ISCSI_ERR_NO_OBJS_FOUND = 21
)

var (
	chapSt = []string{
		"discovery.sendtargets.auth.username",
		"discovery.sendtargets.auth.password",
		"discovery.sendtargets.auth.username_in",
		"discovery.sendtargets.auth.password_in"}
	chapSess = []string{
		"node.session.auth.username",
		"node.session.auth.password",
		"node.session.auth.username_in",
		"node.session.auth.password_in"}
	ifaceTransportNameRe = regexp.MustCompile(`iface.transport_name = (.*)\n`)
	ifaceRe              = regexp.MustCompile(`.+/iface-([^/]+)/.+`)
)

// updateISCSIDiscoverydb 在启用发现阶段 CHAP 时更新 discoverydb 的认证配置。
func updateISCSIDiscoverydb(b iscsiDiskMounter, tp string) error {
	if !b.chapDiscovery {
		return nil
	}
	out, err := execWithLog(b, "iscsiadm", "-m", "discoverydb", "-t", "sendtargets", "-p", tp, "-I", b.Iface, "-o", "update", "-n", "discovery.sendtargets.auth.authmethod", "-v", "CHAP")
	if err != nil {
		return fmt.Errorf("iscsi: failed to update discoverydb with CHAP, output: %v", out)
	}

	for _, k := range chapSt {
		v := b.secret[k]
		if len(v) > 0 {
			// 刻意不用 execWithLog，避免把密钥写进日志
			out, err := b.exec.Command("iscsiadm", "-m", "discoverydb", "-t", "sendtargets", "-p", tp, "-I", b.Iface, "-o", "update", "-n", k, "-v", v).CombinedOutput()
			if err != nil {
				return fmt.Errorf("iscsi: failed to update discoverydb key %q error: %v", k, string(out))
			}
		}
	}
	return nil
}

// updateISCSINode 更新节点记录：关闭自动扫描并按需配置会话 CHAP。
func updateISCSINode(b iscsiDiskMounter, tp string) error {
	// 将 node.session.scan 设为 manual，以规避 https://github.com/kubernetes/kubernetes/issues/90982
	out, err := execWithLog(b, "iscsiadm", "-m", "node", "-p", tp, "-T", b.Iqn, "-I", b.Iface, "-o", "update", "-n", "node.session.scan", "-v", "manual")
	if err != nil {
		// iscsiadm 失败或版本不支持 node.session.scan 时不报错，仅告警提示可能的暴露风险
		klog.Warningf("iscsi: failed to update node with node.session.scan=manual, possible exposure to issue 90982: %v", out)
	}

	if !b.chapSession {
		return nil
	}

	out, err = execWithLog(b, "iscsiadm", "-m", "node", "-p", tp, "-T", b.Iqn, "-I", b.Iface, "-o", "update", "-n", "node.session.auth.authmethod", "-v", "CHAP")
	if err != nil {
		return fmt.Errorf("iscsi: failed to update node with CHAP, output: %v", out)
	}

	for _, k := range chapSess {
		v := b.secret[k]
		if len(v) > 0 {
			// 刻意不用 execWithLog，避免把密钥写进日志
			out, err := b.exec.Command("iscsiadm", "-m", "node", "-p", tp, "-T", b.Iqn, "-I", b.Iface, "-o", "update", "-n", k, "-v", v).CombinedOutput()
			if err != nil {
				return fmt.Errorf("iscsi: failed to update node session key %q error: %v", k, string(out))
			}
		}
	}
	return nil
}

// StatFunc 和 GlobFunc 抽象文件系统查询，便于单元测试注入。
// 路径不存在时最多重试 maxRetries 次；非默认传输方式下设备 PCI id 未知，改用 glob 匹配。
type StatFunc func(string) (os.FileInfo, error)
type GlobFunc func(string) ([]string, error)

// waitForPathToExist 使用真实的 os.Stat 和 filepath.Glob 等待设备路径出现。
func waitForPathToExist(devicePath *string, maxRetries int, deviceTransport string) bool {
	// 抽象成函数参数便于单元测试
	return waitForPathToExistInternal(devicePath, maxRetries, deviceTransport, os.Stat, filepath.Glob)
}

// waitForPathToExistInternal 是等待设备路径出现的可注入实现。
func waitForPathToExistInternal(devicePath *string, maxRetries int, deviceTransport string, osStat StatFunc, filepathGlob GlobFunc) bool {
	if devicePath == nil {
		return false
	}

	for i := 0; i < maxRetries; i++ {
		var err error
		if deviceTransport == "tcp" {
			_, err = osStat(*devicePath)
		} else {
			fpath, _ := filepathGlob(*devicePath)
			if fpath == nil {
				err = os.ErrNotExist
			} else {
				// 多个 PCI 设备连接同一 iSCSI 目标时可能匹配到多条路径，
				// 后续逻辑会处理这种情况，这里只取第一条。
				*devicePath = fpath[0]
			}
		}
		if err == nil {
			return true
		}
		if !os.IsNotExist(err) {
			return false
		}
		if i == maxRetries-1 {
			break
		}
		time.Sleep(time.Second)
	}
	return false
}

// makePDNameInternal 生成类似 /var/lib/kubelet/plugins/kubernetes.io/iscsi/iface_name/portal-some_iqn-lun-lun_id 的目录名。
func makePDNameInternal(host volume.VolumeHost, portal string, iqn string, lun string, iface string) string {
	return filepath.Join(host.GetPluginDir(iscsiPluginName), "iface-"+iface, portal+"-"+iqn+"-lun-"+lun)
}

// makeVDPDNameInternal 生成类似 /var/lib/kubelet/plugins/kubernetes.io/iscsi/volumeDevices/iface_name/portal-some_iqn-lun-lun_id 的目录名。
func makeVDPDNameInternal(host volume.VolumeHost, portal string, iqn string, lun string, iface string) string {
	return filepath.Join(host.GetVolumeDevicePluginDir(iscsiPluginName), "iface-"+iface, portal+"-"+iqn+"-lun-"+lun)
}

// ISCSIUtil 实现 iSCSI 卷的附加、分离和配置持久化操作。
type ISCSIUtil struct{}

// MakeGlobalPDName 返回文件系统卷的全局插件目录路径。
func (util *ISCSIUtil) MakeGlobalPDName(iscsi iscsiDisk) string {
	return makePDNameInternal(iscsi.plugin.host, iscsi.Portals[0], iscsi.Iqn, iscsi.Lun, iscsi.Iface)
}

// MakeGlobalVDPDName 返回块设备卷的全局插件目录路径。
func (util *ISCSIUtil) MakeGlobalVDPDName(iscsi iscsiDisk) string {
	return makeVDPDNameInternal(iscsi.plugin.host, iscsi.Portals[0], iscsi.Iqn, iscsi.Lun, iscsi.Iface)
}

// persistISCSIFile 将 iSCSI 卷配置写入指定目录，供 DetachDisk 使用。
func (util *ISCSIUtil) persistISCSIFile(conf iscsiDisk, mnt string) error {
	file := filepath.Join(mnt, "iscsi.json")
	fp, err := os.Create(file)
	if err != nil {
		return fmt.Errorf("iscsi: create %s err %s", file, err)
	}
	defer fp.Close()
	encoder := json.NewEncoder(fp)
	if err = encoder.Encode(conf); err != nil {
		return fmt.Errorf("iscsi: encode err: %v", err)
	}
	return nil
}

// loadISCSI 从指定目录读取持久化的 iSCSI 卷配置。
func (util *ISCSIUtil) loadISCSI(conf *iscsiDisk, mnt string) error {
	file := filepath.Join(mnt, "iscsi.json")
	fp, err := os.Open(file)
	if err != nil {
		return fmt.Errorf("iscsi: open %s err %s", file, err)
	}
	defer fp.Close()
	decoder := json.NewDecoder(fp)
	if err = decoder.Decode(conf); err != nil {
		return fmt.Errorf("iscsi: decode err: %v", err)
	}
	return nil
}

// scanOneLun 只扫描指定 SCSI 总线上的单个 LUN。
// 扫描整条总线会让内核发现本不该看到的 LUN，这些多余 LUN 被删除时我们收不到通知，
// 内核清理不干净会导致 I/O 错误和超时，明显拖慢后续操作。
func scanOneLun(hostNumber int, lunNumber int) error {
	filename := fmt.Sprintf("/sys/class/scsi_host/host%d/scan", hostNumber)
	fd, err := os.OpenFile(filename, os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	defer fd.Close()

	// iSCSI 的 Channel/Target 固定为 0
	scanCmd := fmt.Sprintf("0 0 %d", lunNumber)
	if written, err := fd.WriteString(scanCmd); err != nil {
		return err
	} else if 0 == written {
		return fmt.Errorf("no data written to file: %s", filename)
	}

	klog.V(3).Infof("Scanned SCSI host %d LUN %d", hostNumber, lunNumber)
	return nil
}

// waitForMultiPathToExist 尝试为给定路径找到对应的多路径设备，找不到返回空串。
func waitForMultiPathToExist(devicePaths []string, maxRetries int, deviceUtil volumeutil.DeviceUtil) string {
	if 0 == len(devicePaths) {
		return ""
	}

	for i := 0; i < maxRetries; i++ {
		for _, path := range devicePaths {
			// 理论上不应出现空路径，保险起见跳过
			if path == "" {
				continue
			}
			// 检查设备是否属于多路径，是则使用 dm-XX 设备
			if mappedDevicePath := deviceUtil.FindMultipathDeviceForDevice(path); mappedDevicePath != "" {
				return mappedDevicePath
			}
		}
		if i == maxRetries-1 {
			break
		}
		time.Sleep(time.Second)
	}
	return ""
}

// AttachDisk 附加 iSCSI 卷，成功时返回设备路径，失败时返回错误。
func (util *ISCSIUtil) AttachDisk(b iscsiDiskMounter) (string, error) {
	var devicePath string
	devicePaths := map[string]string{}
	var iscsiTransport string
	var lastErr error

	out, err := execWithLog(b, "iscsiadm", "-m", "iface", "-I", b.InitIface, "-o", "show")
	if err != nil {
		klog.Errorf("iscsi: could not read iface %s error: %s", b.InitIface, out)
		return "", err
	}

	iscsiTransport = extractTransportname(out)

	bkpPortal := b.Portals

	// 指定了发起端名称时 iface 尚未创建，需复制预配置 iface 的参数后新建
	if b.InitiatorName != "" {
		if err = cloneIface(b); err != nil {
			klog.Errorf("iscsi: failed to clone iface: %s error: %v", b.InitIface, err)
			return "", err
		}
	}

	// 登录期间锁定目标，避免共享同一目标的两个卷同时登录，或一边登录一边登出。
	b.plugin.targetLocks.LockKey(b.Iqn)
	defer b.plugin.targetLocks.UnlockKey(b.Iqn)

	// 建立 portal 到 SCSI host 的映射，后续按此执行总线重扫。
	portalHostMap, err := b.deviceUtil.GetISCSIPortalHostMapForTarget(b.Iqn)
	if err != nil {
		return "", err
	}
	klog.V(4).Infof("AttachDisk portal->host map for %s is %v", b.Iqn, portalHostMap)

	for i := 1; i <= maxAttachAttempts; i++ {
		for _, tp := range bkpPortal {
			if _, found := devicePaths[tp]; found {
				klog.V(4).Infof("Device for portal %q already known", tp)
				continue
			}

			hostNumber, loggedIn := portalHostMap[tp]
			if !loggedIn {
				klog.V(4).Infof("Could not get SCSI host number for portal %s, will attempt login", tp)

				// 建立 discoverydb 记录并发现 iSCSI 目标
				execWithLog(b, "iscsiadm", "-m", "discoverydb", "-t", "sendtargets", "-p", tp, "-I", b.Iface, "-o", "new")

				// 用 CHAP 密钥更新 discoverydb
				err = updateISCSIDiscoverydb(b, tp)
				if err != nil {
					lastErr = fmt.Errorf("iscsi: failed to update discoverydb to portal %s error: %v", tp, err)
					continue
				}

				out, err = execWithLog(b, "iscsiadm", "-m", "discoverydb", "-t", "sendtargets", "-p", tp, "-I", b.Iface, "--discover")
				if err != nil {
					// 发现失败时删除 discoverydb 记录
					execWithLog(b, "iscsiadm", "-m", "discoverydb", "-t", "sendtargets", "-p", tp, "-I", b.Iface, "-o", "delete")
					lastErr = fmt.Errorf("iscsi: failed to sendtargets to portal %s output: %s, err %v", tp, out, err)
					continue
				}

				err = updateISCSINode(b, tp)
				if err != nil {
					// 更新 node 库失败很少见，但删除记录会影响已在使用的客户端，故保留
					lastErr = fmt.Errorf("iscsi: failed to update iscsi node to portal %s error: %v", tp, err)
					continue
				}

				// 登录 iSCSI 目标
				out, err = execWithLog(b, "iscsiadm", "-m", "node", "-p", tp, "-T", b.Iqn, "-I", b.Iface, "--login")
				if err != nil {
					// 登录失败时删除 node 记录
					execWithLog(b, "iscsiadm", "-m", "node", "-p", tp, "-I", b.Iface, "-T", b.Iqn, "-o", "delete")
					lastErr = fmt.Errorf("iscsi: failed to attach disk: Error: %s (%v)", out, err)
					continue
				}

				// 显式设为手动登录，避免节点故障/重启后在启动时卡住
				_, err = execWithLog(b, "iscsiadm", "-m", "node", "-p", tp, "-T", b.Iqn, "-o", "update", "-n", "node.startup", "-v", "manual")
				if err != nil {
					// 设置启动模式失败不视为错误，但记录告警以便排查
					klog.Warningf("Failed to set iSCSI login mode to manual: %v", err)
				}

				// 登录后重建 host 映射
				portalHostMap, err := b.deviceUtil.GetISCSIPortalHostMapForTarget(b.Iqn)
				if err != nil {
					return "", err
				}
				klog.V(6).Infof("AttachDisk portal->host map for %s is %v", b.Iqn, portalHostMap)

				hostNumber, loggedIn = portalHostMap[tp]
				if !loggedIn {
					klog.Warningf("Could not get SCSI host number for portal %s after logging in", tp)
					continue
				}
			}

			klog.V(5).Infof("AttachDisk: scanning SCSI host %d LUN %s", hostNumber, b.Lun)
			lunNumber, err := strconv.Atoi(b.Lun)
			if err != nil {
				return "", fmt.Errorf("AttachDisk: lun is not a number: %s\nError: %v", b.Lun, err)
			}

			// 扫描 iSCSI 总线以发现 LUN
			err = scanOneLun(hostNumber, lunNumber)
			if err != nil {
				return "", err
			}

			if iscsiTransport == "" {
				klog.Errorf("iscsi: could not find transport name in iface %s", b.Iface)
				return "", fmt.Errorf("could not parse iface file for %s", b.Iface)
			}

			addr := tp
			if strings.HasPrefix(tp, "[") {
				// /dev/disk/by-path 下的链接名不含 IPv6 地址的方括号
				addr = strings.NewReplacer("[", "", "]", "").Replace(tp)
			}
			if iscsiTransport == "tcp" {
				devicePath = strings.Join([]string{"/dev/disk/by-path/ip", addr, "iscsi", b.Iqn, "lun", b.Lun}, "-")
			} else {
				devicePath = strings.Join([]string{"/dev/disk/by-path/pci", "*", "ip", addr, "iscsi", b.Iqn, "lun", b.Lun}, "-")
			}

			if exist := waitForPathToExist(&devicePath, deviceDiscoveryTimeout, iscsiTransport); !exist {
				msg := fmt.Sprintf("Timed out waiting for device at path %s after %ds", devicePath, deviceDiscoveryTimeout)
				klog.Error(msg)
				// 记录最后一次错误
				lastErr = errors.New(msg)
				continue
			} else {
				devicePaths[tp] = devicePath
			}
		}
		klog.V(4).Infof("iscsi: tried all devices for %q %d times, %d paths found", b.Iqn, i, len(devicePaths))
		if len(devicePaths) == 0 {
			// 一条路径都没附加成功：报错停止，kubelet 稍后会重试；同时删除克隆的 iface
			execWithLog(b, "iscsiadm", "-m", "iface", "-I", b.Iface, "-o", "delete")
			klog.Errorf("iscsi: failed to get any path for iscsi disk, last err seen:\n%v", lastErr)
			return "", fmt.Errorf("failed to get any path for iscsi disk, last err seen:\n%v", lastErr)
		}
		if len(devicePaths) == len(bkpPortal) {
			// 所有路径均已找到
			klog.V(4).Infof("iscsi: all devices for %q found", b.Iqn)
			break
		}
		if len(devicePaths) >= minMultipathCount && i >= minAttachAttempts {
			// 已有多路径所需的最少路径数，且其余路径已尝试足够久
			klog.V(4).Infof("%d devices found for %q", len(devicePaths), b.Iqn)
			break
		}
	}

	if lastErr != nil {
		klog.Errorf("iscsi: last error occurred during iscsi init:\n%v", lastErr)
	}

	devicePathList := []string{}
	for _, path := range devicePaths {
		devicePathList = append(devicePathList, path)
	}
	// 尝试为卷找到多路径设备
	if len(bkpPortal) > 1 {
		// 请求了多路径卷：最多等待 multipathDeviceTimeout 秒让多路径设备出现。
		devicePath = waitForMultiPathToExist(devicePathList, multipathDeviceTimeout, b.deviceUtil)
	} else {
		// 单 portal 的 PV 只尝试一次，避免多路径设备根本不会出现时长时间卡顿，
		// 同时保持旧行为。
		devicePath = waitForMultiPathToExist(devicePathList, 1, b.deviceUtil)
	}

	// 找不到多路径设备时，直接使用第一条设备路径（通常也是唯一一条）
	if devicePath == "" {
		devicePath = devicePathList[0]
	}

	klog.V(5).Infof("iscsi: AttachDisk devicePath: %s", devicePath)

	if err = util.persistISCSI(b); err != nil {
		// 返回"进度不确定"错误，使 kubelet 在 Pod 删除时仍调用 Unmount/Unmap
		return "", types.NewUncertainProgressError(err.Error())
	}
	return devicePath, nil
}

// persistISCSI 将 iSCSI 卷配置写入全局挂载/映射目录，供 DetachDisk 使用。
func (util *ISCSIUtil) persistISCSI(b iscsiDiskMounter) error {
	klog.V(5).Infof("iscsi: AttachDisk volumeMode: %s", b.volumeMode)
	var globalPDPath string
	if b.volumeMode == v1.PersistentVolumeBlock {
		globalPDPath = b.manager.MakeGlobalVDPDName(*b.iscsiDisk)
	} else {
		globalPDPath = b.manager.MakeGlobalPDName(*b.iscsiDisk)
	}

	if err := os.MkdirAll(globalPDPath, 0750); err != nil {
		klog.Errorf("iscsi: failed to mkdir %s: %v", globalPDPath, err)
		return err
	}

	if b.volumeMode == v1.PersistentVolumeFilesystem {
		notMnt, err := b.mounter.IsLikelyNotMountPoint(globalPDPath)
		if err != nil {
			return err
		}
		if !notMnt {
			// 卷已挂载，说明此前的 WaitForAttach 已持久化元数据。而且元数据文件位于
			// globalPDPath 内部，此刻被卷挂载遮蔽，无法写入。
			klog.V(4).Infof("Skipping persistISCSI, the volume is already mounted at %s", globalPDPath)
			return nil
		}
	}

	// 将磁盘配置写成 JSON 文件，供 DetachDisk 读取
	return util.persistISCSIFile(*(b.iscsiDisk), globalPDPath)
}

// deleteDevice 从 SCSI 子系统中删除一个形如 sd* 的块设备。
func deleteDevice(deviceName string) error {
	filename := fmt.Sprintf("/sys/block/%s/device/delete", deviceName)
	fd, err := os.OpenFile(filename, os.O_WRONLY, 0)
	if err != nil {
		// 文件不存在说明设备已删除，直接返回成功
		return nil
	}
	defer fd.Close()

	if written, err := fd.WriteString("1"); err != nil {
		return err
	} else if 0 == written {
		return fmt.Errorf("no data written to file: %s", filename)
	}
	klog.V(4).Infof("Deleted block device: %s", deviceName)
	return nil
}

// deleteDevices 删除与给定 iSCSI 设备关联的所有块设备和多路径设备。
func deleteDevices(c iscsiDiskUnmounter) error {
	lunNumber, err := strconv.Atoi(c.iscsiDisk.Lun)
	if err != nil {
		klog.Errorf("iscsi delete devices: lun is not a number: %s\nError: %v", c.iscsiDisk.Lun, err)
		return err
	}
	// 先枚举出要删除的设备
	deviceNames, err := c.deviceUtil.FindDevicesForISCSILun(c.iscsiDisk.Iqn, lunNumber)
	if err != nil {
		klog.Errorf("iscsi delete devices: could not get devices associated with LUN %d on target %s\nError: %v",
			lunNumber, c.iscsiDisk.Iqn, err)
		return err
	}
	// 找出关联的多路径设备
	mpathDevices := make(map[string]bool)
	for _, deviceName := range deviceNames {
		path := "/dev/" + deviceName
		// 检查设备是否属于多路径，是则记录其 dm-XX 设备
		if mappedDevicePath := c.deviceUtil.FindMultipathDeviceForDevice(path); mappedDevicePath != "" {
			mpathDevices[mappedDevicePath] = true
		}
	}
	// 清理多路径设备映射
	for mpathDevice := range mpathDevices {
		_, err = c.exec.Command("multipath", "-f", mpathDevice).CombinedOutput()
		if err != nil {
			klog.Warningf("Failed to flush multipath device map: %s: %v", mpathDevice, err)
			// 继续删除块设备，不因清理失败中断
		}
		klog.V(4).Infof("Flushed multipath device: %s", mpathDevice)
	}
	for _, deviceName := range deviceNames {
		err = deleteDevice(deviceName)
		if err != nil {
			klog.Warningf("Failed to delete block device: %s: %v", deviceName, err)
			// 继续删除其他块设备
		}
	}
	return nil
}

// DetachDisk 从节点卸载并分离文件系统模式的 iSCSI 卷。
func (util *ISCSIUtil) DetachDisk(c iscsiDiskUnmounter, mntPath string) error {
	if pathExists, pathErr := mount.PathExists(mntPath); pathErr != nil {
		return fmt.Errorf("error checking if path exists: %w", pathErr)
	} else if !pathExists {
		klog.Warningf("Unmount skipped because path does not exist: %v", mntPath)
		return nil
	}

	notMnt, err := c.mounter.IsLikelyNotMountPoint(mntPath)
	if err != nil {
		return err
	}
	if !notMnt {
		if err := c.mounter.Unmount(mntPath); err != nil {
			klog.Errorf("iscsi detach disk: failed to unmount: %s\nError: %v", mntPath, err)
			return err
		}
	}

	// 卸载后检查是否还有人在用该设备，以决定是否需要登出目标
	device, _, err := extractDeviceAndPrefix(mntPath)
	if err != nil {
		return err
	}

	var bkpPortal []string
	var volName, iqn, iface, initiatorName string
	found := true

	// 从 JSON 文件读取 iSCSI 磁盘配置
	if err := util.loadISCSI(c.iscsiDisk, mntPath); err == nil {
		bkpPortal, iqn, iface, volName = c.iscsiDisk.Portals, c.iscsiDisk.Iqn, c.iscsiDisk.Iface, c.iscsiDisk.VolName
		initiatorName = c.iscsiDisk.InitiatorName
	} else {
		// 找不到配置文件时回退到旧行为：从设备路径解析 portal 和 iqn 后登出。
		bkpPortal = make([]string, 1)
		bkpPortal[0], iqn, err = extractPortalAndIqn(device)
		if err != nil {
			return err
		}
		// 从挂载路径解析 iface 用于登出；找不到则保持旧行为以便 kubelet 升级。
		// 指定接口上可能没有对应会话，登出失败属正常情况。
		iface, found = extractIface(mntPath)
	}

	// 卸载后删除所有 SCSI 设备和多路径设备
	if err = deleteDevices(c); err != nil {
		klog.Warningf("iscsi detach disk: failed to delete devices\nError: %v", err)
		// 即使删除失败也继续：登出或许能解决问题
	}

	// 判断能否安全登出期间锁定目标
	c.plugin.targetLocks.LockKey(iqn)
	defer c.plugin.targetLocks.UnlockKey(iqn)

	portals := removeDuplicate(bkpPortal)
	if len(portals) == 0 {
		return fmt.Errorf("iscsi detach disk: failed to detach iscsi disk. Couldn't get connected portals from configurations")
	}

	// 卸载后检查是否还有人在用该设备，以决定是否需要登出目标
	if isSessionBusy(c.iscsiDisk.plugin.host, portals[0], iqn) {
		return nil
	}

	err = util.detachISCSIDisk(c.exec, portals, iqn, iface, volName, initiatorName, found)
	if err != nil {
		return fmt.Errorf("failed to finish detachISCSIDisk, err: %v", err)
	}
	return nil
}

// DetachBlockISCSIDisk 移除块设备卷的 loopback 设备并从节点分离卷。
func (util *ISCSIUtil) DetachBlockISCSIDisk(c iscsiDiskUnmapper, mapPath string) error {
	if pathExists, pathErr := mount.PathExists(mapPath); pathErr != nil {
		return fmt.Errorf("error checking if path exists: %w", pathErr)
	} else if !pathExists {
		klog.Warningf("Unmap skipped because path does not exist: %v", mapPath)
		return nil
	}
	// 能走到这里说明设备已无人使用，检查是否需要登出目标。
	// device 形如 192.168.0.10:3260-iqn.2017-05.com.example:test-lun-0
	device, _, err := extractDeviceAndPrefix(mapPath)
	if err != nil {
		return err
	}
	var bkpPortal []string
	var volName, iqn, lun, iface, initiatorName string
	found := true
	// 从 JSON 文件读取 iSCSI 磁盘配置
	if err := util.loadISCSI(c.iscsiDisk, mapPath); err == nil {
		bkpPortal, iqn, lun, iface, volName = c.iscsiDisk.Portals, c.iscsiDisk.Iqn, c.iscsiDisk.Lun, c.iscsiDisk.Iface, c.iscsiDisk.VolName
		initiatorName = c.iscsiDisk.InitiatorName
	} else {
		// 找不到配置文件时回退到旧行为：从设备路径解析 portal 和 iqn 后登出。
		bkpPortal = make([]string, 1)
		bkpPortal[0], iqn, err = extractPortalAndIqn(device)
		if err != nil {
			return err
		}
		arr := strings.Split(device, "-lun-")
		if len(arr) < 2 {
			return fmt.Errorf("failed to retrieve lun from mapPath: %v", mapPath)
		}
		lun = arr[1]
		// 从挂载路径解析 iface 用于登出；找不到则保持旧行为以便 kubelet 升级。
		// 指定接口上可能没有对应会话，登出失败属正常情况。
		iface, found = extractIface(mapPath)
	}
	portals := removeDuplicate(bkpPortal)
	if len(portals) == 0 {
		return fmt.Errorf("iscsi detach disk: failed to detach iscsi disk. Couldn't get connected portals from configurations")
	}

	devicePath := getDevByPath(portals[0], iqn, lun)
	klog.V(5).Infof("iscsi: devicePath: %s", devicePath)
	if _, err = os.Stat(devicePath); err != nil {
		return fmt.Errorf("failed to validate devicePath: %s", devicePath)
	}

	// 判断能否安全登出期间锁定目标
	c.plugin.targetLocks.LockKey(iqn)
	defer c.plugin.targetLocks.UnlockKey(iqn)

	// 卸载后检查是否还有人在用该设备，以决定是否需要登出目标
	if isSessionBusy(c.iscsiDisk.plugin.host, portals[0], iqn) {
		return nil
	}

	// 从 kubelet 节点分离卷
	err = util.detachISCSIDisk(c.exec, portals, iqn, iface, volName, initiatorName, found)
	if err != nil {
		return fmt.Errorf("failed to finish detachISCSIDisk, err: %v", err)
	}
	return nil
}

// detachISCSIDisk 对所有 portal 执行登出并删除 node 记录，最后清理插件自建的 iface。
func (util *ISCSIUtil) detachISCSIDisk(exec utilexec.Interface, portals []string, iqn, iface, volName, initiatorName string, found bool) error {
	for _, portal := range portals {
		logoutArgs := []string{"-m", "node", "-p", portal, "-T", iqn, "--logout"}
		deleteArgs := []string{"-m", "node", "-p", portal, "-T", iqn, "-o", "delete"}
		if found {
			logoutArgs = append(logoutArgs, []string{"-I", iface}...)
			deleteArgs = append(deleteArgs, []string{"-I", iface}...)
		}
		klog.Infof("iscsi: log out target %s iqn %s iface %s", portal, iqn, iface)
		out, err := exec.Command("iscsiadm", logoutArgs...).CombinedOutput()
		err = ignoreExitCodes(err, exit_ISCSI_ERR_NO_OBJS_FOUND, exit_ISCSI_ERR_SESS_NOT_FOUND)
		if err != nil {
			klog.Errorf("iscsi: failed to detach disk Error: %s", string(out))
			return err
		}
		// 删除 node 记录
		klog.Infof("iscsi: delete node record target %s iqn %s", portal, iqn)
		out, err = exec.Command("iscsiadm", deleteArgs...).CombinedOutput()
		err = ignoreExitCodes(err, exit_ISCSI_ERR_NO_OBJS_FOUND, exit_ISCSI_ERR_SESS_NOT_FOUND)
		if err != nil {
			klog.Errorf("iscsi: failed to delete node record Error: %s", string(out))
			return err
		}
	}
	// 所有会话登出后才能删除 iface；非插件创建的 iface 不删
	if initiatorName != "" && found && iface == (portals[0]+":"+volName) {
		deleteArgs := []string{"-m", "iface", "-I", iface, "-o", "delete"}
		out, err := exec.Command("iscsiadm", deleteArgs...).CombinedOutput()
		err = ignoreExitCodes(err, exit_ISCSI_ERR_NO_OBJS_FOUND, exit_ISCSI_ERR_SESS_NOT_FOUND)
		if err != nil {
			klog.Errorf("iscsi: failed to delete iface Error: %s", string(out))
			return err
		}
	}

	return nil
}

// getDevByPath 拼接 /dev/disk/by-path 下对应的设备路径。
func getDevByPath(portal, iqn, lun string) string {
	return "/dev/disk/by-path/ip-" + portal + "-iscsi-" + iqn + "-lun-" + lun
}

// extractTransportname 从 iscsiadm iface 输出中解析传输方式。
func extractTransportname(ifaceOutput string) (iscsiTransport string) {
	rexOutput := ifaceTransportNameRe.FindStringSubmatch(ifaceOutput)
	if rexOutput == nil {
		return ""
	}
	iscsiTransport = rexOutput[1]

	// iface.transport_name 理论上是必填项，但仍兼容未指定的情况
	if iscsiTransport == "<empty>" {
		iscsiTransport = "tcp"
	}
	return iscsiTransport
}

// extractDeviceAndPrefix 从挂载/映射路径中拆出设备名和 "-lun-" 之前的前缀。
func extractDeviceAndPrefix(mntPath string) (string, string, error) {
	ind := strings.LastIndex(mntPath, "/")
	if ind < 0 {
		return "", "", fmt.Errorf("iscsi detach disk: malformatted mnt path: %s", mntPath)
	}
	device := mntPath[(ind + 1):]
	// 从挂载路径中剥离 -lun- 部分
	ind = strings.LastIndex(mntPath, "-lun-")
	if ind < 0 {
		return "", "", fmt.Errorf("iscsi detach disk: malformatted mnt path: %s", mntPath)
	}
	prefix := mntPath[:ind]
	return device, prefix, nil
}

// extractIface 从路径中提取 iface 名称，找不到时返回 false。
func extractIface(mntPath string) (string, bool) {
	reOutput := ifaceRe.FindStringSubmatch(mntPath)
	if len(reOutput) > 1 {
		return reOutput[1], true
	}

	return "", false
}

// extractPortalAndIqn 从设备名中解析 portal 和 IQN（兼容 iqn. 与 eui.）。
func extractPortalAndIqn(device string) (string, string, error) {
	ind1 := strings.Index(device, "-")
	if ind1 < 0 {
		return "", "", fmt.Errorf("iscsi detach disk: no portal in %s", device)
	}
	portal := device[0:ind1]
	ind2 := strings.Index(device, "iqn.")
	if ind2 < 0 {
		ind2 = strings.Index(device, "eui.")
	}
	if ind2 < 0 {
		return "", "", fmt.Errorf("iscsi detach disk: no iqn in %s", device)
	}
	ind := strings.LastIndex(device, "-lun-")
	iqn := device[ind2:ind]
	return portal, iqn, nil
}

// removeDuplicate 原地去除字符串切片中的空串和重复项。
func removeDuplicate(s []string) []string {
	m := map[string]bool{}
	for _, v := range s {
		if v != "" && !m[v] {
			s[len(m)] = v
			m[v] = true
		}
	}
	s = s[:len(m)]
	return s
}

// parseIscsiadmShow 解析 iscsiadm iface show 的输出为参数映射。
func parseIscsiadmShow(output string) (map[string]string, error) {
	params := make(map[string]string)
	slice := strings.Split(output, "\n")
	for _, line := range slice {
		if !strings.HasPrefix(line, "iface.") || strings.Contains(line, "<empty>") {
			continue
		}
		iface := strings.Fields(line)
		if len(iface) != 3 || iface[1] != "=" {
			return nil, fmt.Errorf("error: invalid iface setting: %v", iface)
		}
		// iface 创建后 iscsi_ifacename 不可修改
		if iface[0] == "iface.iscsi_ifacename" {
			continue
		}
		params[iface[0]] = iface[2]
	}
	return params, nil
}

// cloneIface 复制预配置 iface 的参数，创建使用自定义发起端名称的新 iface。
func cloneIface(b iscsiDiskMounter) error {
	var lastErr error
	if b.InitIface == b.Iface {
		return fmt.Errorf("iscsi: cannot clone iface with same name: %s", b.InitIface)
	}
	// 读取预配置 iface 的记录
	out, err := execWithLog(b, "iscsiadm", "-m", "iface", "-I", b.InitIface, "-o", "show")
	if err != nil {
		lastErr = fmt.Errorf("iscsi: failed to show iface records: %s (%v)", out, err)
		return lastErr
	}
	// 解析读取到的记录
	params, err := parseIscsiadmShow(out)
	if err != nil {
		lastErr = fmt.Errorf("iscsi: failed to parse iface records: %s (%v)", out, err)
		return lastErr
	}
	// 替换为自定义的发起端名称
	params["iface.initiatorname"] = b.InitiatorName
	// 创建新 iface
	out, err = execWithLog(b, "iscsiadm", "-m", "iface", "-I", b.Iface, "-o", "new")
	if err != nil {
		exit, ok := err.(utilexec.ExitError)
		if ok && exit.ExitStatus() == iscsiadmErrorSessExists {
			klog.Infof("iscsi: there is a session already logged in with iface %s", b.Iface)
		} else {
			lastErr = fmt.Errorf("iscsi: failed to create new iface: %s (%v)", out, err)
			return lastErr
		}
	}
	// 排序 key 以保证写入顺序稳定
	var keys []string
	for k := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	// 逐项写入新 iface 的参数
	for _, key := range keys {
		_, err = execWithLog(b, "iscsiadm", "-m", "iface", "-I", b.Iface, "-o", "update", "-n", key, "-v", params[key])
		if err != nil {
			execWithLog(b, "iscsiadm", "-m", "iface", "-I", b.Iface, "-o", "delete")
			lastErr = fmt.Errorf("iscsi: failed to update iface records: %s (%v). iface(%s) will be used", out, err, b.InitIface)
			break
		}
	}
	return lastErr
}

// isSessionBusy 通过统计在用的文件系统卷和块设备卷数判断 iSCSI 会话是否繁忙。
func isSessionBusy(host volume.VolumeHost, portal, iqn string) bool {
	fsDir := host.GetPluginDir(iscsiPluginName)
	countFS, err := getVolCount(fsDir, portal, iqn)
	if err != nil {
		klog.Errorf("iscsi: could not determine FS volumes in use: %v", err)
		return true
	}

	blockDir := host.GetVolumeDevicePluginDir(iscsiPluginName)
	countBlock, err := getVolCount(blockDir, portal, iqn)
	if err != nil {
		klog.Errorf("iscsi: could not determine block volumes in use: %v", err)
		return true
	}

	return countFS+countBlock > 1
}

// getVolCount 统计 kubelet 在用的卷数：按给定 portal 和 IQN 前缀匹配目录名。
func getVolCount(dir, portal, iqn string) (int, error) {
	// 文件系统卷的顶层目录按 iface 命名，如 iface-default 或 iface-127.0.0.1:3260:pv0；
	// 块设备卷的顶层目录是 volumeDevices。
	contents, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}

	// 在每个 iface 目录下查找以 portal + iqn 为前缀的卷目录，
	// 例如 127.0.0.1:3260-iqn.2003-01.io.k8s:e2e.volume-1-lun-2
	var counter int
	for _, c := range contents {
		if !c.IsDir() || c.Name() == kubeletconfig.DefaultKubeletVolumeDevicesDirName {
			continue
		}

		mounts, err := os.ReadDir(filepath.Join(dir, c.Name()))
		if err != nil {
			return 0, err
		}

		for _, m := range mounts {
			volumeMount := m.Name()
			prefix := portal + "-" + iqn
			if strings.HasPrefix(volumeMount, prefix) {
				counter++
			}
		}
	}

	return counter, nil
}

// ignoreExitCodes 将指定的 iscsiadm 退出码视为成功，其余错误原样返回。
func ignoreExitCodes(err error, ignoredExitCodes ...int) error {
	exitError, ok := err.(utilexec.ExitError)
	if !ok {
		return err
	}
	for _, code := range ignoredExitCodes {
		if exitError.ExitStatus() == code {
			klog.V(4).Infof("ignored iscsiadm exit code %d", code)
			return nil
		}
	}
	return err
}

// execWithLog 执行命令并在 V(5) 日志中记录耗时与输出（不含密钥）。
func execWithLog(b iscsiDiskMounter, cmd string, args ...string) (string, error) {
	start := time.Now()
	out, err := b.exec.Command(cmd, args...).CombinedOutput()
	if klogV := klog.V(5); klogV.Enabled() {
		d := time.Since(start)
		klogV.Infof("Executed %s %v in %v, err: %v", cmd, args, d, err)
		klogV.Infof("Output: %s", string(out))
	}
	return string(out), err
}
