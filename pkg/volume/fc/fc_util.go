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

package fc

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"k8s.io/klog/v2"
	"k8s.io/mount-utils"
	utilexec "k8s.io/utils/exec"

	"k8s.io/kubernetes/pkg/volume"
	volumeutil "k8s.io/kubernetes/pkg/volume/util"
)

// ioHandler 封装文件系统操作，便于测试中替换真实 I/O。
type ioHandler interface {
	ReadDir(dirname string) ([]os.FileInfo, error)
	Lstat(name string) (os.FileInfo, error)
	EvalSymlinks(path string) (string, error)
	WriteFile(filename string, data []byte, perm os.FileMode) error
}

// osIOHandler 是基于真实操作系统调用实现的 ioHandler。
type osIOHandler struct{}

const (
	byPath = "/dev/disk/by-path/"
	byID   = "/dev/disk/by-id/"
)

// ReadDir 读取目录下的所有条目并返回其文件信息。
func (handler *osIOHandler) ReadDir(dirname string) ([]os.FileInfo, error) {
	entries, err := os.ReadDir(dirname)
	if err != nil {
		return nil, err
	}
	infos := make([]os.FileInfo, 0, len(entries))
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			return nil, err
		}
		infos = append(infos, info)
	}
	return infos, nil
}

// Lstat 获取文件或符号链接本身的信息。
func (handler *osIOHandler) Lstat(name string) (os.FileInfo, error) {
	return os.Lstat(name)
}

// EvalSymlinks 解析路径中的所有符号链接。
func (handler *osIOHandler) EvalSymlinks(path string) (string, error) {
	return filepath.EvalSymlinks(path)
}

// WriteFile 以指定权限向文件写入数据。
func (handler *osIOHandler) WriteFile(filename string, data []byte, perm os.FileMode) error {
	return os.WriteFile(filename, data, perm)
}

// findDisk 根据给定的 wwn 和 lun 查找设备及其对应的 device-mapper 父设备。
func findDisk(wwn, lun string, io ioHandler, deviceUtil volumeutil.DeviceUtil) (string, string) {
	fcPathExp := "^(pci-.*-fc|fc)-0x" + wwn + "-lun-" + lun + "$"
	r := regexp.MustCompile(fcPathExp)
	devPath := byPath
	if dirs, err := io.ReadDir(devPath); err == nil {
		for _, f := range dirs {
			name := f.Name()
			if r.MatchString(name) {
				if disk, err1 := io.EvalSymlinks(devPath + name); err1 == nil {
					dm := deviceUtil.FindMultipathDeviceForDevice(disk)
					klog.Infof("fc: find disk: %v, dm: %v, fc path: %v", disk, dm, name)
					return disk, dm
				}
			}
		}
	}
	return "", ""
}

// findDiskWWIDs 根据给定的 wwid 查找设备及其对应的 device-mapper 父设备。
func findDiskWWIDs(wwid string, io ioHandler, deviceUtil volumeutil.DeviceUtil) (string, string) {
	// wwid 格式示例：
	//   3600508b400105e210000900000490000
	//   <VENDOR NAME> <IDENTIFIER NUMBER>
	// by-id 下的符号链接示例：
	//   /dev/by-id/scsi-3600508b400105e210000900000490000
	//   /dev/by-id/scsi-<VENDOR NAME>_<IDENTIFIER NUMBER>
	// wwid 中可能含空格，暴露到 /dev/by-id 时空格会被替换为下划线。

	fcPath := "scsi-" + wwid
	devID := byID
	if dirs, err := io.ReadDir(devID); err == nil {
		for _, f := range dirs {
			name := f.Name()
			if name == fcPath {
				disk, err := io.EvalSymlinks(devID + name)
				if err != nil {
					klog.V(2).Infof("fc: failed to find a corresponding disk from symlink[%s], error %v", devID+name, err)
					return "", ""
				}
				dm := deviceUtil.FindMultipathDeviceForDevice(disk)
				klog.Infof("fc: find disk: %v, dm: %v", disk, dm)
				return disk, dm
			}
		}
	}
	klog.V(2).Infof("fc: failed to find a disk [%s]", devID+fcPath)
	return "", ""
}

// flushDevice 将设备上未完成的 I/O 刷盘。
func flushDevice(deviceName string, exec utilexec.Interface) {
	out, err := exec.Command("blockdev", "--flushbufs", deviceName).CombinedOutput()
	if err != nil {
		// 忽略刷盘失败并继续删除设备：出错后不会再重试。
		klog.Warningf("Failed to flush device %s: %s\n%s", deviceName, err, string(out))
	}
	klog.V(4).Infof("Flushed device %s", deviceName)
}

// removeFromScsiSubsystem 通过 /dev/sdX 名称从 SCSI 子系统中移除设备。
func removeFromScsiSubsystem(deviceName string, io ioHandler) {
	fileName := "/sys/block/" + deviceName + "/device/delete"
	klog.V(4).Infof("fc: remove device from scsi-subsystem: path: %s", fileName)
	data := []byte("1")
	io.WriteFile(fileName, data, 0666)
}

// scsiHostRescan 重新扫描所有 SCSI 总线。
func scsiHostRescan(io ioHandler) {
	scsiPath := "/sys/class/scsi_host/"
	if dirs, err := io.ReadDir(scsiPath); err == nil {
		for _, f := range dirs {
			name := scsiPath + f.Name() + "/scan"
			data := []byte("- - -")
			io.WriteFile(name, data, 0666)
		}
	}
}

// makePDNameInternal 生成类似 /var/lib/kubelet/plugins/kubernetes.io/fc/target1-target2-lun-0 的目录名。
func makePDNameInternal(host volume.VolumeHost, wwns []string, lun string, wwids []string) string {
	if len(wwns) != 0 {
		w := strings.Join(wwns, "-")
		return filepath.Join(host.GetPluginDir(fcPluginName), w+"-lun-"+lun)
	}
	return filepath.Join(host.GetPluginDir(fcPluginName), strings.Join(wwids, "-"))
}

// makeVDPDNameInternal 生成类似 /var/lib/kubelet/plugins/kubernetes.io/fc/volumeDevices/target-lun-0 的目录名。
func makeVDPDNameInternal(host volume.VolumeHost, wwns []string, lun string, wwids []string) string {
	if len(wwns) != 0 {
		w := strings.Join(wwns, "-")
		return filepath.Join(host.GetVolumeDevicePluginDir(fcPluginName), w+"-lun-"+lun)
	}
	return filepath.Join(host.GetVolumeDevicePluginDir(fcPluginName), strings.Join(wwids, "-"))
}

// parsePDName 解析由 makePDNameInternal 或 makeVDPDNameInternal 生成的目录名。
func parsePDName(path string) (wwns []string, lun int32, wwids []string, err error) {
	dirname := filepath.Base(path)
	components := strings.Split(dirname, "-")
	l := len(components)
	if l == 1 {
		// 没有连字符，说明是单个 WWID
		return nil, 0, components, nil
	}
	if components[l-2] == "lun" {
		// 含 -lun-，说明是 WWN 列表加末尾的 lun 号
		if l == 2 {
			return nil, 0, nil, fmt.Errorf("no wwn in: %s", dirname)
		}
		lun, err := strconv.Atoi(components[l-1])
		if err != nil {
			return nil, 0, nil, err
		}

		return components[:l-2], int32(lun), nil, nil
	}
	// 没有 -lun-，说明是 WWID 列表
	return nil, 0, components, nil
}

// fcUtil 实现 FC 卷的设备查找、挂载与卸载操作。
type fcUtil struct{}

// MakeGlobalPDName 返回文件卷的全局插件目录名。
func (util *fcUtil) MakeGlobalPDName(fc fcDisk) string {
	return makePDNameInternal(fc.plugin.host, fc.wwns, fc.lun, fc.wwids)
}

// MakeGlobalVDPDName 返回块设备卷的全局插件目录名。
func (util *fcUtil) MakeGlobalVDPDName(fc fcDisk) string {
	return makeVDPDNameInternal(fc.plugin.host, fc.wwns, fc.lun, fc.wwids)
}

// searchDisk 查找 FC 卷对应的设备，优先使用多路径设备。
func searchDisk(b fcDiskMounter) (string, error) {
	var diskIDs []string
	var disk string
	var dm string
	io := b.io
	wwids := b.wwids
	wwns := b.wwns
	lun := b.lun

	if len(wwns) != 0 {
		diskIDs = wwns
	} else {
		diskIDs = wwids
	}

	rescanned := false
	// 两阶段查找：
	// 第一阶段在现有设备路径中查找，找到多路径 dm 设备就退出循环；
	// 否则第二阶段重新扫描 SCSI 总线后再查一次，找到什么就返回什么。
	for true {
		for _, diskID := range diskIDs {
			if len(wwns) != 0 {
				disk, dm = findDisk(diskID, lun, io, b.deviceUtil)
			} else {
				disk, dm = findDiskWWIDs(diskID, io, b.deviceUtil)
			}
			// 找到多路径设备即可跳出
			if dm != "" {
				break
			}
		}
		// 找到 dm 设备则结束循环
		if rescanned || dm != "" {
			break
		}
		// 重新扫描 SCSI 总线后再查一遍
		scsiHostRescan(io)
		rescanned = true
	}
	// 没有匹配 wwn 和 lun 的磁盘则报错退出
	if disk == "" && dm == "" {
		return "", fmt.Errorf("no fc disk found")
	}

	// 找到多路径 dm 设备就用它，否则使用原始磁盘
	if dm != "" {
		return dm, nil
	}
	return disk, nil
}

// AttachDisk 查找并确认 FC 卷设备存在。
func (util *fcUtil) AttachDisk(b fcDiskMounter) (string, error) {
	devicePath, err := searchDisk(b)
	if err != nil {
		return "", err
	}

	exists, err := mount.PathExists(devicePath)
	if exists && err == nil {
		return devicePath, nil
	}
	if exists == false {
		return "", fmt.Errorf("device %s does not exist", devicePath)
	} else {
		return "", err
	}
}

// DetachDisk 从节点上移除 /dev/sdX 之类的 SCSI 设备文件。
func (util *fcUtil) DetachDisk(c fcDiskUnmounter, devicePath string) error {
	var devices []string
	// devicePath 可能是 /dev/mapper/mpathX，先解析出真实设备。
	dstPath, err := c.io.EvalSymlinks(devicePath)
	if err != nil {
		return err
	}
	// 对于多路径设备，先找出其下的从设备
	if strings.HasPrefix(dstPath, "/dev/dm-") {
		devices = c.deviceUtil.FindSlaveDevicesOnMultipath(dstPath)
		if err := util.deleteMultipathDevice(c.exec, dstPath); err != nil {
			return err
		}
	} else {
		// 单设备直接加入待处理列表
		devices = append(devices, dstPath)
	}
	klog.V(4).Infof("fc: DetachDisk devicePath: %v, dstPath: %v, devices: %v", devicePath, dstPath, devices)
	var lastErr error
	for _, device := range devices {
		err := util.detachFCDisk(c.io, c.exec, device)
		if err != nil {
			klog.Errorf("fc: detachFCDisk failed. device: %v err: %v", device, err)
			lastErr = fmt.Errorf("fc: detachFCDisk failed. device: %v err: %v", device, err)
		}
	}
	if lastErr != nil {
		klog.Errorf("fc: last error occurred during detach disk:\n%v", lastErr)
		return lastErr
	}
	return nil
}

// detachFCDisk 从节点上移除 /dev/sdX 之类的 SCSI 设备文件。
func (util *fcUtil) detachFCDisk(io ioHandler, exec utilexec.Interface, devicePath string) error {
	// 从节点上移除 SCSI 设备。
	if !strings.HasPrefix(devicePath, "/dev/") {
		return fmt.Errorf("fc detach disk: invalid device name: %s", devicePath)
	}
	flushDevice(devicePath, exec)
	arr := strings.Split(devicePath, "/")
	dev := arr[len(arr)-1]
	removeFromScsiSubsystem(dev, io)
	return nil
}

// DetachBlockFCDisk 从 kubelet 节点卸载卷，移除 /dev/sdX 之类的 SCSI 设备文件，
// 并删除该 SCSI 设备的 loopback 映射。
func (util *fcUtil) DetachBlockFCDisk(c fcDiskUnmapper, mapPath, devicePath string) error {
	// 校验 devicePath 是否有效
	if len(devicePath) != 0 {
		if pathExists, pathErr := checkPathExists(devicePath); !pathExists || pathErr != nil {
			return pathErr
		}
	} else {
		// TODO: FC 插件无法从 kubelet 获取 devicePath，因为卷附加到 kubelet 节点时
		// volume 对象中的 devicePath 并未更新。
		klog.Infof("fc: devicePath is empty. Try to retrieve FC configuration from global map path: %v", mapPath)
	}

	// 校验全局映射路径是否有效，示例：
	//   wwn+lun: plugins/kubernetes.io/fc/volumeDevices/50060e801049cfd1-lun-0/
	//   wwid: plugins/kubernetes.io/fc/volumeDevices/3600508b400105e210000900000490000/
	if pathExists, pathErr := checkPathExists(mapPath); !pathExists || pathErr != nil {
		return pathErr
	}

	// 从全局映射路径中提取类似 '50060e801049cfd1-lun-0' 的插件相关路径。
	arr := strings.Split(mapPath, "/")
	if len(arr) < 1 {
		return fmt.Errorf("failed to retrieve volume plugin information from global map path: %v", mapPath)
	}
	volumeInfo := arr[len(arr)-1]

	// 在 /dev/disk/by-path 或 /dev/disk/by-id 下查找与 volumeInfo 匹配的符号链接，
	// 再从其指向获取真实设备路径。
	searchPath := byID
	if strings.Contains(volumeInfo, "-lun-") {
		searchPath = byPath
	}
	fis, err := os.ReadDir(searchPath)
	if err != nil {
		return err
	}
	for _, fi := range fis {
		if strings.Contains(fi.Name(), volumeInfo) {
			devicePath = filepath.Join(searchPath, fi.Name())
			klog.V(5).Infof("fc: updated devicePath: %s", devicePath)
			break
		}
	}
	if len(devicePath) == 0 {
		return fmt.Errorf("fc: failed to find corresponding device from searchPath: %v", searchPath)
	}
	dstPath, err := c.io.EvalSymlinks(devicePath)
	if err != nil {
		return err
	}
	klog.V(4).Infof("fc: find destination device path from symlink: %v", dstPath)

	var devices []string
	dm := c.deviceUtil.FindMultipathDeviceForDevice(dstPath)
	if len(dm) != 0 {
		dstPath = dm
	}

	// 从 kubelet 节点卸载卷
	if len(dm) != 0 {
		// 找出多路径设备管理的所有从设备
		devices = c.deviceUtil.FindSlaveDevicesOnMultipath(dm)
		if err := util.deleteMultipathDevice(c.exec, dm); err != nil {
			return err
		}
	} else {
		// 单设备直接加入待处理列表
		devices = append(devices, dstPath)
	}
	var lastErr error
	for _, device := range devices {
		err = util.detachFCDisk(c.io, c.exec, device)
		if err != nil {
			klog.Errorf("fc: detachFCDisk failed. device: %v err: %v", device, err)
			lastErr = fmt.Errorf("fc: detachFCDisk failed. device: %v err: %v", device, err)
		}
	}
	if lastErr != nil {
		klog.Errorf("fc: last error occurred during detach disk:\n%v", lastErr)
		return lastErr
	}
	return nil
}

// deleteMultipathDevice 强制删除指定的多路径设备。
func (util *fcUtil) deleteMultipathDevice(exec utilexec.Interface, dmDevice string) error {
	out, err := exec.Command("multipath", "-f", dmDevice).CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to flush multipath device %s: %s\n%s", dmDevice, err, string(out))
	}
	klog.V(4).Infof("Flushed multipath device: %s", dmDevice)
	return nil
}

// checkPathExists 检查路径是否存在；路径不存在不算错误，仅跳过后续操作。
func checkPathExists(path string) (bool, error) {
	if pathExists, pathErr := mount.PathExists(path); pathErr != nil {
		return pathExists, fmt.Errorf("error checking if path exists: %w", pathErr)
	} else if !pathExists {
		klog.Warningf("Unmap skipped because path does not exist: %v", path)
		return pathExists, nil
	}
	return true, nil
}
