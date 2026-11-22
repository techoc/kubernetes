//go:build linux

/*
Copyright 2016 The Kubernetes Authors.

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

package util

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"k8s.io/klog/v2"
)

// FindMultipathDeviceForDevice 根据 /dev/sdx 这样的设备名查找其 devicemapper 父设备；
// 传入已是 dm 设备的名称时原样返回。
func (handler *deviceHandler) FindMultipathDeviceForDevice(device string) string {
	if strings.HasPrefix(device, "/dev/dm-") {
		return device
	}

	io := handler.getIo
	disk, err := findDeviceForPath(device, io)
	if err != nil {
		return ""
	}
	sysPath := "/sys/block/"
	if dirs, err := io.ReadDir(sysPath); err == nil {
		for _, f := range dirs {
			name := f.Name()
			if strings.HasPrefix(name, "dm-") {
				if _, err1 := io.Lstat(sysPath + name + "/slaves/" + disk); err1 == nil {
					return "/dev/" + name
				}
			}
		}
	}
	return ""
}

// findDeviceForPath 解析符号链接，返回 /dev/disk/by-path/XXXX 等路径底层的
// 磁盘名（如 sdX、hdX）；传入 /dev/sdX 时返回 sdX。
func findDeviceForPath(path string, io IoUtil) (string, error) {
	devicePath, err := io.EvalSymlinks(path)
	if err != nil {
		return "", err
	}
	// /dev/hdX 按 "/" 切分后取最后一段。
	parts := strings.Split(devicePath, "/")
	if len(parts) == 3 && strings.HasPrefix(parts[1], "dev") {
		return parts[2], nil
	}
	return "", errors.New("Illegal path for device " + devicePath)
}

// FindSlaveDevicesOnMultipath 根据 /dev/dm-1 这样的名称查找该多路径设备
// 管理的所有从属设备。
func (handler *deviceHandler) FindSlaveDevicesOnMultipath(dm string) []string {
	var devices []string
	io := handler.getIo
	// 将 /dev/dm-1 切分为 "", "dev", "dm-1"。
	parts := strings.Split(dm, "/")
	if len(parts) != 3 || !strings.HasPrefix(parts[1], "dev") {
		return devices
	}
	disk := parts[2]
	slavesPath := filepath.Join("/sys/block/", disk, "/slaves/")
	if files, err := io.ReadDir(slavesPath); err == nil {
		for _, f := range files {
			devices = append(devices, filepath.Join("/dev/", f.Name()))
		}
	}
	return devices
}

// GetISCSIPortalHostMapForTarget 根据 target IQN 查找已登录该 target 的所有
// SCSI 主机，返回 iSCSI portal 到 SCSI 主机号的映射。
//
//	例如：{
//	   "192.168.30.7:3260": 2,
//	   "192.168.30.8:3260": 3,
//	}
func (handler *deviceHandler) GetISCSIPortalHostMapForTarget(targetIqn string) (map[string]int, error) {
	portalHostMap := make(map[string]int)
	io := handler.getIo

	// 遍历 sysfs 中所有 iSCSI 主机。
	sysPath := "/sys/class/iscsi_host"
	hostDirs, err := io.ReadDir(sysPath)
	if err != nil {
		if os.IsNotExist(err) {
			return portalHostMap, nil
		}
		return nil, err
	}
	for _, hostDir := range hostDirs {
		// iSCSI 主机名格式固定为 "host%d"，参见 Linux drivers/scsi/hosts.c。
		hostName := hostDir.Name()
		if !strings.HasPrefix(hostName, "host") {
			continue
		}
		hostNumber, err := strconv.Atoi(strings.TrimPrefix(hostName, "host"))
		if err != nil {
			klog.Errorf("Could not get number from iSCSI host: %s", hostName)
			continue
		}

		// 遍历 iscsi_host 设备的子项，寻找关联的会话。
		devicePath := sysPath + "/" + hostName + "/device"
		deviceDirs, err := io.ReadDir(devicePath)
		if err != nil {
			return nil, err
		}
		for _, deviceDir := range deviceDirs {
			// 跳过非会话项；会话名格式为 "session%u"，参见
			// Linux drivers/scsi/scsi_transport_iscsi.c。
			sessionName := deviceDir.Name()
			if !strings.HasPrefix(sessionName, "session") {
				continue
			}

			sessionPath := devicePath + "/" + sessionName

			// 读取 iSCSI 会话对应的 target 名称。
			targetNamePath := sessionPath + "/iscsi_session/" + sessionName + "/targetname"
			targetName, err := io.ReadFile(targetNamePath)
			if err != nil {
				klog.Infof("Failed to process session %s, assuming this session is unavailable: %s", sessionName, err)
				continue
			}

			// 跳过 target 名称不匹配的主机。
			if strings.TrimSpace(string(targetName)) != targetIqn {
				continue
			}

			// 遍历会话子项寻找 iSCSI 连接。
			dirs2, err := io.ReadDir(sessionPath)
			if err != nil {
				klog.Infof("Failed to process session %s, assuming this session is unavailable: %s", sessionName, err)
				continue
			}
			for _, dir2 := range dirs2 {
				// 跳过非连接项；连接名格式为 "connection%d:%u"，
				// 参见 Linux drivers/scsi/scsi_transport_iscsi.c。
				dirName := dir2.Name()
				if !strings.HasPrefix(dirName, "connection") {
					continue
				}

				connectionPath := sessionPath + "/" + dirName + "/iscsi_connection/" + dirName

				// 读取连接的当前和持久 portal 信息。
				addrPath := connectionPath + "/address"
				addr, err := io.ReadFile(addrPath)
				if err != nil {
					klog.Infof("Failed to process connection %s, assuming this connection is unavailable: %s", dirName, err)
					continue
				}

				portPath := connectionPath + "/port"
				port, err := io.ReadFile(portPath)
				if err != nil {
					klog.Infof("Failed to process connection %s, assuming this connection is unavailable: %s", dirName, err)
					continue
				}

				persistentAddrPath := connectionPath + "/persistent_address"
				persistentAddr, err := io.ReadFile(persistentAddrPath)
				if err != nil {
					klog.Infof("Failed to process connection %s, assuming this connection is unavailable: %s", dirName, err)
					continue
				}

				persistentPortPath := connectionPath + "/persistent_port"
				persistentPort, err := io.ReadFile(persistentPortPath)
				if err != nil {
					klog.Infof("Failed to process connection %s, assuming this connection is unavailable: %s", dirName, err)
					continue
				}

				// 当前和持久 portal 都映射到对应 SCSI 主机；
				// JoinHostPort 会为 IPv6 地址加上方括号。
				portal := net.JoinHostPort(strings.TrimSpace(string(addr)), strings.TrimSpace(string(port)))
				portalHostMap[portal] = hostNumber

				persistentPortal := net.JoinHostPort(strings.TrimSpace(string(persistentAddr)), strings.TrimSpace(string(persistentPort)))
				portalHostMap[persistentPortal] = hostNumber
			}
		}
	}

	return portalHostMap, nil
}

// FindDevicesForISCSILun 根据 target IQN 和 LUN 号查找对应的所有块设备。
func (handler *deviceHandler) FindDevicesForISCSILun(targetIqn string, lun int) ([]string, error) {
	devices := make([]string, 0)
	io := handler.getIo

	// 遍历 sysfs 中所有 iSCSI 主机。
	sysPath := "/sys/class/iscsi_host"
	hostDirs, err := io.ReadDir(sysPath)
	if err != nil {
		return nil, err
	}
	for _, hostDir := range hostDirs {
		// iSCSI 主机名格式固定为 "host%d"，参见 Linux drivers/scsi/hosts.c。
		hostName := hostDir.Name()
		if !strings.HasPrefix(hostName, "host") {
			continue
		}
		hostNumber, err := strconv.Atoi(strings.TrimPrefix(hostName, "host"))
		if err != nil {
			klog.Errorf("Could not get number from iSCSI host: %s", hostName)
			continue
		}

		// 遍历 iscsi_host 设备的子项，寻找关联的会话。
		devicePath := sysPath + "/" + hostName + "/device"
		deviceDirs, err := io.ReadDir(devicePath)
		if err != nil {
			return nil, err
		}
		for _, deviceDir := range deviceDirs {
			// 跳过非会话项；会话名格式为 "session%u"，参见
			// Linux drivers/scsi/scsi_transport_iscsi.c。
			sessionName := deviceDir.Name()
			if !strings.HasPrefix(sessionName, "session") {
				continue
			}

			// 读取 iSCSI 会话对应的 target 名称。
			targetNamePath := devicePath + "/" + sessionName + "/iscsi_session/" + sessionName + "/targetname"
			targetName, err := io.ReadFile(targetNamePath)
			if err != nil {
				return nil, err
			}

			// 仅处理 target 名称匹配的会话。
			if strings.TrimSpace(string(targetName)) != targetIqn {
				continue
			}

			// SCSI 总线上的块设备位于 "target%d:%d:%d" 目录，参见
			// Linux drivers/scsi/scsi_scan.c；iSCSI 中 channel 和 device 恒为 0。
			targetPath := devicePath + "/" + sessionName + fmt.Sprintf("/target%d:0:0", hostNumber)

			// LUN 对应目录格式为 host:channel:bus:LUN。
			blockDevicePath := targetPath + fmt.Sprintf("/%d:0:0:%d", hostNumber, lun)

			// LUN 在该总线上不存在则跳过。
			_, err = io.Lstat(blockDevicePath)
			if err != nil {
				continue
			}

			// block 目录下应只有一个子项，即 "sd*" 块设备。
			path := blockDevicePath + "/block"
			dirs, err := io.ReadDir(path)
			if err != nil {
				return nil, err
			}
			if 0 < len(dirs) {
				devices = append(devices, dirs[0].Name())
			}
		}
	}

	return devices, nil
}
