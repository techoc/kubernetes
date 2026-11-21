//go:build windows

/*
Copyright 2017 The Kubernetes Authors.

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

package winstats

import (
	"fmt"

	"golang.org/x/sys/windows/registry"
)

// OSInfo 保存 Windows 操作系统版本信息。
type OSInfo struct {
	// BuildNumber 是系统构建号，ProductName 是产品名称。
	BuildNumber, ProductName string
	// 主版本、次版本与更新构建修订号（UBR）。
	MajorVersion, MinorVersion, UBR uint64
}

// GetOSInfo 从注册表读取 Windows 版本信息。
func GetOSInfo() (*OSInfo, error) {
	// 用于日志中的注册表路径。
	var keyPrefix string = `regedit:LOCAL_MACHINE\SOFTWARE\Microsoft\Windows NT\CurrentVersion`

	// 从注册表读取 Windows 版本信息。
	k, err := registry.OpenKey(registry.LOCAL_MACHINE, `SOFTWARE\Microsoft\Windows NT\CurrentVersion`, registry.QUERY_VALUE)
	if err != nil {
		return nil, fmt.Errorf("getOSInfo, open Windows %s failed: %w", keyPrefix, err)
	}
	defer k.Close()

	buildNumber, _, err := k.GetStringValue("CurrentBuildNumber")
	if err != nil {
		return nil, fmt.Errorf("getOSInfo, get %s\\CurrentBuildNumber failed: %w", keyPrefix, err)
	}

	majorVersionNumber, _, err := k.GetIntegerValue("CurrentMajorVersionNumber")
	if err != nil {
		return nil, fmt.Errorf("getOSInfo, get %s\\CurrentMajorVersionNumber failed: %w", keyPrefix, err)
	}

	minorVersionNumber, _, err := k.GetIntegerValue("CurrentMinorVersionNumber")
	if err != nil {
		return nil, fmt.Errorf("getOSInfo, get %s\\CurrentMinorVersionNumber failed: %w", keyPrefix, err)
	}

	revision, _, err := k.GetIntegerValue("UBR")
	if err != nil {
		return nil, fmt.Errorf("getOSInfo, get %s\\UBR failed: %w", keyPrefix, err)
	}

	productName, _, err := k.GetStringValue("ProductName")
	if err != nil {
		return nil, fmt.Errorf("getOSInfo, get %s\\ProductName failed: %w", keyPrefix, err)
	}

	return &OSInfo{
		BuildNumber:  buildNumber,
		ProductName:  productName,
		MajorVersion: majorVersionNumber,
		MinorVersion: minorVersionNumber,
		UBR:          revision,
	}, nil
}

// GetPatchVersion 返回包含补丁号的完整 OS 版本。
func (o *OSInfo) GetPatchVersion() string {
	return fmt.Sprintf("%d.%d.%s.%d", o.MajorVersion, o.MinorVersion, o.BuildNumber, o.UBR)
}

// GetBuild 返回到构建号为止的 OS 版本。
func (o *OSInfo) GetBuild() string {
	return fmt.Sprintf("%d.%d.%s", o.MajorVersion, o.MinorVersion, o.BuildNumber)
}
