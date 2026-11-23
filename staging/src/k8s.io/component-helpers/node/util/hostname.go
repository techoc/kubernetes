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

package util

import (
	"fmt"
	"os"
	"strings"
)

// GetHostname 在 hostnameOverride 为空时返回操作系统的主机名，否则返回 hostnameOverride。
// 无论哪种来源，结果都会被规范化：去掉首尾空白并转为小写。
//
// 节点名既是 API 对象的名字，也是各组件之间对齐同一节点身份的键。同一个节点若在不同来源下
// 得到大小写不同或带空白的名字，就会被当成两个对象，因此对两种来源统一做规范化。
// 覆盖值优先于系统主机名，是为了让容器化等场景下调用方能显式指定身份，
// 而不必依赖运行环境恰好提供了正确的 hostname。
func GetHostname(hostnameOverride string) (string, error) {
	hostName := hostnameOverride
	// 调用方显式指定优先；留空才回退到系统主机名。
	if len(hostName) == 0 {
		nodeName, err := os.Hostname()
		if err != nil {
			return "", fmt.Errorf("couldn't determine hostname: %w", err)
		}
		hostName = nodeName
	}

	// 先去掉首尾空白，避免出现“看不见内容却合法”的主机名；在 Linux 上系统主机名
	// 直接读自 /proc/sys/kernel/hostname。
	//
	// 先去掉空白再判空：系统文件常带换行，若直接转小写，得到的将是一个看不见内容却合法的字符串。
	// 空白也一并算作无效输入，免得把这种错误推迟到后续注册节点时才暴露。
	hostName = strings.TrimSpace(hostName)
	if len(hostName) == 0 {
		return "", fmt.Errorf("empty hostname is invalid")
	}

	// 统一转小写，保证不同来源得到的节点名一致。
	return strings.ToLower(hostName), nil
}
