//go:build !windows

/*
Copyright 2019 The Kubernetes Authors.

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

package dns

import (
	"strings"
)

const etcHostsFile = "/etc/hosts"

// 由 kubelet 管理的 /etc/resolv.conf 文件内容如下：
// nameserver DNS_CLUSTER_IP
// search test-dns.svc.cluster.local svc.cluster.local cluster.local q53aahaikqaehcai3ylfqdtc5b.bx.internal.cloudapp.net
// options ndots:5

// GetDNSSuffixList 读取 DNS 配置文件，返回已配置的 DNS 后缀列表
func GetDNSSuffixList() []string {
	fileData := readFile("/etc/resolv.conf")
	lines := strings.Split(fileData, "\n")
	for _, line := range lines {
		if strings.HasPrefix(line, "search") {
			// 去掉开头的 "search"。
			return strings.Split(line, " ")[1:]
		}
	}

	panic("Could not find DNS search list!")
}

func getDNSServerList() []string {
	fileData := readFile("/etc/resolv.conf")
	lines := strings.Split(fileData, "\n")
	for _, line := range lines {
		if strings.HasPrefix(line, "nameserver") {
			// 去掉开头的 "nameserver"。
			return strings.Split(line, " ")[1:]
		}
	}

	panic("Could not find DNS search list!")
}
