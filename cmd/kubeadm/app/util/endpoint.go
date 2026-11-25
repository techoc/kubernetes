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

package util

import (
	"net"
	"net/url"
	"strconv"

	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/klog/v2"
	netutils "k8s.io/utils/net"

	kubeadmapi "k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
)

// GetControlPlaneEndpoint 按以下规则生成格式正确的控制平面端点：
// - 已定义 controlPlaneEndpoint 时直接使用。
// - 已定义但未指定端口时，拼接 controlPlaneEndpoint 与 localEndpoint.BindPort。
// - 未定义时，使用 localEndpoint.AdvertiseAddress 与 localEndpoint.BindPort。
func GetControlPlaneEndpoint(controlPlaneEndpoint string, localEndpoint *kubeadmapi.APIEndpoint) (string, error) {
	// 获取本地端点的 URL。
	localAPIEndpoint, err := GetLocalAPIEndpoint(localEndpoint)
	if err != nil {
		return "", err
	}

	// 若控制平面端点已定义。
	if len(controlPlaneEndpoint) > 0 {
		// 解析控制平面端点。
		var host, port string
		var err error
		if host, port, err = ParseHostPort(controlPlaneEndpoint); err != nil {
			return "", errors.Wrapf(err, "invalid value %q given for controlPlaneEndpoint", controlPlaneEndpoint)
		}

		// controlPlaneAddress 含端口时告知用户将使用该端口，否则使用 bindPort。
		localEndpointPort := strconv.Itoa(int(localEndpoint.BindPort))
		if port != "" {
			if port != localEndpointPort {
				klog.Warning("[endpoint] WARNING: port specified in controlPlaneEndpoint overrides bindPort in the controlplane address")
			}
		} else {
			port = localEndpointPort
		}

		// 使用 controlPlaneAddress（可能含 bindPort）覆盖控制平面 URL。
		return formatURL(host, port).String(), nil
	}

	return localAPIEndpoint, nil
}

// GetLocalAPIEndpoint 将 APIEndpoint 解析为字符串；解析失败时返回错误。
func GetLocalAPIEndpoint(localEndpoint *kubeadmapi.APIEndpoint) (string, error) {
	// 获取本地端点的 URL。
	localEndpointIP, localEndpointPort, err := parseAPIEndpoint(localEndpoint)
	if err != nil {
		return "", err
	}
	url := formatURL(localEndpointIP.String(), localEndpointPort)
	return url.String(), nil
}

// ParseHostPort 将 "host:port"、"ipv4:port"、"[ipv6]:port" 形式的网络地址解析为主机和端口；
// 可省略 ":port"。地址无效时返回错误。
func ParseHostPort(hostport string) (string, string, error) {
	var host, port string
	var err error

	// 尝试拆分主机和端口。
	if host, port, err = net.SplitHostPort(hostport); err != nil {
		// SplitHostPort 返回错误时，将整个 hostport 视为主机。
		host = hostport
	}

	// 若定义了端口，解析并校验。
	if port != "" {
		if _, err := ParsePort(port); err != nil {
			return "", "", errors.Errorf("hostport %s: port %s must be a valid number between 1 and 65535, inclusive", hostport, port)
		}
	}

	// 主机为有效 IP 时返回它。
	if ip := netutils.ParseIPSloppy(host); ip != nil {
		return host, port, nil
	}

	// 主机为有效的 RFC-1123 子域时返回它。
	if errs := validation.IsDNS1123Subdomain(host); len(errs) == 0 {
		return host, port, nil
	}

	return "", "", errors.Errorf("hostport %s: host '%s' must be a valid IP address or a valid RFC-1123 DNS subdomain", hostport, host)
}

// ParsePort 解析表示 TCP 端口的字符串；无效时返回错误。
func ParsePort(port string) (int, error) {
	portInt, err := netutils.ParsePort(port, true)
	if err == nil && (1 <= portInt && portInt <= 65535) {
		return portInt, nil
	}

	return 0, errors.New("port must be a valid number between 1 and 65535, inclusive")
}

// parseAPIEndpoint 将 APIEndpoint 的 AdvertiseAddress 解析为 net.IP，BindPort 解析为字符串；
// 任一字段无效时返回错误。
func parseAPIEndpoint(localEndpoint *kubeadmapi.APIEndpoint) (net.IP, string, error) {
	// 解析绑定端口。
	bindPortString := strconv.Itoa(int(localEndpoint.BindPort))
	if _, err := ParsePort(bindPortString); err != nil {
		return nil, "", errors.Wrapf(err, "invalid value %q given for api.bindPort", localEndpoint.BindPort)
	}

	// 解析 AdvertiseAddress。
	var ip = netutils.ParseIPSloppy(localEndpoint.AdvertiseAddress)
	if ip == nil {
		return nil, "", errors.Errorf("invalid value `%s` given for api.advertiseAddress", localEndpoint.AdvertiseAddress)
	}

	return ip, bindPortString, nil
}

// formatURL 使用 HTTPS scheme 根据主机和端口字符串创建 net.URL。
func formatURL(host, port string) *url.URL {
	return &url.URL{
		Scheme: "https",
		Host:   net.JoinHostPort(host, port),
	}
}
