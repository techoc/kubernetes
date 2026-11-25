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
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"

	netutil "k8s.io/apimachinery/pkg/util/net"
	versionutil "k8s.io/apimachinery/pkg/util/version"
	pkgversion "k8s.io/component-base/version"
	"k8s.io/klog/v2"

	"k8s.io/kubernetes/cmd/kubeadm/app/constants"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
)

const (
	getReleaseVersionTimeout = 10 * time.Second
)

var (
	kubeReleaseBucketURL  = "https://dl.k8s.io"
	kubeCIBucketURL       = "https://storage.googleapis.com/k8s-release-dev"
	kubeReleaseRegex      = regexp.MustCompile(`^v?(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)([-\w.+]*)?$`)
	kubeReleaseLabelRegex = regexp.MustCompile(`^((latest|stable)+(-[1-9](\.[1-9](\d)?)?)?)\z`)
	kubeBucketPrefixes    = regexp.MustCompile(`^((release|ci)/)?([-\w.+]+)$`)
)

// 单元测试中 KubernetesReleaseVersion 等于 kubernetesReleaseVersionTest，返回静态占位版本；
// 非单元测试时等于 kubernetesReleaseVersionDefault。
var KubernetesReleaseVersion = kubernetesReleaseVersionDefault

// kubernetesReleaseVersionDefault 根据 "stable" 或 "latest" 等标签名，从发布服务器获取版本信息。
// 参数已是语义化版本时原样返回；为标签时从发布服务器获取实际语义化版本。
// 发布服务器支持的名称：
//
//	stable      （最新稳定版）
//	stable-1    （1.x 最新稳定版）
//	stable-1.0  （以及类似的 1.1、1.2、1.3 等）
//	latest      （最新版，含 alpha/beta）
//	latest-1    （1.x 最新版，含 alpha/beta）
//	latest-1.0  （以及类似的 1.1、1.2、1.3 等）
func kubernetesReleaseVersionDefault(version string) (string, error) {
	return kubernetesReleaseVersion(version, fetchFromURL)
}

// kubernetesReleaseVersion 获取可用版本信息；测试中用于避免联网。
func kubernetesReleaseVersion(version string, fetcher func(string, time.Duration) (string, error)) (string, error) {
	ver := normalizedBuildVersion(version)
	if len(ver) != 0 {
		return ver, nil
	}

	bucketURL, versionLabel, err := splitVersion(version)
	if err != nil {
		return "", err
	}

	// 若请求 CI bucket 中的精确构建，重新校验。
	ver = normalizedBuildVersion(versionLabel)
	if len(ver) != 0 {
		return ver, nil
	}

	// kubeReleaseLabelRegex 匹配 latest、latest-1、latest-1.10 等标签。
	if kubeReleaseLabelRegex.MatchString(versionLabel) {
		// 尝试获取客户端版本。pkgversion.Get().String() 应始终返回 Go 链接器和构建系统写入的正确版本；
		// 但单独测试某些包时仍可能缺失。
		clientVersion, clientVersionErr := kubeadmVersion(pkgversion.Get().String())
		// 从互联网获取版本。
		url := fmt.Sprintf("%s/%s.txt", bucketURL, versionLabel)
		body, err := fetcher(url, getReleaseVersionTimeout)
		if err != nil {
			if clientVersionErr == nil {
				// 通过回退到客户端版本支持隔离网络环境。
				klog.Warningf("could not fetch a Kubernetes version from the internet: %v", err)
				klog.Warningf("falling back to the local client version: %s", clientVersion)
				return kubernetesReleaseVersion(clientVersion, fetcher)
			}
		}

		if clientVersionErr != nil {
			if err != nil {
				klog.Warningf("could not obtain neither client nor remote version; fall back to: %s", constants.CurrentKubernetesVersion)
				return kubernetesReleaseVersion(constants.CurrentKubernetesVersion.String(), fetcher)
			}

			klog.Warningf("could not obtain client version; using remote version: %s", body)
			return kubernetesReleaseVersion(body, fetcher)
		}

		// 已获取客户端和远程版本；校验并选择稳定版本。
		body, err = validateStableVersion(body, clientVersion)
		if err != nil {
			return "", err
		}
		// 重新校验收到的版本并返回。
		return kubernetesReleaseVersion(body, fetcher)
	}
	return "", errors.Errorf("version %q doesn't match patterns for neither semantic version nor labels (stable, latest, ...)", version)
}

// KubernetesVersionToImageTag 将标签字符串中不允许的字符替换为下划线。
// 镜像标签仅允许大小写字母、数字、下划线、句点和连字符。
// 当前用于 CI 镜像，除 '+' 外均合法；该函数也用于无法预校验的通用输入。
func KubernetesVersionToImageTag(version string) string {
	allowed := regexp.MustCompile(`[^-\w.]`)
	return allowed.ReplaceAllString(version, "_")
}

// KubernetesIsCIVersion 检查用户是否请求 CI 版本。
func KubernetesIsCIVersion(version string) bool {
	subs := kubeBucketPrefixes.FindAllStringSubmatch(version, 1)
	if len(subs) == 1 && len(subs[0]) == 4 && strings.HasPrefix(subs[0][2], "ci") {
		return true
	}
	return false
}

// 内部辅助函数：返回规范化构建版本（必要时添加 "v" 前缀）；输入不匹配已知版本模式时返回空字符串。
func normalizedBuildVersion(version string) string {
	if kubeReleaseRegex.MatchString(version) {
		if strings.HasPrefix(version, "v") {
			return version
		}
		return "v" + version
	}
	return ""
}

// 内部辅助函数：拆分版本各部分，返回基础 URL 和清理后的版本。
func splitVersion(version string) (string, string, error) {
	var bucketURL, urlSuffix string
	subs := kubeBucketPrefixes.FindAllStringSubmatch(version, 1)
	if len(subs) != 1 || len(subs[0]) != 4 {
		return "", "", errors.Errorf("invalid version %q", version)
	}

	switch {
	case strings.HasPrefix(subs[0][2], "ci"):
		// 直接使用用户指定的版本。
		urlSuffix = subs[0][2]
		bucketURL = kubeCIBucketURL
	default:
		urlSuffix = "release"
		bucketURL = kubeReleaseBucketURL
	}
	url := fmt.Sprintf("%s/%s", bucketURL, urlSuffix)
	return url, subs[0][3], nil
}

// 内部辅助函数：返回 URL 内容。
func fetchFromURL(url string, timeout time.Duration) (string, error) {
	klog.V(2).Infof("fetching Kubernetes version from URL: %s", url)
	client := &http.Client{Timeout: timeout, Transport: netutil.SetOldTransportDefaults(&http.Transport{})}
	resp, err := client.Get(url)
	if err != nil {
		return "", errors.Errorf("unable to get URL %q: %s", url, err.Error())
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", errors.Errorf("unable to read content of URL %q: %s", url, err.Error())
	}
	bodyString := strings.TrimSpace(string(body))

	if resp.StatusCode != http.StatusOK {
		msg := fmt.Sprintf("unable to fetch file. URL: %q, status: %v", url, resp.Status)
		return bodyString, errors.New(msg)
	}
	return bodyString, nil
}

// kubeadmVersion 返回不含元数据的客户端版本。
func kubeadmVersion(info string) (string, error) {
	v, err := versionutil.ParseSemantic(info)
	if err != nil {
		return "", errors.Wrap(err, "kubeadm version error")
	}
	// versionutil 没有获取不含元数据版本的工具，因此需手工格式化。
	// 丢弃发布标签后的偏移，仅保留 alpha.0 等标签，不带 .206 等偏移，以符合 GCR 镜像标签要求。
	pre := v.PreRelease()
	patch := v.Patch()
	if len(pre) > 0 {
		if patch > 0 {
			// patch 版本大于零时将其减一并移除标签，以符合最新稳定补丁版本。
			patch = patch - 1
			pre = ""
		} else {
			split := strings.Split(pre, ".")
			if len(split) > 2 {
				pre = split[0] + "." + split[1] // pre = split[0] + "." + split[1] // 排除第三个元素
			} else if len(split) < 2 {
				pre = split[0] + ".0" // pre = split[0] + ".0" // 为不完整标签追加 .0
			}
			pre = "-" + pre
		}
	}
	vStr := fmt.Sprintf("v%d.%d.%d%s", v.Major(), v.Minor(), patch, pre)
	return vStr, nil
}

// 校验远程版本是否仅比客户端版本高一个 MINOR，并保持相同 PATCH，
// 以符合 "stable-X" 规则。
func validateStableVersion(remoteVersion, clientVersion string) (string, error) {
	verRemote, err := versionutil.ParseGeneric(remoteVersion)
	if err != nil {
		return "", errors.Wrap(err, "remote version error")
	}
	verClient, err := versionutil.ParseGeneric(clientVersion)
	if err != nil {
		return "", errors.Wrap(err, "client version error")
	}
	// 远程 Major 更大，或 Major 相同但 Minor 更大时，使用客户端版本发布，同时处理 Major 升级。
	if verClient.Major() < verRemote.Major() ||
		(verClient.Major() == verRemote.Major()) && verClient.Minor() < verRemote.Minor() {
		estimatedRelease := fmt.Sprintf("stable-%d.%d", verClient.Major(), verClient.Minor())
		klog.Infof("remote version is much newer: %s; falling back to: %s", remoteVersion, estimatedRelease)
		return estimatedRelease, nil
	}
	return remoteVersion, nil
}
