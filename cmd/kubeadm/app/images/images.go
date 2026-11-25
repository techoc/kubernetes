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

package images

import (
	"fmt"

	"k8s.io/klog/v2"

	kubeadmapi "k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm"
	kubeadmapiv1 "k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm/v1beta4"
	"k8s.io/kubernetes/cmd/kubeadm/app/constants"
	kubeadmutil "k8s.io/kubernetes/cmd/kubeadm/app/util"
)

// GetGenericImage 生成并返回一个平台无关的镜像（由 manifest list 支持）
func GetGenericImage(prefix, image, tag string) string {
	return fmt.Sprintf("%s/%s:%s", prefix, image, tag)
}

// GetKubernetesImage 生成并返回 Kubernetes 主仓库中管理的组件的镜像，
// 包括控制面组件和 kube-proxy。
func GetKubernetesImage(image string, cfg *kubeadmapi.ClusterConfiguration) string {
	repoPrefix := cfg.GetControlPlaneImageRepository()
	kubernetesImageTag := kubeadmutil.KubernetesVersionToImageTag(cfg.KubernetesVersion)
	return GetGenericImage(repoPrefix, image, kubernetesImageTag)
}

// GetDNSImage 生成并返回 CoreDNS 的镜像。
func GetDNSImage(cfg *kubeadmapi.ClusterConfiguration) string {
	// DNS 默认使用默认镜像仓库
	dnsImageRepository := cfg.ImageRepository
	// 除非指定了覆盖值
	if cfg.DNS.ImageRepository != "" {
		dnsImageRepository = cfg.DNS.ImageRepository
	}
	// 处理官方镜像从 "registry.k8s.io/coredns" 到 "registry.k8s.io/coredns/coredns 的重命名
	if dnsImageRepository == kubeadmapiv1.DefaultImageRepository {
		dnsImageRepository = fmt.Sprintf("%s/coredns", dnsImageRepository)
	}
	// DNS 使用与 Kubernetes 版本匹配的 DNS 版本对应的 imageTag
	dnsImageTag := constants.CoreDNSVersion

	// 除非指定了覆盖值
	if cfg.DNS.ImageTag != "" {
		dnsImageTag = cfg.DNS.ImageTag
	}
	return GetGenericImage(dnsImageRepository, constants.CoreDNSImageName, dnsImageTag)
}

// GetEtcdImage 生成并返回 etcd 的镜像
func GetEtcdImage(cfg *kubeadmapi.ClusterConfiguration, supportedEtcdVersion map[uint8]string) string {
	// Etcd 默认使用默认镜像仓库
	etcdImageRepository := cfg.ImageRepository
	// 除非指定了覆盖值
	if cfg.Etcd.Local != nil && cfg.Etcd.Local.ImageRepository != "" {
		etcdImageRepository = cfg.Etcd.Local.ImageRepository
	}
	etcdImageTag := GetEtcdImageTag(cfg, supportedEtcdVersion)
	return GetGenericImage(etcdImageRepository, constants.Etcd, etcdImageTag)
}

// GetEtcdImageTag 生成并返回 etcd 的镜像标签
func GetEtcdImageTag(cfg *kubeadmapi.ClusterConfiguration, supportedEtcdVersion map[uint8]string) string {
	// Etcd 使用与 Kubernetes 版本匹配的 etcd 版本对应的 imageTag
	etcdImageTag := constants.DefaultEtcdVersion
	etcdVersion, warning, err := constants.EtcdSupportedVersion(supportedEtcdVersion, cfg.KubernetesVersion)
	if err == nil {
		etcdImageTag = etcdVersion.String()
	}
	if warning != nil {
		klog.V(1).Infof("WARNING: %v", warning)
	}
	// 除非指定了覆盖值
	if cfg.Etcd.Local != nil && cfg.Etcd.Local.ImageTag != "" {
		etcdImageTag = cfg.Etcd.Local.ImageTag
	}
	return etcdImageTag
}

// GetControlPlaneImages 返回 kubeadm 预期在控制面节点上使用的容器镜像列表
func GetControlPlaneImages(cfg *kubeadmapi.ClusterConfiguration) []string {
	images := make([]string, 0)

	// 从核心 Kubernetes 镜像开始
	images = append(images, GetKubernetesImage(constants.KubeAPIServer, cfg))
	images = append(images, GetKubernetesImage(constants.KubeControllerManager, cfg))
	images = append(images, GetKubernetesImage(constants.KubeScheduler, cfg))

	// 如果未禁用 Proxy 插件，则添加其镜像
	if cfg.Proxy.Disabled {
		klog.V(1).Infof("skipping the kube-proxy image pull since the bundled addon is disabled")
	} else {
		images = append(images, GetKubernetesImage(constants.KubeProxy, cfg))
	}
	// 如果未禁用 DNS 插件，则添加其镜像
	if cfg.DNS.Disabled {
		klog.V(1).Infof("skipping the CoreDNS image pull since the bundled addon is disabled")
	} else {
		images = append(images, GetDNSImage(cfg))
	}

	// pause 在 ci 镜像仓库中不可用，因此使用默认镜像仓库。
	images = append(images, GetPauseImage(cfg))

	// 如果 etcd 不是外部的，则添加其镜像，因为需要它
	if cfg.Etcd.Local != nil {
		images = append(images, GetEtcdImage(cfg, constants.SupportedEtcdVersion))
	}

	return images
}

// GetPauseImage 返回 "pause" 容器的镜像
func GetPauseImage(cfg *kubeadmapi.ClusterConfiguration) string {
	return GetGenericImage(cfg.ImageRepository, "pause", constants.PauseVersion)
}
