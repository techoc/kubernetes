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

package upgrade

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	versionutil "k8s.io/apimachinery/pkg/util/version"
	pkgversion "k8s.io/apimachinery/pkg/version"
	fakediscovery "k8s.io/client-go/discovery/fake"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/component-base/version"

	"k8s.io/kubernetes/cmd/kubeadm/app/constants"
	"k8s.io/kubernetes/cmd/kubeadm/app/phases/addons/dns"
	kubeadmutil "k8s.io/kubernetes/cmd/kubeadm/app/util"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/image"
)

// VersionGetter 定义获取各种版本的接口。
// 便于为实现该接口的伪造版本提供单元测试。
type VersionGetter interface {
	// ClusterVersion 应返回集群版本，即 API 服务器版本。
	ClusterVersion() (string, *versionutil.Version, error)
	// KubeadmVersion 应返回 kubeadm CLI 版本。
	KubeadmVersion() (string, *versionutil.Version, error)
	// VersionFromCILabel 应将 `latest`、`stable`、`stable-1.8` 等 CI 标签解析为实际版本。
	VersionFromCILabel(string, string) (string, *versionutil.Version, error)
	// KubeletVersions 应返回版本到节点名称列表的映射，以描述各版本的 kubelet 数量。
	KubeletVersions() (map[string][]string, error)
	// ComponentVersions 应返回版本到节点名称列表的映射，以描述各版本的控制平面组件数量。
	ComponentVersions(string) (map[string][]string, error)
	// DNSAddonVersion 返回已部署的 CoreDNS 镜像标签；若未部署则返回零值。
	DNSAddonVersion() (string, error)
}

// KubeVersionGetter 负责从外部来源获取版本。
type KubeVersionGetter struct {
	client clientset.Interface
}

// NewKubeVersionGetter 创建 KubeVersionGetter 实例。
func NewKubeVersionGetter(client clientset.Interface) VersionGetter {
	return &KubeVersionGetter{
		client: client,
	}
}

// ClusterVersion 获取 API 服务器版本。
func (g *KubeVersionGetter) ClusterVersion() (string, *versionutil.Version, error) {
	var (
		clusterVersionInfo *pkgversion.Info
		err                error
	)
	// 试运行时不尝试获取 /version 资源，而是返回存储的 FakeServerVersion。
	// 该值在 common.go#getClient() 中构造试运行客户端时设置。
	// 问题在于升级试运行客户端反应器由动态客户端通过 NewClientBackedDryRunGetterFromKubeconfig() 支持，
	// 而 GetActions 似乎没有与动态客户端的 Discovery().ServerVersion() 相对应的资源。
	fakeclientDiscovery, ok := g.client.Discovery().(*fakediscovery.FakeDiscovery)
	if ok {
		clusterVersionInfo = fakeclientDiscovery.FakedServerVersion
	} else {
		clusterVersionInfo, err = g.client.Discovery().ServerVersion()
		if err != nil {
			return "", nil, errors.Wrap(err, "Couldn't fetch cluster version from the API Server")
		}
	}

	clusterVersion, err := versionutil.ParseSemantic(clusterVersionInfo.String())
	if err != nil {
		return "", nil, errors.Wrap(err, "Couldn't parse cluster version")
	}
	return clusterVersionInfo.String(), clusterVersion, nil
}

// KubeadmVersion 获取 kubeadm 版本。
func (g *KubeVersionGetter) KubeadmVersion() (string, *versionutil.Version, error) {
	kubeadmVersionInfo := version.Get()

	kubeadmVersion, err := versionutil.ParseSemantic(kubeadmVersionInfo.String())
	if err != nil {
		return "", nil, errors.Wrap(err, "Couldn't parse kubeadm version")
	}
	return kubeadmVersionInfo.String(), kubeadmVersion, nil
}

// VersionFromCILabel 使用公开的 Kubernetes CI 上传信息，将 "latest" 或 "stable" 等版本标签解析为实际版本。
func (g *KubeVersionGetter) VersionFromCILabel(ciVersionLabel, description string) (string, *versionutil.Version, error) {
	versionStr, err := kubeadmutil.KubernetesReleaseVersion(ciVersionLabel)
	if err != nil {
		return "", nil, errors.Wrapf(err, "Couldn't fetch latest %s from the internet", description)
	}

	ver, err := versionutil.ParseSemantic(versionStr)
	if err != nil {
		return "", nil, errors.Wrapf(err, "Couldn't parse latest %s", description)
	}
	return versionStr, ver, nil
}

// KubeletVersions 获取集群中的 kubelet 版本。
func (g *KubeVersionGetter) KubeletVersions() (map[string][]string, error) {
	nodes, err := g.client.CoreV1().Nodes().List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return nil, errors.New("couldn't list all nodes in cluster")
	}

	// 将 kubelet 版本映射到节点名称列表。
	kubeletVersions := make(map[string][]string)
	for _, node := range nodes.Items {
		kver := node.Status.NodeInfo.KubeletVersion
		kubeletVersions[kver] = append(kubeletVersions[kver], node.Name)
	}
	return kubeletVersions, nil
}

// DNSAddonVersion 返回集群中部署的 CoreDNS 镜像；若存在多个实例，则返回错误。
func (g *KubeVersionGetter) DNSAddonVersion() (string, error) {
	return dns.DeployedDNSAddon(g.client)
}

// ComponentVersions 获取集群中控制平面组件的版本。
// name 参数是要获取版本的组件名称。
// 函数返回以版本为键、节点名称列表为值的映射。
func (g *KubeVersionGetter) ComponentVersions(name string) (map[string][]string, error) {
	podList, err := g.client.CoreV1().Pods(metav1.NamespaceSystem).List(
		context.TODO(),
		metav1.ListOptions{
			LabelSelector: fmt.Sprintf("component=%s,tier=%s", name, constants.ControlPlaneTier),
		},
	)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't list pods in cluster")
	}

	componentVersions := make(map[string][]string)
	for _, pod := range podList.Items {
		tag := convertImageTagMetadataToSemver(image.TagFromImage(pod.Spec.Containers[0].Image))
		componentVersions[tag] = append(componentVersions[tag], pod.Spec.NodeName)
	}
	return componentVersions, nil
}

// OfflineVersionGetter 使用提供的版本；未提供时委托给内嵌的 VersionGetter。
type OfflineVersionGetter struct {
	VersionGetter
	version string
}

// NewOfflineVersionGetter 包装 VersionGetter；提供默认信息时跳过在线通信。
// Version 可以为 ""，此时行为与传入的 versionGetter 相同。
func NewOfflineVersionGetter(versionGetter VersionGetter, version string) VersionGetter {
	return &OfflineVersionGetter{
		VersionGetter: versionGetter,
		version:       version,
	}
}

// VersionFromCILabel 返回传入结构体的版本；若未提供版本，则委托给内嵌的 VersionGetter。
func (o *OfflineVersionGetter) VersionFromCILabel(ciVersionLabel, description string) (string, *versionutil.Version, error) {
	if o.version == "" {
		return o.VersionGetter.VersionFromCILabel(ciVersionLabel, description)
	}
	ver, err := versionutil.ParseSemantic(o.version)
	if err != nil {
		return "", nil, errors.Wrapf(err, "Couldn't parse version %s", description)
	}
	return o.version, ver, nil
}
