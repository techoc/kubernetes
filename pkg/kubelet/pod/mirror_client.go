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

package pod

import (
	"context"
	"fmt"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	kubetypes "k8s.io/kubernetes/pkg/kubelet/types"
)

// MirrorClient 负责在 API Server 中创建/删除镜像 Pod。
type MirrorClient interface {
	// CreateMirrorPod 为给定 Pod 在 API Server 中创建镜像 Pod；
	// 镜像 Pod 继承原注解，并额外记录静态 Pod 的 hash。
	CreateMirrorPod(ctx context.Context, pod *v1.Pod) error
	// DeleteMirrorPod 按全名删除镜像 Pod；uid 非 nil 时仅当 UID 匹配才删除。
	DeleteMirrorPod(ctx context.Context, podFullName string, uid *types.UID) (bool, error)
}

// nodeGetter 是 NodeLister 的子集，便于测试。
type nodeGetter interface {
	// 按名称获取 Node。
	Get(name string) (*v1.Node, error)
}

// basicMirrorClient 是 MirrorClient 的功能实现。
// 镜像 Pod 直接由 kubelet 维护，因为它们需要与内部 Pod 保持同步。
type basicMirrorClient struct {
	apiserverClient clientset.Interface
	nodeGetter      nodeGetter
	nodeName        string
}

// NewBasicMirrorClient 返回新的 MirrorClient。
func NewBasicMirrorClient(apiserverClient clientset.Interface, nodeName string, nodeGetter nodeGetter) MirrorClient {
	return &basicMirrorClient{
		apiserverClient: apiserverClient,
		nodeName:        nodeName,
		nodeGetter:      nodeGetter,
	}
}

// CreateMirrorPod 复制 Pod 并写入镜像注解与节点 ownerReference，然后在 API Server 创建。
func (mc *basicMirrorClient) CreateMirrorPod(ctx context.Context, pod *v1.Pod) error {
	if mc.apiserverClient == nil {
		return nil
	}
	// 复制 Pod，避免修改调用方传入的对象。
	copyPod := *pod
	copyPod.Annotations = make(map[string]string)

	for k, v := range pod.Annotations {
		copyPod.Annotations[k] = v
	}
	// 记录静态 Pod 的 hash，用于判断已有镜像 Pod 是否是最新的。
	hash := getPodHash(pod)
	copyPod.Annotations[kubetypes.ConfigMirrorAnnotationKey] = hash

	// 启用 MirrorPodNodeRestriction 后，镜像 Pod 必须带指向所属节点的 ownerReference，
	// 参见 https://git.k8s.io/enhancements/keps/sig-auth/1314-node-restriction-pods/README.md。
	nodeUID, err := mc.getNodeUID()
	if err != nil {
		return fmt.Errorf("failed to get node UID: %v", err)
	}
	controller := true
	copyPod.OwnerReferences = []metav1.OwnerReference{{
		APIVersion: v1.SchemeGroupVersion.String(),
		Kind:       "Node",
		Name:       mc.nodeName,
		UID:        nodeUID,
		Controller: &controller,
	}}

	// 已存在时比较 hash：一致说明镜像已是最新，返回成功。
	apiPod, err := mc.apiserverClient.CoreV1().Pods(copyPod.Namespace).Create(ctx, &copyPod, metav1.CreateOptions{})
	if err != nil && apierrors.IsAlreadyExists(err) {
		// Check if the existing pod is the same as the pod we want to create.
		if h, ok := apiPod.Annotations[kubetypes.ConfigMirrorAnnotationKey]; ok && h == hash {
			return nil
		}
	}
	return err
}

// DeleteMirrorPod 删除镜像 Pod。
// 参数为 Pod 全名与可选 UID；UID 非 nil 时仅当 Pod UID 匹配才删除。
// 返回是否真正删除，以及解析 Pod 全名时的错误。
// Pod 不存在或 UID 不匹配不算错误，此时返回 false。
func (mc *basicMirrorClient) DeleteMirrorPod(ctx context.Context, podFullName string, uid *types.UID) (bool, error) {
	if mc.apiserverClient == nil {
		return false, nil
	}
	logger := klog.FromContext(ctx)
	// 全名格式非法时返回错误，调用方无法安全删除。
	name, namespace, err := kubecontainer.ParsePodFullName(podFullName)
	if err != nil {
		logger.Error(err, "Failed to parse a pod full name", "podFullName", podFullName)
		return false, err
	}

	// 预构造 UID 值用于日志。
	var uidValue types.UID
	if uid != nil {
		uidValue = *uid
	}
	logger.V(2).Info("Deleting a mirror pod", "pod", klog.KRef(namespace, name), "podUID", uidValue)

	// 设置 0 秒宽限期，立即删除。
	var GracePeriodSeconds int64
	if err := mc.apiserverClient.CoreV1().Pods(namespace).Delete(ctx, name, metav1.DeleteOptions{GracePeriodSeconds: &GracePeriodSeconds, Preconditions: &metav1.Preconditions{UID: uid}}); err != nil {
		// 无法通过通用错误判断前置条件失败：NotFound/Conflict 属于预期情况。
		if !(apierrors.IsNotFound(err) || apierrors.IsConflict(err)) {
			// 这里本应返回错误，但历史上该函数只在解析 Pod 名失败时返回错误，
			// 因此保持兼容只记录日志。
			logger.Error(err, "Failed deleting a mirror pod", "pod", klog.KRef(namespace, name))
		}
		return false, nil
	}
	return true, nil
}

// getNodeUID 获取节点 UID，缺失 UID 时返回错误。
func (mc *basicMirrorClient) getNodeUID() (types.UID, error) {
	node, err := mc.nodeGetter.Get(mc.nodeName)
	if err != nil {
		return "", err
	}
	if node.UID == "" {
		return "", fmt.Errorf("UID unset for node %s", mc.nodeName)
	}
	return node.UID, nil
}

// getHashFromMirrorPod 读取镜像 Pod 注解中的源 Pod hash。
func getHashFromMirrorPod(pod *v1.Pod) (string, bool) {
	hash, ok := pod.Annotations[kubetypes.ConfigMirrorAnnotationKey]
	return hash, ok
}

// getPodHash 读取静态 Pod 的 hash 注解。
func getPodHash(pod *v1.Pod) string {
	// 所有静态 Pod 都带有该注解。
	return pod.Annotations[kubetypes.ConfigHashAnnotationKey]
}
