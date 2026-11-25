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

package pod

import (
	"context"
	"fmt"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	imageutils "k8s.io/kubernetes/test/utils/image"
	admissionapi "k8s.io/pod-security-admission/api"
)

const (
	VolumeMountPathTemplate = "/mnt/volume%d"
	VolumeMountPath1        = "/mnt/volume1"
)

// Config 包含创建 pod 所需的全部参数。
// SELinux 测试需要以布尔参数形式传入 HostIPC 和 HostPID。
type Config struct {
	NS                     string
	PVCs                   []*v1.PersistentVolumeClaim
	PVCsReadOnly           bool
	InlineVolumeSources    []*v1.VolumeSource
	SecurityLevel          admissionapi.Level
	Command                string
	HostIPC                bool
	HostPID                bool
	SeLinuxLabel           *v1.SELinuxOptions
	FsGroup                *int64
	NodeSelection          NodeSelection
	ImageID                imageutils.ImageID
	PodFSGroupChangePolicy *v1.PodFSGroupChangePolicy
	PodSELinuxChangePolicy *v1.PodSELinuxChangePolicy
}

// CreateUnschedulablePod 按 node selector 和给定的 PVC 创建不可调度的 pod。
func CreateUnschedulablePod(ctx context.Context, client clientset.Interface, namespace string, nodeSelector map[string]string, pvclaims []*v1.PersistentVolumeClaim, securityLevel admissionapi.Level, command string) (*v1.Pod, error) {
	pod := MakePod(namespace, nodeSelector, pvclaims, securityLevel, command)
	pod, err := client.CoreV1().Pods(namespace).Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("pod Create API error: %w", err)
	}
	// 等待 pod 变成 Unschedulable
	err = WaitForPodNameUnschedulableInNamespace(ctx, client, pod.Name, namespace)
	if err != nil {
		return pod, fmt.Errorf("pod %q is not Unschedulable: %w", pod.Name, err)
	}
	// 重新获取最新的 pod 信息
	pod, err = client.CoreV1().Pods(namespace).Get(ctx, pod.Name, metav1.GetOptions{})
	if err != nil {
		return pod, fmt.Errorf("pod Get API error: %w", err)
	}
	return pod, nil
}

// CreateClientPod 定义并创建一个挂载了 PV 的 pod，它会一直运行直到被杀掉。
func CreateClientPod(ctx context.Context, c clientset.Interface, ns string, pvc *v1.PersistentVolumeClaim) (*v1.Pod, error) {
	return CreatePod(ctx, c, ns, nil, []*v1.PersistentVolumeClaim{pvc}, admissionapi.LevelPrivileged, "")
}

// CreatePod 按 node selector 和给定的 PVC 创建 pod。
func CreatePod(ctx context.Context, client clientset.Interface, namespace string, nodeSelector map[string]string, pvclaims []*v1.PersistentVolumeClaim, securityLevel admissionapi.Level, command string) (*v1.Pod, error) {
	pod := MakePod(namespace, nodeSelector, pvclaims, securityLevel, command)
	pod, err := client.CoreV1().Pods(namespace).Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("pod Create API error: %w", err)
	}
	// 等待 pod 进入 running 状态
	err = WaitForPodNameRunningInNamespace(ctx, client, pod.Name, namespace)
	if err != nil {
		return pod, fmt.Errorf("pod %q is not Running: %w", pod.Name, err)
	}
	// 重新获取最新的 pod 信息
	pod, err = client.CoreV1().Pods(namespace).Get(ctx, pod.Name, metav1.GetOptions{})
	if err != nil {
		return pod, fmt.Errorf("pod Get API error: %w", err)
	}
	return pod, nil
}

// CreateSecPod 用给定的 Config 创建安全测试 pod。
func CreateSecPod(ctx context.Context, client clientset.Interface, podConfig *Config, timeout time.Duration) (*v1.Pod, error) {
	return CreateSecPodWithNodeSelection(ctx, client, podConfig, timeout)
}

// CreateSecPodWithNodeSelection 用给定的 Config 创建带节点选择的安全测试 pod。
func CreateSecPodWithNodeSelection(ctx context.Context, client clientset.Interface, podConfig *Config, timeout time.Duration) (*v1.Pod, error) {
	pod, err := MakeSecPod(podConfig)
	if err != nil {
		return nil, fmt.Errorf("Unable to create pod: %w", err)
	}

	pod, err = client.CoreV1().Pods(podConfig.NS).Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("pod Create API error: %w", err)
	}

	// 等待 pod 进入 running 状态
	err = WaitTimeoutForPodRunningInNamespace(ctx, client, pod.Name, podConfig.NS, timeout)
	if err != nil {
		return pod, fmt.Errorf("pod %q is not Running: %w", pod.Name, err)
	}
	// 重新获取最新的 pod 信息
	pod, err = client.CoreV1().Pods(podConfig.NS).Get(ctx, pod.Name, metav1.GetOptions{})
	if err != nil {
		return pod, fmt.Errorf("pod Get API error: %w", err)
	}
	return pod, nil
}

// MakePod 在指定 namespace 下返回 pod 定义，引用给定 PVC 的名称。
// 可以传入一组 BASH 命令作为 pod 要执行的参数。
func MakePod(ns string, nodeSelector map[string]string, pvclaims []*v1.PersistentVolumeClaim, securityLevel admissionapi.Level, command string) *v1.Pod {
	if len(command) == 0 {
		command = InfiniteSleepCommand
	}
	podSpec := &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "pvc-tester-",
			Namespace:    ns,
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:            "write-pod",
					Image:           GetDefaultTestImage(),
					Command:         GenerateScriptCmd(command),
					SecurityContext: GenerateContainerSecurityContext(securityLevel),
				},
			},
			RestartPolicy: v1.RestartPolicyOnFailure,
		},
	}
	setVolumes(&podSpec.Spec, pvclaims, nil /*inline volume sources*/, false /*PVCs readonly*/)
	if nodeSelector != nil {
		podSpec.Spec.NodeSelector = nodeSelector
	}
	if securityLevel == admissionapi.LevelRestricted {
		podSpec = MustMixinRestrictedPodSecurity(podSpec)
	}

	return podSpec
}

// MakeSecPod 在指定 namespace 下返回 pod 定义，引用给定 PVC 的名称。
// 可以传入一组 BASH 命令作为 pod 要执行的参数。
func MakeSecPod(podConfig *Config) (*v1.Pod, error) {
	if podConfig.NS == "" {
		return nil, fmt.Errorf("Cannot create pod with empty namespace")
	}
	if len(podConfig.Command) == 0 {
		podConfig.Command = InfiniteSleepCommand
	}

	podName := "pod-" + string(uuid.NewUUID())
	if podConfig.FsGroup == nil && !framework.NodeOSDistroIs("windows") {
		podConfig.FsGroup = func(i int64) *int64 {
			return &i
		}(1000)
	}
	podSpec := &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: podConfig.NS,
		},
		Spec: *MakePodSpec(podConfig),
	}
	return podSpec, nil
}

// MakePodSpec 返回 PodSpec 定义。
func MakePodSpec(podConfig *Config) *v1.PodSpec {
	image := imageutils.BusyBox
	if podConfig.ImageID != imageutils.None {
		image = podConfig.ImageID
	}
	securityLevel := podConfig.SecurityLevel
	if securityLevel == "" {
		securityLevel = admissionapi.LevelBaseline
	}
	podSpec := &v1.PodSpec{
		HostIPC:         podConfig.HostIPC,
		HostPID:         podConfig.HostPID,
		SecurityContext: GeneratePodSecurityContext(podConfig.FsGroup, podConfig.SeLinuxLabel),
		Containers: []v1.Container{
			{
				Name:            "write-pod",
				Image:           GetTestImage(image),
				Command:         GenerateScriptCmd(podConfig.Command),
				SecurityContext: GenerateContainerSecurityContext(securityLevel),
			},
		},
		RestartPolicy: v1.RestartPolicyOnFailure,
	}

	if podConfig.PodFSGroupChangePolicy != nil {
		podSpec.SecurityContext.FSGroupChangePolicy = podConfig.PodFSGroupChangePolicy
	}
	if podConfig.PodSELinuxChangePolicy != nil {
		podSpec.SecurityContext.SELinuxChangePolicy = podConfig.PodSELinuxChangePolicy
	}

	setVolumes(podSpec, podConfig.PVCs, podConfig.InlineVolumeSources, podConfig.PVCsReadOnly)
	SetNodeSelection(podSpec, podConfig.NodeSelection)
	return podSpec
}

func setVolumes(podSpec *v1.PodSpec, pvcs []*v1.PersistentVolumeClaim, inlineVolumeSources []*v1.VolumeSource, pvcsReadOnly bool) {
	var volumeMounts = make([]v1.VolumeMount, 0)
	var volumeDevices = make([]v1.VolumeDevice, 0)
	var volumes = make([]v1.Volume, len(pvcs)+len(inlineVolumeSources))
	volumeIndex := 0
	for _, pvclaim := range pvcs {
		volumename := fmt.Sprintf("volume%v", volumeIndex+1)
		volumeMountPath := fmt.Sprintf(VolumeMountPathTemplate, volumeIndex+1)
		if pvclaim.Spec.VolumeMode != nil && *pvclaim.Spec.VolumeMode == v1.PersistentVolumeBlock {
			volumeDevices = append(volumeDevices, v1.VolumeDevice{Name: volumename, DevicePath: volumeMountPath})
		} else {
			volumeMounts = append(volumeMounts, v1.VolumeMount{Name: volumename, MountPath: volumeMountPath})
		}
		volumes[volumeIndex] = v1.Volume{
			Name: volumename,
			VolumeSource: v1.VolumeSource{
				PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
					ClaimName: pvclaim.Name,
					ReadOnly:  pvcsReadOnly,
				},
			},
		}
		volumeIndex++
	}
	for _, src := range inlineVolumeSources {
		volumename := fmt.Sprintf("volume%v", volumeIndex+1)
		volumeMountPath := fmt.Sprintf(VolumeMountPathTemplate, volumeIndex+1)
		// inline volume 只能是 filesystem 类型，不能是 block。
		volumeMounts = append(volumeMounts, v1.VolumeMount{Name: volumename, MountPath: volumeMountPath})
		volumes[volumeIndex] = v1.Volume{Name: volumename, VolumeSource: *src}
		volumeIndex++
	}
	podSpec.Containers[0].VolumeMounts = volumeMounts
	podSpec.Containers[0].VolumeDevices = volumeDevices
	podSpec.Volumes = volumes
}
