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

package deployment

import (
	"context"
	"fmt"
	"sort"

	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	testutils "k8s.io/kubernetes/test/utils"
	admissionapi "k8s.io/pod-security-admission/api"
)

// UpdateDeploymentWithRetries 带重试地更新指定的 deployment。
func UpdateDeploymentWithRetries(c clientset.Interface, namespace, name string, applyUpdate testutils.UpdateDeploymentFunc) (*appsv1.Deployment, error) {
	return testutils.UpdateDeploymentWithRetries(c, namespace, name, applyUpdate, framework.Logf, poll, pollShortTimeout)
}

// NewDeployment 按指定参数返回一个 deployment spec。
func NewDeployment(deploymentName string, replicas int32, podLabels map[string]string, containerName, image string, strategyType appsv1.DeploymentStrategyType) *appsv1.Deployment {
	zero := int64(0)
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:   deploymentName,
			Labels: podLabels,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: podLabels},
			Strategy: appsv1.DeploymentStrategy{
				Type: strategyType,
			},
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: podLabels,
				},
				Spec: v1.PodSpec{
					TerminationGracePeriodSeconds: &zero,
					Containers: []v1.Container{
						{
							Name:            containerName,
							Image:           image,
							SecurityContext: &v1.SecurityContext{},
						},
					},
				},
			},
		},
	}
}

// CreateDeployment 创建一个 deployment。
func CreateDeployment(ctx context.Context, client clientset.Interface, replicas int32, podLabels map[string]string, nodeSelector map[string]string, namespace string, pvclaims []*v1.PersistentVolumeClaim, securityLevel admissionapi.Level, command string) (*appsv1.Deployment, error) {
	deploymentSpec := testDeployment(replicas, podLabels, nodeSelector, namespace, pvclaims, securityLevel, command)
	deployment, err := client.AppsV1().Deployments(namespace).Create(ctx, deploymentSpec, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("deployment %q Create API error: %w", deploymentSpec.Name, err)
	}
	framework.Logf("Waiting deployment %q to complete", deploymentSpec.Name)
	err = WaitForDeploymentComplete(client, deployment)
	if err != nil {
		return nil, fmt.Errorf("deployment %q failed to complete: %w", deploymentSpec.Name, err)
	}
	return deployment, nil
}

// GetPodsForDeployment 获取给定 deployment 下的 pod。
func GetPodsForDeployment(ctx context.Context, client clientset.Interface, deployment *appsv1.Deployment) (*v1.PodList, error) {
	replicaSetSelector, err := metav1.LabelSelectorAsSelector(deployment.Spec.Selector)
	if err != nil {
		return nil, err
	}

	replicaSetListOptions := metav1.ListOptions{LabelSelector: replicaSetSelector.String()}
	allReplicaSets, err := client.AppsV1().ReplicaSets(deployment.Namespace).List(ctx, replicaSetListOptions)
	if err != nil {
		return nil, err
	}

	ownedReplicaSets := make([]*appsv1.ReplicaSet, 0, len(allReplicaSets.Items))
	for i := range allReplicaSets.Items {
		if !metav1.IsControlledBy(&allReplicaSets.Items[i], deployment) {
			continue
		}

		ownedReplicaSets = append(ownedReplicaSets, &allReplicaSets.Items[i])
	}

	// 忽略 pod-template-hash，因为：
	// 1. podTemplateSpec API 变化（例如新增字段）会导致 hash 结果改变
	// 2. deployment 模板本身没有 hash label
	podTemplatesEqualsIgnoringHash := func(template1, template2 *v1.PodTemplateSpec) bool {
		t1Copy := template1.DeepCopy()
		t2Copy := template2.DeepCopy()
		// 比较前先从 template.Labels 中去掉 hash label
		delete(t1Copy.Labels, appsv1.DefaultDeploymentUniqueLabelKey)
		delete(t2Copy.Labels, appsv1.DefaultDeploymentUniqueLabelKey)
		return apiequality.Semantic.DeepEqual(t1Copy, t2Copy)
	}

	var replicaSet *appsv1.ReplicaSet
	// 极少数情况下（例如集群升级后），Deployment 可能出现多个模板相同的新
	// ReplicaSet，见 https://github.com/kubernetes/kubernetes/issues/40415
	// 这里确定性地选择最早创建的那个。
	sort.Sort(replicaSetsByCreationTimestamp(ownedReplicaSets))
	for i, rs := range ownedReplicaSets {
		if !podTemplatesEqualsIgnoringHash(&ownedReplicaSets[i].Spec.Template, &deployment.Spec.Template) {
			continue
		}

		replicaSet = rs
		break
	}

	if replicaSet == nil {
		return nil, fmt.Errorf("expected a new replica set for deployment %q, found none", deployment.Name)
	}

	podSelector, err := metav1.LabelSelectorAsSelector(deployment.Spec.Selector)
	if err != nil {
		return nil, err
	}
	podListOptions := metav1.ListOptions{LabelSelector: podSelector.String()}
	allPods, err := client.CoreV1().Pods(deployment.Namespace).List(ctx, podListOptions)
	if err != nil {
		return nil, err
	}

	replicaSetUID := replicaSet.UID
	ownedPods := &v1.PodList{Items: make([]v1.Pod, 0, len(allPods.Items))}
	for i, pod := range allPods.Items {
		controllerRef := metav1.GetControllerOf(&allPods.Items[i])
		if controllerRef != nil && controllerRef.UID == replicaSetUID {
			ownedPods.Items = append(ownedPods.Items, pod)
		}
	}

	return ownedPods, nil
}

// replicaSetsByCreationTimestamp 按创建时间排序 ReplicaSet 列表，时间相同时以名称为 tie breaker。
type replicaSetsByCreationTimestamp []*appsv1.ReplicaSet

func (o replicaSetsByCreationTimestamp) Len() int      { return len(o) }
func (o replicaSetsByCreationTimestamp) Swap(i, j int) { o[i], o[j] = o[j], o[i] }
func (o replicaSetsByCreationTimestamp) Less(i, j int) bool {
	if o[i].CreationTimestamp.Equal(&o[j].CreationTimestamp) {
		return o[i].Name < o[j].Name
	}
	return o[i].CreationTimestamp.Before(&o[j].CreationTimestamp)
}

// testDeployment 在指定 namespace 下创建 deployment 定义，引用给定 PVC 的名称。
// 可传入一组 BASH 命令作为 pod 要执行的参数。
func testDeployment(replicas int32, podLabels map[string]string, nodeSelector map[string]string, namespace string, pvclaims []*v1.PersistentVolumeClaim, securityLevel admissionapi.Level, command string) *appsv1.Deployment {
	if len(command) == 0 {
		command = e2epod.InfiniteSleepCommand
	}
	zero := int64(0)
	deploymentName := "deployment-" + string(uuid.NewUUID())
	deploymentSpec := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      deploymentName,
			Namespace: namespace,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: podLabels,
			},
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: podLabels,
				},
				Spec: v1.PodSpec{
					TerminationGracePeriodSeconds: &zero,
					Containers: []v1.Container{
						{
							Name:            "write-pod",
							Image:           e2epod.GetDefaultTestImage(),
							Command:         e2epod.GenerateScriptCmd(command),
							SecurityContext: e2epod.GenerateContainerSecurityContext(securityLevel),
						},
					},
					RestartPolicy: v1.RestartPolicyAlways,
				},
			},
		},
	}
	var volumeMounts = make([]v1.VolumeMount, len(pvclaims))
	var volumes = make([]v1.Volume, len(pvclaims))
	for index, pvclaim := range pvclaims {
		volumename := fmt.Sprintf("volume%v", index+1)
		volumeMounts[index] = v1.VolumeMount{Name: volumename, MountPath: "/mnt/" + volumename}
		volumes[index] = v1.Volume{Name: volumename, VolumeSource: v1.VolumeSource{PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{ClaimName: pvclaim.Name, ReadOnly: false}}}
	}
	deploymentSpec.Spec.Template.Spec.Containers[0].VolumeMounts = volumeMounts
	deploymentSpec.Spec.Template.Spec.Volumes = volumes
	if nodeSelector != nil {
		deploymentSpec.Spec.Template.Spec.NodeSelector = nodeSelector
	}
	return deploymentSpec
}
