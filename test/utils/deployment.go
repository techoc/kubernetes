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

package utils

import (
	"context"
	"errors"
	"fmt"
	"time"

	apps "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	deploymentutil "k8s.io/kubernetes/pkg/controller/deployment/util"
	labelsutil "k8s.io/kubernetes/pkg/util/labels"
	"k8s.io/utils/dump"
)

type LogfFn func(format string, args ...interface{})

func LogReplicaSetsOfDeployment(deployment *apps.Deployment, allOldRSs []*apps.ReplicaSet, newRS *apps.ReplicaSet, logf LogfFn) {
	if newRS != nil {
		logf("New ReplicaSet %q of Deployment %q:\n%s", newRS.Name, deployment.Name, dump.Pretty(*newRS))
	} else {
		logf("New ReplicaSet of Deployment %q is nil.", deployment.Name)
	}
	if len(allOldRSs) > 0 {
		logf("All old ReplicaSets of Deployment %q:", deployment.Name)
	}
	for i := range allOldRSs {
		logf(dump.Pretty(*allOldRSs[i]))
	}
}

func LogPodsOfDeployment(c clientset.Interface, deployment *apps.Deployment, rsList []*apps.ReplicaSet, logf LogfFn) {
	minReadySeconds := deployment.Spec.MinReadySeconds
	podListFunc := func(namespace string, options metav1.ListOptions) (*v1.PodList, error) {
		return c.CoreV1().Pods(namespace).List(context.TODO(), options)
	}

	podList, err := deploymentutil.ListPods(deployment, rsList, podListFunc)
	if err != nil {
		logf("Failed to list Pods of Deployment %q: %v", deployment.Name, err)
		return
	}
	for _, pod := range podList.Items {
		availability := "not available"
		if podutil.IsPodAvailable(&pod, minReadySeconds, metav1.Now()) {
			availability = "available"
		}
		logf("Pod %q is %s:\n%s", pod.Name, availability, dump.Pretty(pod))
	}
}

// 等待 deployment 完成。
// 如果在滚动更新期间（rolling == true），且 deployment 的
// 滚动更新策略（max unavailable 或 max surge）在任意时刻被破坏，则返回错误。
// 如果是在扩缩容事件后不久，或者 deployment 刚创建，则不会视为滚动更新。
func waitForDeploymentCompleteMaybeCheckRolling(c clientset.Interface, d *apps.Deployment, rolling bool, logf LogfFn, pollInterval, pollTimeout time.Duration) error {
	var (
		deployment *apps.Deployment
		reason     string
	)

	err := wait.PollImmediate(pollInterval, pollTimeout, func() (bool, error) {
		var err error
		deployment, err = c.AppsV1().Deployments(d.Namespace).Get(context.TODO(), d.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}

		// 如果处于滚动更新期间，确保滚动更新策略在任何时刻都没有被破坏。
		if rolling {
			reason, err = checkRollingUpdateStatus(c, deployment, logf)
			if err != nil {
				return false, err
			}
			logf(reason)
		}

		// 当 deployment 状态及其底层资源达到期望状态时，就完成了
		if deploymentutil.DeploymentComplete(d, &deployment.Status) {
			return true, nil
		}

		reason = fmt.Sprintf("deployment status: %#v", deployment.Status)
		logf(reason)

		return false, nil
	})

	if wait.Interrupted(err) {
		err = fmt.Errorf("%s", reason)
	}
	if err != nil {
		return fmt.Errorf("error waiting for deployment %q status to match expectation: %v", d.Name, err)
	}
	return nil
}

func checkRollingUpdateStatus(c clientset.Interface, deployment *apps.Deployment, logf LogfFn) (string, error) {
	var reason string
	oldRSs, allOldRSs, newRS, err := GetAllReplicaSets(deployment, c)
	if err != nil {
		return "", err
	}
	if newRS == nil {
		// 新的 RC 还没有创建。
		reason = "new replica set hasn't been created yet"
		return reason, nil
	}
	allRSs := append(oldRSs, newRS)
	// 新旧 ReplicaSet 都必须包含 pod-template-hash 标签
	for i := range allRSs {
		if !labelsutil.SelectorHasLabel(allRSs[i].Spec.Selector, apps.DefaultDeploymentUniqueLabelKey) {
			reason = "all replica sets need to contain the pod-template-hash label"
			return reason, nil
		}
	}

	// 检查 max surge 和 min available
	totalCreated := deploymentutil.GetReplicaCountForReplicaSets(allRSs)
	maxCreated := *(deployment.Spec.Replicas) + deploymentutil.MaxSurge(*deployment)
	if totalCreated > maxCreated {
		LogReplicaSetsOfDeployment(deployment, allOldRSs, newRS, logf)
		LogPodsOfDeployment(c, deployment, allRSs, logf)
		return "", fmt.Errorf("total pods created: %d, more than the max allowed: %d", totalCreated, maxCreated)
	}
	minAvailable := deploymentutil.MinAvailable(deployment)
	if deployment.Status.AvailableReplicas < minAvailable {
		LogReplicaSetsOfDeployment(deployment, allOldRSs, newRS, logf)
		LogPodsOfDeployment(c, deployment, allRSs, logf)
		return "", fmt.Errorf("total pods available: %d, less than the min required: %d", deployment.Status.AvailableReplicas, minAvailable)
	}
	return "", nil
}

// GetAllReplicaSets 返回给定 Deployment 对应的新旧 replica set。
// 它从 client 接口获取 PodList 和 ReplicaSetList。
// 注意，第一组旧 replica set 不包含没有 pod 的那些，
// 而第二组旧 replica set 包含所有旧 replica set。
// 第三个返回值是新的 replica set，如果它还不存在，则可能为 nil。
func GetAllReplicaSets(deployment *apps.Deployment, c clientset.Interface) ([]*apps.ReplicaSet, []*apps.ReplicaSet, *apps.ReplicaSet, error) {
	rsList, err := deploymentutil.ListReplicaSets(deployment, deploymentutil.RsListFromClient(c.AppsV1()))
	if err != nil {
		return nil, nil, nil, err
	}
	oldRSes, allOldRSes := deploymentutil.FindOldReplicaSets(deployment, rsList)
	newRS := deploymentutil.FindNewReplicaSet(deployment, rsList)
	return oldRSes, allOldRSes, newRS, nil
}

// GetOldReplicaSets 返回给定 Deployment 对应的旧 replica set；
// 它从 client 接口获取 PodList 和 ReplicaSetList。
// 注意，第一组旧 replica set 不包含没有 pod 的那些，
// 而第二组旧 replica set 包含所有旧 replica set。
func GetOldReplicaSets(deployment *apps.Deployment, c clientset.Interface) ([]*apps.ReplicaSet, []*apps.ReplicaSet, error) {
	rsList, err := deploymentutil.ListReplicaSets(deployment, deploymentutil.RsListFromClient(c.AppsV1()))
	if err != nil {
		return nil, nil, err
	}
	oldRSes, allOldRSes := deploymentutil.FindOldReplicaSets(deployment, rsList)
	return oldRSes, allOldRSes, nil
}

// GetNewReplicaSet 返回符合给定 deployment 意图的 replica set；
// 它从 client 接口获取 ReplicaSetList。
// 如果新的 replica set 还不存在，则返回 nil。
func GetNewReplicaSet(deployment *apps.Deployment, c clientset.Interface) (*apps.ReplicaSet, error) {
	rsList, err := deploymentutil.ListReplicaSets(deployment, deploymentutil.RsListFromClient(c.AppsV1()))
	if err != nil {
		return nil, err
	}
	return deploymentutil.FindNewReplicaSet(deployment, rsList), nil
}

// 等待 deployment 完成，并检查滚动更新策略在任何时刻都没有被破坏。
// 在滚动更新期间不应破坏滚动更新策略。
func WaitForDeploymentCompleteAndCheckRolling(c clientset.Interface, d *apps.Deployment, logf LogfFn, pollInterval, pollTimeout time.Duration) error {
	rolling := true
	return waitForDeploymentCompleteMaybeCheckRolling(c, d, rolling, logf, pollInterval, pollTimeout)
}

// 等待 deployment 完成，但不检查滚动更新策略是否被破坏。
// 滚动更新策略只在滚动更新期间适用，在其他情况下可能被违反，
// 例如扩缩容事件后不久，或者 deployment 刚创建时。
func WaitForDeploymentComplete(c clientset.Interface, d *apps.Deployment, logf LogfFn, pollInterval, pollTimeout time.Duration) error {
	rolling := false
	return waitForDeploymentCompleteMaybeCheckRolling(c, d, rolling, logf, pollInterval, pollTimeout)
}

// WaitForDeploymentRevisionAndImage 等待 deployment 及其新 RS 的 revision 和容器镜像
// 与给定的 revision 和镜像相匹配。
// 注意，deployment 的 revision 及其新 RS 的 revision 应当很快更新，
// 因此这里只等待 1 分钟，以便尽早失败。
func WaitForDeploymentRevisionAndImage(c clientset.Interface, ns, deploymentName string, revision, image string, logf LogfFn, pollInterval, pollTimeout time.Duration) error {
	var deployment *apps.Deployment
	var newRS *apps.ReplicaSet
	var reason string
	err := wait.PollImmediate(pollInterval, pollTimeout, func() (bool, error) {
		var err error
		deployment, err = c.AppsV1().Deployments(ns).Get(context.TODO(), deploymentName, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		// 新的 ReplicaSet 必须非 nil，且包含 pod-template-hash 标签
		newRS, err = GetNewReplicaSet(deployment, c)
		if err != nil {
			return false, err
		}
		if err := checkRevisionAndImage(deployment, newRS, revision, image); err != nil {
			reason = err.Error()
			logf(reason)
			return false, nil
		}
		return true, nil
	})
	if wait.Interrupted(err) {
		LogReplicaSetsOfDeployment(deployment, nil, newRS, logf)
		err = errors.New(reason)
	}
	if newRS == nil {
		return fmt.Errorf("deployment %q failed to create new replica set", deploymentName)
	}
	if err != nil {
		if deployment == nil {
			return fmt.Errorf("error creating new replica set for deployment %q: %w", deploymentName, err)
		}
		deploymentImage := ""
		if len(deployment.Spec.Template.Spec.Containers) > 0 {
			deploymentImage = deployment.Spec.Template.Spec.Containers[0].Image
		}
		newRSImage := ""
		if len(newRS.Spec.Template.Spec.Containers) > 0 {
			newRSImage = newRS.Spec.Template.Spec.Containers[0].Image
		}
		return fmt.Errorf("error waiting for deployment %q (got %s / %s) and new replica set %q (got %s / %s) revision and image to match expectation (expected %s / %s): %v", deploymentName, deployment.Annotations[deploymentutil.RevisionAnnotation], deploymentImage, newRS.Name, newRS.Annotations[deploymentutil.RevisionAnnotation], newRSImage, revision, image, err)
	}
	return nil
}

// CheckDeploymentRevisionAndImage 检查输入的 deployment 及其新 replica set 的
// revision 和镜像是否符合预期。
func CheckDeploymentRevisionAndImage(c clientset.Interface, ns, deploymentName, revision, image string) error {
	deployment, err := c.AppsV1().Deployments(ns).Get(context.TODO(), deploymentName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("unable to get deployment %s during revision check: %v", deploymentName, err)
	}

	// 检查该 deployment 的新 replica set 的 revision
	newRS, err := GetNewReplicaSet(deployment, c)
	if err != nil {
		return fmt.Errorf("unable to get new replicaset of deployment %s during revision check: %v", deploymentName, err)
	}
	return checkRevisionAndImage(deployment, newRS, revision, image)
}

func checkRevisionAndImage(deployment *apps.Deployment, newRS *apps.ReplicaSet, revision, image string) error {
	// 新的 ReplicaSet 必须非 nil，且包含 pod-template-hash 标签
	if newRS == nil {
		return fmt.Errorf("new replicaset for deployment %q is yet to be created", deployment.Name)
	}
	if !labelsutil.SelectorHasLabel(newRS.Spec.Selector, apps.DefaultDeploymentUniqueLabelKey) {
		return fmt.Errorf("new replica set %q doesn't have %q label selector", newRS.Name, apps.DefaultDeploymentUniqueLabelKey)
	}
	// 检查该 deployment 的 revision，以及该 deployment 的新 replica set 的 revision
	if deployment.Annotations == nil || deployment.Annotations[deploymentutil.RevisionAnnotation] != revision {
		return fmt.Errorf("deployment %q doesn't have the required revision set", deployment.Name)
	}
	if newRS.Annotations == nil || newRS.Annotations[deploymentutil.RevisionAnnotation] != revision {
		return fmt.Errorf("new replicaset %q doesn't have the required revision set", newRS.Name)
	}
	// 检查该 deployment 的镜像，以及该 deployment 的新 replica set 的镜像
	if !containsImage(deployment.Spec.Template.Spec.Containers, image) {
		return fmt.Errorf("deployment %q doesn't have the required image %s set", deployment.Name, image)
	}
	if !containsImage(newRS.Spec.Template.Spec.Containers, image) {
		return fmt.Errorf("new replica set %q doesn't have the required image %s.", newRS.Name, image)
	}
	return nil
}

func containsImage(containers []v1.Container, imageName string) bool {
	for _, container := range containers {
		if container.Image == imageName {
			return true
		}
	}
	return false
}

type UpdateDeploymentFunc func(d *apps.Deployment)

func UpdateDeploymentWithRetries(c clientset.Interface, namespace, name string, applyUpdate UpdateDeploymentFunc, logf LogfFn, pollInterval, pollTimeout time.Duration) (*apps.Deployment, error) {
	var deployment *apps.Deployment
	var updateErr error
	pollErr := wait.PollImmediate(pollInterval, pollTimeout, func() (bool, error) {
		var err error
		if deployment, err = c.AppsV1().Deployments(namespace).Get(context.TODO(), name, metav1.GetOptions{}); err != nil {
			return false, err
		}
		// 应用更新，然后尝试将其推送到 apiserver。
		applyUpdate(deployment)
		if deployment, err = c.AppsV1().Deployments(namespace).Update(context.TODO(), deployment, metav1.UpdateOptions{}); err == nil {
			logf("Updating deployment %s", name)
			return true, nil
		}
		updateErr = err
		return false, nil
	})
	if wait.Interrupted(pollErr) {
		pollErr = fmt.Errorf("couldn't apply the provided updated to deployment %q: %v", name, updateErr)
	}
	return deployment, pollErr
}

func WaitForObservedDeployment(c clientset.Interface, ns, deploymentName string, desiredGeneration int64) error {
	return deploymentutil.WaitForObservedDeployment(func() (*apps.Deployment, error) {
		return c.AppsV1().Deployments(ns).Get(context.TODO(), deploymentName, metav1.GetOptions{})
	}, desiredGeneration, 2*time.Second, 1*time.Minute)
}

// WaitForDeploymentUpdatedReplicasGTE 等待给定 deployment 被 controller 观察到，
// 并且至少有一定数量的 updatedReplicas
func WaitForDeploymentUpdatedReplicasGTE(c clientset.Interface, ns, deploymentName string, minUpdatedReplicas int32, desiredGeneration int64, pollInterval, pollTimeout time.Duration) error {
	var deployment *apps.Deployment
	err := wait.PollImmediate(pollInterval, pollTimeout, func() (bool, error) {
		d, err := c.AppsV1().Deployments(ns).Get(context.TODO(), deploymentName, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		deployment = d
		return deployment.Status.ObservedGeneration >= desiredGeneration && deployment.Status.UpdatedReplicas >= minUpdatedReplicas, nil
	})
	if err != nil {
		return fmt.Errorf("error waiting for deployment %q to have at least %d updatedReplicas: %v; latest .status.updatedReplicas: %d", deploymentName, minUpdatedReplicas, err, deployment.Status.UpdatedReplicas)
	}
	return nil
}

func WaitForDeploymentWithCondition(c clientset.Interface, ns, deploymentName, reason string, condType apps.DeploymentConditionType, logf LogfFn, pollInterval, pollTimeout time.Duration) error {
	var deployment *apps.Deployment
	pollErr := wait.PollImmediate(pollInterval, pollTimeout, func() (bool, error) {
		d, err := c.AppsV1().Deployments(ns).Get(context.TODO(), deploymentName, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		deployment = d
		cond := deploymentutil.GetDeploymentCondition(deployment.Status, condType)
		return cond != nil && cond.Reason == reason, nil
	})
	if wait.Interrupted(pollErr) {
		pollErr = fmt.Errorf("deployment %q never updated with the desired condition and reason, latest deployment conditions: %+v", deployment.Name, deployment.Status.Conditions)
		_, allOldRSs, newRS, err := GetAllReplicaSets(deployment, c)
		if err == nil {
			LogReplicaSetsOfDeployment(deployment, allOldRSs, newRS, logf)
			LogPodsOfDeployment(c, deployment, append(allOldRSs, newRS), logf)
		}
	}
	return pollErr
}
