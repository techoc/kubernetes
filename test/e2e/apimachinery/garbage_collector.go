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

package apimachinery

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensionsclientset "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	apiextensionstestserver "k8s.io/apiextensions-apiserver/test/integration/fixtures"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/storage/names"
	clientset "k8s.io/client-go/kubernetes"
	clientv1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/kubernetes/test/e2e/framework"
	e2edeployment "k8s.io/kubernetes/test/e2e/framework/deployment"
	e2emetrics "k8s.io/kubernetes/test/e2e/framework/metrics"
	e2enode "k8s.io/kubernetes/test/e2e/framework/node"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	imageutils "k8s.io/kubernetes/test/utils/image"
	admissionapi "k8s.io/pod-security-admission/api"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

// estimateMaximumPods 估算集群能容纳的 pod 数，并留出余量，
// 避免因 pod 数量触顶而无法调度。
// 调用方应使用 framework.WithSerial()，因为这些测试会消耗大量 pod，
// 并发运行并不安全。
func estimateMaximumPods(ctx context.Context, c clientset.Interface, min, max int32) int32 {
	nodes, err := e2enode.GetReadySchedulableNodes(ctx, c)
	framework.ExpectNoError(err)

	availablePods := int32(0)
	// 为每个 node 上的非测试 pod 预留合理的余量
	const daemonSetReservedPods = 10
	for _, node := range nodes.Items {
		if q, ok := node.Status.Allocatable["pods"]; ok {
			if num, ok := q.AsInt64(); ok {
				if num > daemonSetReservedPods {
					availablePods += int32(num - daemonSetReservedPods)
				}
				continue
			}
		}
		// 只有在取不到数值时才退回按 node 估算：默认 maxPerCore 为 10，
		// 而大多数 node 至少有一个 core
		availablePods += 10
	}
	// 不按上限满额创建 pod，留出一半余量
	availablePods = int32(float32(availablePods) * 0.5)
	// 限制上下界
	if availablePods > max {
		availablePods = max
	}
	if availablePods < min {
		availablePods = min
	}
	return availablePods
}

func getForegroundOptions() metav1.DeleteOptions {
	policy := metav1.DeletePropagationForeground
	return metav1.DeleteOptions{PropagationPolicy: &policy}
}

func getBackgroundOptions() metav1.DeleteOptions {
	policy := metav1.DeletePropagationBackground
	return metav1.DeleteOptions{PropagationPolicy: &policy}
}

func getOrphanOptions() metav1.DeleteOptions {
	policy := metav1.DeletePropagationOrphan
	return metav1.DeleteOptions{PropagationPolicy: &policy}
}

var (
	zero       = int64(0)
	lablecount atomic.Int64
)

const (
	// GC controller 会周期性重新发现可用 API，并同步这些资源的 informer。
	// 若此前可用的 API 在同步过程中被移除，同步会失败并需要重试。
	//
	// e2e 运行中并行测试会增删 API 资源（创建/删除 CRD 和聚合 API），
	// 因此 GC 很可能需要在某个时刻重试 informer 同步。
	//
	// 该超时覆盖两个 resync/重试周期，应加到 wait 超时中，
	// 以容纳其他测试的 API 变更给 GC controller 带来的延迟。
	gcInformerResyncRetryTimeout = time.Minute

	// 这些测试中的许多操作按副本执行，可能需要 100 次变更请求。
	// controller 默认 client QPS 为 5，qps 打满时完成 100 次请求需要 20 秒。
	// e2e 测试并行运行，controller 可能忙着处理其他测试。
	replicaSyncTimeout = 2 * time.Minute
)

func getPodTemplateSpec(labels map[string]string) v1.PodTemplateSpec {
	return v1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels: labels,
		},
		Spec: v1.PodSpec{
			TerminationGracePeriodSeconds: &zero,
			Containers: []v1.Container{
				{
					Name:  "nginx",
					Image: imageutils.GetE2EImage(imageutils.Nginx),
				},
			},
		},
	}
}

func newOwnerDeployment(f *framework.Framework, deploymentName string, labels map[string]string) *appsv1.Deployment {
	return e2edeployment.NewDeployment(deploymentName, 2, labels, "nginx", imageutils.GetE2EImage(imageutils.Nginx), appsv1.RollingUpdateDeploymentStrategyType)
}

func newOwnerRC(f *framework.Framework, name string, replicas int32, labels map[string]string) *v1.ReplicationController {
	template := getPodTemplateSpec(labels)
	return &v1.ReplicationController{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ReplicationController",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: f.Namespace.Name,
			Name:      name,
		},
		Spec: v1.ReplicationControllerSpec{
			Replicas: &replicas,
			Selector: labels,
			Template: &template,
		},
	}
}

func newGCPod(name string) *v1.Pod {
	return &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: v1.PodSpec{
			TerminationGracePeriodSeconds: new(int64),
			Containers: []v1.Container{
				{
					Name:  "nginx",
					Image: imageutils.GetE2EImage(imageutils.Nginx),
				},
			},
		},
	}
}

// verifyRemainingObjects 校验剩余对象数量是否符合预期。
// 与 API server 通信失败时返回 error。
func verifyRemainingObjects(ctx context.Context, f *framework.Framework, objects map[string]int) (bool, error) {
	var ret = true

	for object, num := range objects {
		switch object {
		case "Pods":
			pods, err := f.ClientSet.CoreV1().Pods(f.Namespace.Name).List(ctx, metav1.ListOptions{})
			if err != nil {
				return false, fmt.Errorf("failed to list pods: %w", err)
			}
			if len(pods.Items) != num {
				ret = false
				ginkgo.By(fmt.Sprintf("expected %d pods, got %d pods", num, len(pods.Items)))
			}
		case "Deployments":
			deployments, err := f.ClientSet.AppsV1().Deployments(f.Namespace.Name).List(ctx, metav1.ListOptions{})
			if err != nil {
				return false, fmt.Errorf("failed to list deployments: %w", err)
			}
			if len(deployments.Items) != num {
				ret = false
				ginkgo.By(fmt.Sprintf("expected %d Deployments, got %d Deployments", num, len(deployments.Items)))
			}
		case "ReplicaSets":
			rs, err := f.ClientSet.AppsV1().ReplicaSets(f.Namespace.Name).List(ctx, metav1.ListOptions{})
			if err != nil {
				return false, fmt.Errorf("failed to list rs: %w", err)
			}
			if len(rs.Items) != num {
				ret = false
				ginkgo.By(fmt.Sprintf("expected %d rs, got %d rs", num, len(rs.Items)))
			}
		case "ReplicationControllers":
			rcs, err := f.ClientSet.CoreV1().ReplicationControllers(f.Namespace.Name).List(ctx, metav1.ListOptions{})
			if err != nil {
				return false, fmt.Errorf("failed to list replication controllers: %w", err)
			}
			if len(rcs.Items) != num {
				ret = false
				ginkgo.By(fmt.Sprintf("expected %d RCs, got %d RCs", num, len(rcs.Items)))
			}
		case "CronJobs":
			cronJobs, err := f.ClientSet.BatchV1().CronJobs(f.Namespace.Name).List(ctx, metav1.ListOptions{})
			if err != nil {
				return false, fmt.Errorf("failed to list cronjobs: %w", err)
			}
			if len(cronJobs.Items) != num {
				ret = false
				ginkgo.By(fmt.Sprintf("expected %d cronjobs, got %d cronjobs", num, len(cronJobs.Items)))
			}
		case "Jobs":
			jobs, err := f.ClientSet.BatchV1().Jobs(f.Namespace.Name).List(ctx, metav1.ListOptions{})
			if err != nil {
				return false, fmt.Errorf("failed to list jobs: %w", err)
			}
			if len(jobs.Items) != num {
				ret = false
				ginkgo.By(fmt.Sprintf("expected %d jobs, got %d jobs", num, len(jobs.Items)))
			}
		default:
			return false, fmt.Errorf("object %s is not supported", object)
		}
	}

	return ret, nil
}

func gatherMetrics(ctx context.Context, f *framework.Framework) {
	ginkgo.By("Gathering metrics")
	var summary framework.TestDataSummary
	grabber, err := e2emetrics.NewMetricsGrabber(ctx, f.ClientSet, f.KubemarkExternalClusterClientSet, f.ClientConfig(), false, false, true, false, false, false)
	if err != nil {
		framework.Logf("Failed to create MetricsGrabber. Skipping metrics gathering.")
	} else {
		received, err := grabber.Grab(ctx)
		if err != nil {
			framework.Logf("MetricsGrabber failed grab metrics. Skipping metrics gathering.")
		} else {
			summary = (*e2emetrics.ComponentCollection)(&received)
			framework.Logf("%s", summary.PrintHumanReadable())
		}
	}
}

func newCronJob(name, schedule string) *batchv1.CronJob {
	parallelism := int32(1)
	completions := int32(1)
	return &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		TypeMeta: metav1.TypeMeta{
			Kind: "CronJob",
		},
		Spec: batchv1.CronJobSpec{
			Schedule: schedule,
			JobTemplate: batchv1.JobTemplateSpec{
				Spec: batchv1.JobSpec{
					Parallelism: &parallelism,
					Completions: &completions,
					Template: v1.PodTemplateSpec{
						Spec: v1.PodSpec{
							RestartPolicy:                 v1.RestartPolicyOnFailure,
							TerminationGracePeriodSeconds: &zero,
							Containers: []v1.Container{
								{
									Name:    "c",
									Image:   imageutils.GetE2EImage(imageutils.BusyBox),
									Command: []string{"sleep", "300"},
								},
							},
						},
					},
				},
			},
		},
	}
}

// getUniqLabel 基于 labeLkey 和 labelvalue 生成唯一的 UniqLabel。
func getUniqLabel(labelkey, labelvalue string) map[string]string {
	count := lablecount.Add(1)
	uniqlabelkey := fmt.Sprintf("%s-%05d", labelkey, count)
	uniqlabelvalue := fmt.Sprintf("%s-%05d", labelvalue, count)
	return map[string]string{uniqlabelkey: uniqlabelvalue}
}

var _ = SIGDescribe("Garbage collector", func() {
	f := framework.NewDefaultFramework("gc")
	f.NamespacePodSecurityLevel = admissionapi.LevelBaseline

	/*
		Release: v1.9
		Testname: Garbage Collector, delete replication controller, propagation policy background
		Description: 创建带 2 个 Pod 的 replication controller。RC 和第一个 Pod 创建完成后，使用 deleteOptions.PropagationPolicy=Background 删除 RC。删除 RC MUST 同时删除由该 RC 创建的 pod。
	*/
	framework.ConformanceIt("should delete pods created by rc when not orphaning", func(ctx context.Context) {
		clientSet := f.ClientSet
		rcClient := clientSet.CoreV1().ReplicationControllers(f.Namespace.Name)
		podClient := clientSet.CoreV1().Pods(f.Namespace.Name)
		rcName := "simpletest.rc"
		uniqLabels := getUniqLabel("gctest", "delete_pods")
		rc := newOwnerRC(f, rcName, 2, uniqLabels)
		ginkgo.By("create the rc")
		rc, err := rcClient.Create(ctx, rc, metav1.CreateOptions{})
		if err != nil {
			framework.Failf("Failed to create replication controller: %v", err)
		}
		// 等待 rc 创建出一些 pod
		if err := wait.Poll(5*time.Second, 30*time.Second, func() (bool, error) {
			pods, err := podClient.List(ctx, metav1.ListOptions{})
			if err != nil {
				return false, fmt.Errorf("failed to list pods: %w", err)
			}
			// 故意不等待 pod 数达到 rc.Spec.Replicas：要在 rc 达到稳定前就删除它，
			// 以验证 garbage collector 和 rc manager 能否正确处理
			if len(pods.Items) > 0 {
				return true, nil
			}
			return false, nil

		}); err != nil {
			framework.Failf("failed to wait for the rc to create some pods: %v", err)
		}
		ginkgo.By("delete the rc")
		deleteOptions := getBackgroundOptions()
		deleteOptions.Preconditions = metav1.NewUIDPreconditions(string(rc.UID))
		if err := rcClient.Delete(ctx, rc.ObjectMeta.Name, deleteOptions); err != nil {
			framework.Failf("failed to delete the rc: %v", err)
		}
		ginkgo.By("wait for all pods to be garbage collected")
		// 等待 RC 和 Pod 数量达到预期
		if err := wait.PollUntilContextTimeout(ctx, 5*time.Second, (60*time.Second)+gcInformerResyncRetryTimeout, false, func(ctx context.Context) (bool, error) {
			objects := map[string]int{"ReplicationControllers": 0, "Pods": 0}
			return verifyRemainingObjects(ctx, f, objects)
		}); err != nil {
			framework.Failf("failed to wait for all pods to be deleted: %v", err)
			remainingPods, err := podClient.List(ctx, metav1.ListOptions{})
			if err != nil {
				framework.Failf("failed to list pods post mortem: %v", err)
			} else {
				framework.Failf("remaining pods are: %#v", remainingPods)
			}
		}
		gatherMetrics(ctx, f)
	})

	/*
		Release: v1.9
		Testname: Garbage Collector, delete replication controller, propagation policy orphan
		Description: 创建副本数在 10 到 100 之间、不超过可分配 pod 上限的 replication controller。RC 和所有 Pod 创建完成后，使用 deleteOptions.PropagationPolicy=Orphan 删除 RC。删除 RC MUST 使该 RC 创建的 pod 成为孤儿。
	*/
	framework.ConformanceIt("should orphan pods created by rc if delete options say so", framework.WithSerial(), func(ctx context.Context) {
		clientSet := f.ClientSet
		rcClient := clientSet.CoreV1().ReplicationControllers(f.Namespace.Name)
		podClient := clientSet.CoreV1().Pods(f.Namespace.Name)
		rcName := "simpletest.rc"
		uniqLabels := getUniqLabel("gctest", "orphan_pods")
		rc := newOwnerRC(f, rcName, estimateMaximumPods(ctx, clientSet, 10, 100), uniqLabels)
		ginkgo.By("create the rc")
		rc, err := rcClient.Create(ctx, rc, metav1.CreateOptions{})
		if err != nil {
			framework.Failf("Failed to create replication controller: %v", err)
		}
		// 等待 rc 创建 pod
		waitForReplicas(ctx, rc, rcClient)

		ginkgo.By("delete the rc")
		deleteOptions := getOrphanOptions()
		deleteOptions.Preconditions = metav1.NewUIDPreconditions(string(rc.UID))
		if err := rcClient.Delete(ctx, rc.ObjectMeta.Name, deleteOptions); err != nil {
			framework.Failf("failed to delete the rc: %v", err)
		}
		ginkgo.By("wait for the rc to be deleted")
		// 将 100 个 pod 变为孤儿需要 100 次 PATCH。client 默认 qps 为 5，
		// qps 打满时需要 20 秒。但 apiserver 完成一次 PATCH 需要数百毫秒，
		// 而 gc 单线程发送 patch，实际 qps 低于 5；加上 e2e 并行运行时
		// GC controller 可能被其他测试牵制。根据测试日志，120 秒足够。
		if err := wait.PollUntilContextTimeout(ctx, 5*time.Second, 120*time.Second+gcInformerResyncRetryTimeout, false, func(ctx context.Context) (bool, error) {
			rcs, err := rcClient.List(ctx, metav1.ListOptions{})
			if err != nil {
				return false, fmt.Errorf("failed to list rcs: %w", err)
			}
			if len(rcs.Items) != 0 {
				return false, nil
			}
			return true, nil
		}); err != nil {
			framework.Failf("%v", err)
		}
		ginkgo.By("wait for 30 seconds to see if the garbage collector mistakenly deletes the pods")
		time.Sleep(30 * time.Second)
		pods, err := podClient.List(ctx, metav1.ListOptions{})
		if err != nil {
			framework.Failf("Failed to list pods: %v", err)
		}
		if e, a := int(*(rc.Spec.Replicas)), len(pods.Items); e != a {
			framework.Failf("expect %d pods, got %d pods", e, a)
		}
		gatherMetrics(ctx, f)
		if err = e2epod.DeletePodsWithGracePeriod(ctx, clientSet, pods.Items, 0); err != nil {
			framework.Logf("WARNING: failed to delete pods: %v", err)
		}
	})

	// deleteOptions.OrphanDependents 自 1.7 起废弃，推荐使用 PropagationPolicy。
	// 未来晋升为一致性测试的讨论见 https://github.com/kubernetes/kubernetes/issues/65427。
	ginkgo.It("should orphan pods created by rc if deleteOptions.OrphanDependents is nil", func(ctx context.Context) {
		clientSet := f.ClientSet
		rcClient := clientSet.CoreV1().ReplicationControllers(f.Namespace.Name)
		podClient := clientSet.CoreV1().Pods(f.Namespace.Name)
		rcName := "simpletest.rc"
		uniqLabels := getUniqLabel("gctest", "orphan_pods_nil_option")
		rc := newOwnerRC(f, rcName, 2, uniqLabels)
		ginkgo.By("create the rc")
		rc, err := rcClient.Create(ctx, rc, metav1.CreateOptions{})
		if err != nil {
			framework.Failf("Failed to create replication controller: %v", err)
		}
		// 等待 rc 创建出一些 pod
		waitForReplicas(ctx, rc, rcClient)

		ginkgo.By("delete the rc")
		deleteOptions := metav1.DeleteOptions{
			Preconditions: metav1.NewUIDPreconditions(string(rc.UID)),
		}
		if err := rcClient.Delete(ctx, rc.ObjectMeta.Name, deleteOptions); err != nil {
			framework.Failf("failed to delete the rc: %v", err)
		}
		ginkgo.By("wait for 30 seconds to see if the garbage collector mistakenly deletes the pods")
		time.Sleep(30 * time.Second)
		pods, err := podClient.List(ctx, metav1.ListOptions{})
		if err != nil {
			framework.Failf("Failed to list pods: %v", err)
		}
		if e, a := int(*(rc.Spec.Replicas)), len(pods.Items); e != a {
			framework.Failf("expect %d pods, got %d pods", e, a)
		}
		gatherMetrics(ctx, f)
		if err = e2epod.DeletePodsWithGracePeriod(ctx, clientSet, pods.Items, 0); err != nil {
			framework.Logf("WARNING: failed to delete pods: %v", err)
		}
	})

	/*
		Release: v1.9
		Testname: Garbage Collector, delete deployment,  propagation policy background
		Description: 创建带 replicaset 的 deployment。replicaset 创建完成后，使用 deleteOptions.PropagationPolicy=Background 删除 deployment。删除 deployment MUST 同时删除其创建的 replicaset 和属于该 deployment 的 pod。
	*/
	framework.ConformanceIt("should delete RS created by deployment when not orphaning", func(ctx context.Context) {
		clientSet := f.ClientSet
		deployClient := clientSet.AppsV1().Deployments(f.Namespace.Name)
		rsClient := clientSet.AppsV1().ReplicaSets(f.Namespace.Name)
		deploymentName := "simpletest.deployment"
		uniqLabels := getUniqLabel("gctest", "delete_rs")
		deployment := newOwnerDeployment(f, deploymentName, uniqLabels)
		ginkgo.By("create the deployment")
		createdDeployment, err := deployClient.Create(ctx, deployment, metav1.CreateOptions{})
		if err != nil {
			framework.Failf("Failed to create deployment: %v", err)
		}
		// 等待 deployment 创建出一些 rs
		ginkgo.By("Wait for the Deployment to create new ReplicaSet")
		err = wait.PollUntilContextTimeout(ctx, 500*time.Millisecond, 1*time.Minute, true, func(ctx context.Context) (bool, error) {
			rsList, err := rsClient.List(ctx, metav1.ListOptions{})
			if err != nil {
				return false, fmt.Errorf("failed to list rs: %w", err)
			}
			return len(rsList.Items) > 0, nil

		})
		if err != nil {
			framework.Failf("Failed to wait for the Deployment to create some ReplicaSet: %v", err)
		}

		ginkgo.By("delete the deployment")
		deleteOptions := getBackgroundOptions()
		deleteOptions.Preconditions = metav1.NewUIDPreconditions(string(createdDeployment.UID))
		if err := deployClient.Delete(ctx, deployment.ObjectMeta.Name, deleteOptions); err != nil {
			framework.Failf("failed to delete the deployment: %v", err)
		}
		ginkgo.By("wait for all rs to be garbage collected")
		err = wait.PollUntilContextTimeout(ctx, 500*time.Millisecond, 1*time.Minute+gcInformerResyncRetryTimeout, true, func(ctx context.Context) (bool, error) {
			objects := map[string]int{"Deployments": 0, "ReplicaSets": 0, "Pods": 0}
			return verifyRemainingObjects(ctx, f, objects)
		})
		if err != nil {
			errList := make([]error, 0)
			errList = append(errList, err)
			remainingRSs, err := rsClient.List(ctx, metav1.ListOptions{})
			if err != nil {
				errList = append(errList, fmt.Errorf("failed to list RSs post mortem: %w", err))
			} else {
				errList = append(errList, fmt.Errorf("remaining rs are: %#v", remainingRSs))
			}
			aggregatedError := utilerrors.NewAggregate(errList)
			framework.Failf("Failed to wait for all rs to be garbage collected: %v", aggregatedError)

		}

		gatherMetrics(ctx, f)
	})

	/*
		Release: v1.9
		Testname: Garbage Collector, delete deployment, propagation policy orphan
		Description: 创建带 replicaset 的 deployment。replicaset 创建完成后，使用 deleteOptions.PropagationPolicy=Orphan 删除 deployment。删除 deployment MUST 使其创建的 replicaset 和 pod 都成为孤儿。
	*/
	framework.ConformanceIt("should orphan RS created by deployment when deleteOptions.PropagationPolicy is Orphan", func(ctx context.Context) {
		clientSet := f.ClientSet
		deployClient := clientSet.AppsV1().Deployments(f.Namespace.Name)
		rsClient := clientSet.AppsV1().ReplicaSets(f.Namespace.Name)
		deploymentName := "simpletest.deployment"
		uniqLabels := getUniqLabel("gctest", "orphan_rs")
		deployment := newOwnerDeployment(f, deploymentName, uniqLabels)
		ginkgo.By("create the deployment")
		createdDeployment, err := deployClient.Create(ctx, deployment, metav1.CreateOptions{})
		if err != nil {
			framework.Failf("Failed to create deployment: %v", err)
		}
		// 等待 deployment 创建出一些 rs
		ginkgo.By("Wait for the Deployment to create new ReplicaSet")
		var replicaset appsv1.ReplicaSet
		err = wait.PollUntilContextTimeout(ctx, 500*time.Millisecond, 1*time.Minute, true, func(ctx context.Context) (bool, error) {
			rsList, err := rsClient.List(ctx, metav1.ListOptions{})
			if err != nil {
				return false, fmt.Errorf("failed to list rs: %w", err)
			}
			if len(rsList.Items) > 0 {
				replicaset = rsList.Items[0]
				return true, nil
			}
			return false, nil

		})
		if err != nil {
			framework.Failf("Failed to wait for the Deployment to create some ReplicaSet: %v", err)
		}

		desiredGeneration := replicaset.Generation
		if err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 60*time.Second, true, func(ctx context.Context) (bool, error) {
			newRS, err := clientSet.AppsV1().ReplicaSets(replicaset.Namespace).Get(ctx, replicaset.Name, metav1.GetOptions{})
			if err != nil {
				return false, err
			}
			return newRS.Status.ObservedGeneration >= desiredGeneration && newRS.Status.Replicas == *replicaset.Spec.Replicas, nil
		}); err != nil {
			framework.Failf("failed to verify .Status.Replicas is equal to .Spec.Replicas for replicaset %q: %v", replicaset.Name, err)
		}

		ginkgo.By("delete the deployment")
		deleteOptions := getOrphanOptions()
		deleteOptions.Preconditions = metav1.NewUIDPreconditions(string(createdDeployment.UID))
		if err := deployClient.Delete(ctx, deployment.ObjectMeta.Name, deleteOptions); err != nil {
			framework.Failf("failed to delete the deployment: %v", err)
		}
		ginkgo.By("wait for deployment deletion to see if the garbage collector mistakenly deletes the rs")
		err = wait.PollUntilContextTimeout(ctx, 500*time.Millisecond, 1*time.Minute+gcInformerResyncRetryTimeout, true, func(ctx context.Context) (bool, error) {
			dList, err := deployClient.List(ctx, metav1.ListOptions{})
			if err != nil {
				return false, fmt.Errorf("failed to list deployments: %w", err)
			}
			return len(dList.Items) == 0, nil
		})
		if err != nil {
			framework.Failf("Failed to wait for the Deployment to be deleted: %v", err)
		}
		// deployment 对象消失即表明 GC 已完成所有相关动作
		objects := map[string]int{"Deployments": 0, "ReplicaSets": 1, "Pods": 2}
		ok, err := verifyRemainingObjects(ctx, f, objects)
		if err != nil {
			framework.Failf("Unexpected error while verifying remaining deployments, rs, and pods: %v", err)
		}
		if !ok {
			errList := make([]error, 0)
			remainingRSs, err := rsClient.List(ctx, metav1.ListOptions{})
			if err != nil {
				errList = append(errList, fmt.Errorf("failed to list RSs post mortem: %w", err))
			} else {
				errList = append(errList, fmt.Errorf("remaining rs post mortem: %#v", remainingRSs))
			}
			remainingDSs, err := deployClient.List(ctx, metav1.ListOptions{})
			if err != nil {
				errList = append(errList, fmt.Errorf("failed to list Deployments post mortem: %w", err))
			} else {
				errList = append(errList, fmt.Errorf("remaining deployment's post mortem: %#v", remainingDSs))
			}
			aggregatedError := utilerrors.NewAggregate(errList)
			framework.Failf("Failed to verify remaining deployments, rs, and pods: %v", aggregatedError)
		}
		rs, err := clientSet.AppsV1().ReplicaSets(f.Namespace.Name).List(ctx, metav1.ListOptions{})
		if err != nil {
			framework.Failf("Failed to list ReplicaSet %v", err)
		}
		for _, replicaSet := range rs.Items {
			if metav1.GetControllerOf(&replicaSet.ObjectMeta) != nil {
				framework.Failf("Found ReplicaSet with non nil ownerRef %v", replicaSet)
			}
		}

		gatherMetrics(ctx, f)
	})

	/*
		Release: v1.9
		Testname: Garbage Collector, delete replication controller, after owned pods
		Description: 创建副本数在 10 到 100 之间、不超过可分配 pod 上限的 replication controller。RC 和所有 Pod 创建完成后，使用 deleteOptions.PropagationPolicy=Foreground 删除 RC。该 RC 创建的 pod MUST 在 RC 被删除之前先删除。
	*/
	framework.ConformanceIt("should keep the rc around until all its pods are deleted if the deleteOptions says so", framework.WithSerial(), func(ctx context.Context) {
		clientSet := f.ClientSet
		rcClient := clientSet.CoreV1().ReplicationControllers(f.Namespace.Name)
		podClient := clientSet.CoreV1().Pods(f.Namespace.Name)
		rcName := "simpletest.rc"
		uniqLabels := getUniqLabel("gctest", "delete_pods_foreground")
		rc := newOwnerRC(f, rcName, estimateMaximumPods(ctx, clientSet, 10, 100), uniqLabels)
		ginkgo.By("create the rc")
		rc, err := rcClient.Create(ctx, rc, metav1.CreateOptions{})
		if err != nil {
			framework.Failf("Failed to create replication controller: %v", err)
		}
		// 等待 rc 创建 pod
		waitForReplicas(ctx, rc, rcClient)

		ginkgo.By("delete the rc")
		deleteOptions := getForegroundOptions()
		deleteOptions.Preconditions = metav1.NewUIDPreconditions(string(rc.UID))
		if err := rcClient.Delete(ctx, rc.ObjectMeta.Name, deleteOptions); err != nil {
			framework.Failf("failed to delete the rc: %v", err)
		}
		ginkgo.By("wait for the rc to be deleted")
		// client 默认 QPS 为 20，删除每个 pod 需要 2 次请求，30 秒应足够
		// TODO: 30 秒仅在 owner 删除后立即处理 dependent 时够用；实际中二者之间
		// 可能有较长延迟。目前先增大超时，并调查处理延迟的原因。
		if err := wait.PollUntilContextTimeout(ctx, 1*time.Second, 30*time.Second+gcInformerResyncRetryTimeout, false, func(ctx context.Context) (bool, error) {
			_, err := rcClient.Get(ctx, rc.Name, metav1.GetOptions{})
			if err == nil {
				pods, _ := podClient.List(ctx, metav1.ListOptions{})
				framework.Logf("%d pods remaining", len(pods.Items))
				count := 0
				for _, pod := range pods.Items {
					if pod.ObjectMeta.DeletionTimestamp == nil {
						count++
					}
				}
				framework.Logf("%d pods has nil DeletionTimestamp", count)
				framework.Logf("")
				return false, nil
			}
			if apierrors.IsNotFound(err) {
				return true, nil
			}
			return false, err
		}); err != nil {
			pods, err2 := podClient.List(ctx, metav1.ListOptions{})
			if err2 != nil {
				framework.Failf("%v", err2)
			}
			framework.Logf("%d remaining pods are:", len(pods.Items))
			framework.Logf("The ObjectMeta of the remaining pods are:")
			for _, pod := range pods.Items {
				framework.Logf("%#v", pod.ObjectMeta)
			}
			framework.Failf("failed to delete the rc: %v", err)
		}
		// 此时不应残留任何 pod
		pods, err := podClient.List(ctx, metav1.ListOptions{})
		if err != nil {
			framework.Failf("%v", err)
		}
		if len(pods.Items) != 0 {
			framework.Failf("expected no pods, got %#v", pods)
		}
		gatherMetrics(ctx, f)
	})

	// TODO: 该测试应改为集成测试
	/*
		Release: v1.9
		Testname: Garbage Collector, multiple owners
		Description: 创建 replication controller RC1，副本数在 10 到 100 之间且不超过可分配 pod 上限。再创建 RC2，并将其设为半数副本的 owner。RC1 和所有 Pod 创建完成后，使用 deleteOptions.PropagationPolicy=Foreground 删除 RC1。以 RC2 为 owner 的那部分 pod MUST 不被删除，也不带删除时间戳。删除 RC MUST NOT 删除同时属于多个 replication controller 的 pod。
	*/
	framework.ConformanceIt("should not delete dependents that have both valid owner and owner that's waiting for dependents to be deleted", framework.WithSerial(), func(ctx context.Context) {
		clientSet := f.ClientSet
		rcClient := clientSet.CoreV1().ReplicationControllers(f.Namespace.Name)
		podClient := clientSet.CoreV1().Pods(f.Namespace.Name)
		rc1Name := "simpletest-rc-to-be-deleted"
		replicas := estimateMaximumPods(ctx, clientSet, 10, 100)
		halfReplicas := int(replicas / 2)
		uniqLabelsDeleted := getUniqLabel("gctest_d", "valid_and_pending_owners_d")
		rc1 := newOwnerRC(f, rc1Name, replicas, uniqLabelsDeleted)
		ginkgo.By("create the rc1")
		rc1, err := rcClient.Create(ctx, rc1, metav1.CreateOptions{})
		if err != nil {
			framework.Failf("Failed to create replication controller: %v", err)
		}
		rc2Name := "simpletest-rc-to-stay"
		uniqLabelsStay := getUniqLabel("gctest_s", "valid_and_pending_owners_s")
		rc2 := newOwnerRC(f, rc2Name, 0, uniqLabelsStay)
		ginkgo.By("create the rc2")
		rc2, err = rcClient.Create(ctx, rc2, metav1.CreateOptions{})
		if err != nil {
			framework.Failf("Failed to create replication controller: %v", err)
		}
		// 等待 rc1 稳定
		waitForReplicas(ctx, rc1, rcClient)

		ginkgo.By(fmt.Sprintf("set half of pods created by rc %s to have rc %s as owner as well", rc1Name, rc2Name))
		pods, err := podClient.List(ctx, metav1.ListOptions{})
		framework.ExpectNoError(err, "failed to list pods in namespace: %s", f.Namespace.Name)
		patch := fmt.Sprintf(`{"metadata":{"ownerReferences":[{"apiVersion":"v1","kind":"ReplicationController","name":"%s","uid":"%s"}]}}`, rc2.ObjectMeta.Name, rc2.ObjectMeta.UID)
		for i := range halfReplicas {
			pod := pods.Items[i]
			_, err := podClient.Patch(ctx, pod.Name, types.StrategicMergePatchType, []byte(patch), metav1.PatchOptions{})
			framework.ExpectNoError(err, "failed to apply to pod %s in namespace %s, a strategic merge patch: %s", pod.Name, f.Namespace.Name, patch)
		}

		ginkgo.By(fmt.Sprintf("delete the rc %s", rc1Name))
		deleteOptions := getForegroundOptions()
		deleteOptions.Preconditions = metav1.NewUIDPreconditions(string(rc1.UID))
		if err := rcClient.Delete(ctx, rc1.ObjectMeta.Name, deleteOptions); err != nil {
			framework.Failf("failed to delete the rc: %v", err)
		}
		ginkgo.By("wait for the rc to be deleted")
		// TODO: GC 的周期性 API 重新发现优化后，可缩短该超时。
		// Tracked at https://github.com/kubernetes/kubernetes/issues/50046.
		if err := wait.PollUntilContextTimeout(ctx, 5*time.Second, 90*time.Second, false, func(ctx context.Context) (bool, error) {
			_, err := rcClient.Get(ctx, rc1.Name, metav1.GetOptions{})
			if err == nil {
				pods, _ := podClient.List(ctx, metav1.ListOptions{})
				framework.Logf("%d pods remaining", len(pods.Items))
				count := 0
				for _, pod := range pods.Items {
					if pod.ObjectMeta.DeletionTimestamp == nil {
						count++
					}
				}
				framework.Logf("%d pods has nil DeletionTimestamp", count)
				framework.Logf("")
				return false, nil
			}
			if apierrors.IsNotFound(err) {
				return true, nil
			}
			return false, err
		}); err != nil {
			pods, err2 := podClient.List(ctx, metav1.ListOptions{})
			if err2 != nil {
				framework.Failf("%v", err2)
			}
			framework.Logf("%d remaining pods are:", len(pods.Items))
			framework.Logf("ObjectMeta of remaining pods are:")
			for _, pod := range pods.Items {
				framework.Logf("%#v", pod.ObjectMeta)
			}
			framework.Failf("failed to delete rc %s, err: %v", rc1Name, err)
		}
		// 半数 pod 应仍然存在，
		pods, err = podClient.List(ctx, metav1.ListOptions{})
		if err != nil {
			framework.Failf("%v", err)
		}
		if len(pods.Items) != halfReplicas {
			framework.Failf("expected %d pods, got %d", halfReplicas, len(pods.Items))
		}
		for _, pod := range pods.Items {
			if pod.ObjectMeta.DeletionTimestamp != nil {
				framework.Failf("expected pod DeletionTimestamp to be nil, got %#v", pod.ObjectMeta)
			}
			// 它们应只剩 1 个 ownerReference
			if len(pod.ObjectMeta.OwnerReferences) != 1 {
				framework.Failf("expected pod to only have 1 owner, got %#v", pod.ObjectMeta.OwnerReferences)
			}
		}
		gatherMetrics(ctx, f)
		if err = e2epod.DeletePodsWithGracePeriod(ctx, clientSet, pods.Items, 0); err != nil {
			framework.Logf("WARNING: failed to delete pods: %v", err)
		}
	})

	// TODO: 应改为集成测试
	/*
		Release: v1.9
		Testname: Garbage Collector, dependency cycle
		Description: 创建三个 pod，patch 其 ownerReference 形成环状依赖：pod1 的 owner 是 pod3，pod2 的 owner 是 pod1，pod3 的 owner 是 pod2。删除 pod1 MUST 删除所有 pod。依赖环 MUST NOT 阻塞垃圾回收。
	*/
	framework.ConformanceIt("should not be blocked by dependency circle", func(ctx context.Context) {
		clientSet := f.ClientSet
		podClient := clientSet.CoreV1().Pods(f.Namespace.Name)
		pod1Name := "pod1"
		pod1 := newGCPod(pod1Name)
		pod1, err := podClient.Create(ctx, pod1, metav1.CreateOptions{})
		framework.ExpectNoError(err, "failed to create pod %s in namespace: %s", pod1Name, f.Namespace.Name)
		pod2Name := "pod2"
		pod2 := newGCPod(pod2Name)
		pod2, err = podClient.Create(ctx, pod2, metav1.CreateOptions{})
		framework.ExpectNoError(err, "failed to create pod %s in namespace: %s", pod2Name, f.Namespace.Name)
		pod3Name := "pod3"
		pod3 := newGCPod(pod3Name)
		pod3, err = podClient.Create(ctx, pod3, metav1.CreateOptions{})
		framework.ExpectNoError(err, "failed to create pod %s in namespace: %s", pod3Name, f.Namespace.Name)
		// 构造循环依赖
		addRefPatch := func(name string, uid types.UID) []byte {
			return []byte(fmt.Sprintf(`{"metadata":{"ownerReferences":[{"apiVersion":"v1","kind":"Pod","name":"%s","uid":"%s","controller":true,"blockOwnerDeletion":true}]}}`, name, uid))
		}
		patch1 := addRefPatch(pod3.Name, pod3.UID)
		pod1, err = podClient.Patch(ctx, pod1.Name, types.StrategicMergePatchType, patch1, metav1.PatchOptions{})
		framework.ExpectNoError(err, "failed to apply to pod %s in namespace %s, a strategic merge patch: %s", pod1.Name, f.Namespace.Name, patch1)
		framework.Logf("pod1.ObjectMeta.OwnerReferences=%#v", pod1.ObjectMeta.OwnerReferences)
		patch2 := addRefPatch(pod1.Name, pod1.UID)
		pod2, err = podClient.Patch(ctx, pod2.Name, types.StrategicMergePatchType, patch2, metav1.PatchOptions{})
		framework.ExpectNoError(err, "failed to apply to pod %s in namespace %s, a strategic merge patch: %s", pod2.Name, f.Namespace.Name, patch2)
		framework.Logf("pod2.ObjectMeta.OwnerReferences=%#v", pod2.ObjectMeta.OwnerReferences)
		patch3 := addRefPatch(pod2.Name, pod2.UID)
		pod3, err = podClient.Patch(ctx, pod3.Name, types.StrategicMergePatchType, patch3, metav1.PatchOptions{})
		framework.ExpectNoError(err, "failed to apply to pod %s in namespace %s, a strategic merge patch: %s", pod3.Name, f.Namespace.Name, patch3)
		framework.Logf("pod3.ObjectMeta.OwnerReferences=%#v", pod3.ObjectMeta.OwnerReferences)
		// 删除一个 pod 应导致所有 pod 都被删除
		deleteOptions := getForegroundOptions()
		deleteOptions.Preconditions = metav1.NewUIDPreconditions(string(pod1.UID))
		err = podClient.Delete(ctx, pod1.ObjectMeta.Name, deleteOptions)
		framework.ExpectNoError(err, "failed to delete pod %s in namespace: %s", pod1.Name, f.Namespace.Name)
		var pods *v1.PodList
		var err2 error
		// TODO: GC 的周期性 API 重新发现优化后，可缩短该超时。
		// Tracked at https://github.com/kubernetes/kubernetes/issues/50046.
		if err := wait.PollUntilContextTimeout(ctx, 5*time.Second, 90*time.Second+gcInformerResyncRetryTimeout, false, func(ctx context.Context) (bool, error) {
			pods, err2 = podClient.List(ctx, metav1.ListOptions{})
			if err2 != nil {
				return false, fmt.Errorf("failed to list pods: %w", err)
			}
			if len(pods.Items) == 0 {
				return true, nil
			}
			return false, nil
		}); err != nil {
			data, _ := json.Marshal(pods.Items)
			framework.Logf("pods are %s", string(data))
			framework.Failf("failed to wait for all pods to be deleted: %v", err)
		}
	})

	ginkgo.It("should support cascading deletion of custom resources", func(ctx context.Context) {
		config, err := framework.LoadConfig()
		if err != nil {
			framework.Failf("failed to load config: %v", err)
		}

		apiExtensionClient, err := apiextensionsclientset.NewForConfig(config)
		if err != nil {
			framework.Failf("failed to initialize apiExtensionClient: %v", err)
		}

		// 创建一个随机名称的 CRD，并等待其可用
		definition := apiextensionstestserver.NewRandomNameV1CustomResourceDefinition(apiextensionsv1.ClusterScoped)
		defer func() {
			err = apiextensionstestserver.DeleteV1CustomResourceDefinition(definition, apiExtensionClient)
			if err != nil && !apierrors.IsNotFound(err) {
				framework.Failf("failed to delete CustomResourceDefinition: %v", err)
			}
		}()
		definition, err = apiextensionstestserver.CreateNewV1CustomResourceDefinition(definition, apiExtensionClient, f.DynamicClient)
		if err != nil {
			framework.Failf("failed to create CustomResourceDefinition: %v", err)
		}
		gomega.Expect(definition.Spec.Versions).To(gomega.HaveLen(1), "custom resource definition should have one version")
		version := definition.Spec.Versions[0]

		// 获取该自定义资源的 client
		gvr := schema.GroupVersionResource{Group: definition.Spec.Group, Version: version.Name, Resource: definition.Spec.Names.Plural}
		resourceClient := f.DynamicClient.Resource(gvr)

		apiVersion := definition.Spec.Group + "/" + version.Name

		// 创建自定义 owner 资源
		ownerName := names.SimpleNameGenerator.GenerateName("owner")
		owner := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": apiVersion,
				"kind":       definition.Spec.Names.Kind,
				"metadata": map[string]interface{}{
					"name": ownerName,
				},
			},
		}
		persistedOwner, err := resourceClient.Create(ctx, owner, metav1.CreateOptions{})
		if err != nil {
			framework.Failf("failed to create owner resource %q: %v", ownerName, err)
		}
		framework.Logf("created owner resource %q", ownerName)

		// 创建自定义 dependent 资源
		dependentName := names.SimpleNameGenerator.GenerateName("dependent")
		dependent := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": apiVersion,
				"kind":       definition.Spec.Names.Kind,
				"metadata": map[string]interface{}{
					"name": dependentName,
					"ownerReferences": []interface{}{
						map[string]interface{}{
							"uid":        string(persistedOwner.GetUID()),
							"apiVersion": apiVersion,
							"kind":       definition.Spec.Names.Kind,
							"name":       ownerName,
						},
					},
				},
			},
		}
		persistedDependent, err := resourceClient.Create(ctx, dependent, metav1.CreateOptions{})
		if err != nil {
			framework.Failf("failed to create dependent resource %q: %v", dependentName, err)
		}
		framework.Logf("created dependent resource %q", dependentName)

		// 删除 owner
		background := metav1.DeletePropagationBackground
		err = resourceClient.Delete(ctx, ownerName, metav1.DeleteOptions{PropagationPolicy: &background})
		if err != nil {
			framework.Failf("failed to delete owner resource %q: %v", ownerName, err)
		}

		// 以前台删除模式创建并删除一个无关的自定义资源实例，
		// 以此作为 GC 已识别该自定义资源类型的信号
		canaryName := names.SimpleNameGenerator.GenerateName("canary")
		canary := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": apiVersion,
				"kind":       definition.Spec.Names.Kind,
				"metadata":   map[string]interface{}{"name": canaryName}},
		}
		_, err = resourceClient.Create(ctx, canary, metav1.CreateOptions{})
		if err != nil {
			framework.Failf("failed to create canary resource %q: %v", canaryName, err)
		}
		framework.Logf("created canary resource %q", canaryName)
		foreground := metav1.DeletePropagationForeground
		err = resourceClient.Delete(ctx, canaryName, metav1.DeleteOptions{PropagationPolicy: &foreground})
		if err != nil {
			framework.Failf("failed to delete canary resource %q: %v", canaryName, err)
		}
		// 等待 canary 的前台终结完成，这意味着 GC 已识别新的自定义资源类型
		var lastCanary *unstructured.Unstructured
		if err := wait.PollUntilContextTimeout(ctx, 5*time.Second, 3*time.Minute, true, func(ctx context.Context) (bool, error) {
			lastCanary, err = resourceClient.Get(ctx, dependentName, metav1.GetOptions{})
			return apierrors.IsNotFound(err), nil
		}); err != nil {
			framework.Logf("canary last state: %#v", lastCanary)
			framework.Failf("failed waiting for canary resource %q to be deleted", canaryName)
		}

		// 确认 dependent 已被删除
		var lastDependent *unstructured.Unstructured
		var err2 error
		if err := wait.PollUntilContextTimeout(ctx, 5*time.Second, 60*time.Second, false, func(ctx context.Context) (bool, error) {
			lastDependent, err2 = resourceClient.Get(ctx, dependentName, metav1.GetOptions{})
			return apierrors.IsNotFound(err2), nil
		}); err != nil {
			framework.Logf("owner: %#v", persistedOwner)
			framework.Logf("dependent: %#v", persistedDependent)
			framework.Logf("dependent last state: %#v", lastDependent)
			framework.Failf("failed waiting for dependent resource %q to be deleted", dependentName)
		}

		// 确认 owner 已被删除
		_, err = resourceClient.Get(ctx, ownerName, metav1.GetOptions{})
		if err == nil {
			framework.Failf("expected owner resource %q to be deleted", ownerName)
		} else {
			if !apierrors.IsNotFound(err) {
				framework.Failf("unexpected error getting owner resource %q: %v", ownerName, err)
			}
		}
	})

	ginkgo.It("should support orphan deletion of custom resources", func(ctx context.Context) {
		config, err := framework.LoadConfig()
		if err != nil {
			framework.Failf("failed to load config: %v", err)
		}

		apiExtensionClient, err := apiextensionsclientset.NewForConfig(config)
		if err != nil {
			framework.Failf("failed to initialize apiExtensionClient: %v", err)
		}

		// 创建一个随机名称的 CRD，并等待其可用
		definition := apiextensionstestserver.NewRandomNameV1CustomResourceDefinition(apiextensionsv1.ClusterScoped)
		defer func() {
			err = apiextensionstestserver.DeleteV1CustomResourceDefinition(definition, apiExtensionClient)
			if err != nil && !apierrors.IsNotFound(err) {
				framework.Failf("failed to delete CustomResourceDefinition: %v", err)
			}
		}()
		definition, err = apiextensionstestserver.CreateNewV1CustomResourceDefinition(definition, apiExtensionClient, f.DynamicClient)
		if err != nil {
			framework.Failf("failed to create CustomResourceDefinition: %v", err)
		}
		gomega.Expect(definition.Spec.Versions).To(gomega.HaveLen(1), "custom resource definition should have one version")
		version := definition.Spec.Versions[0]

		// 获取该自定义资源的 client
		gvr := schema.GroupVersionResource{Group: definition.Spec.Group, Version: version.Name, Resource: definition.Spec.Names.Plural}
		resourceClient := f.DynamicClient.Resource(gvr)

		apiVersion := definition.Spec.Group + "/" + version.Name

		// 创建自定义 owner 资源
		ownerName := names.SimpleNameGenerator.GenerateName("owner")
		owner := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": apiVersion,
				"kind":       definition.Spec.Names.Kind,
				"metadata": map[string]interface{}{
					"name": ownerName,
				},
			},
		}
		persistedOwner, err := resourceClient.Create(ctx, owner, metav1.CreateOptions{})
		if err != nil {
			framework.Failf("failed to create owner resource %q: %v", ownerName, err)
		}
		framework.Logf("created owner resource %q", ownerName)

		// 创建自定义 dependent 资源
		dependentName := names.SimpleNameGenerator.GenerateName("dependent")
		dependent := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": apiVersion,
				"kind":       definition.Spec.Names.Kind,
				"metadata": map[string]interface{}{
					"name": dependentName,
					"ownerReferences": []map[string]string{
						{
							"uid":        string(persistedOwner.GetUID()),
							"apiVersion": apiVersion,
							"kind":       definition.Spec.Names.Kind,
							"name":       ownerName,
						},
					},
				},
			},
		}
		_, err = resourceClient.Create(ctx, dependent, metav1.CreateOptions{})
		if err != nil {
			framework.Failf("failed to create dependent resource %q: %v", dependentName, err)
		}
		framework.Logf("created dependent resource %q", dependentName)

		// 删除 owner，并将 dependent 置为孤儿
		err = resourceClient.Delete(ctx, ownerName, getOrphanOptions())
		if err != nil {
			framework.Failf("failed to delete owner resource %q: %v", ownerName, err)
		}

		ginkgo.By("wait for the owner to be deleted")
		if err := wait.PollUntilContextTimeout(ctx, 5*time.Second, 120*time.Second, false, func(ctx context.Context) (bool, error) {
			_, err = resourceClient.Get(ctx, ownerName, metav1.GetOptions{})
			if err == nil {
				return false, nil
			}
			if err != nil && !apierrors.IsNotFound(err) {
				return false, fmt.Errorf("failed to get owner: %w", err)
			}
			return true, nil
		}); err != nil {
			framework.Failf("timeout in waiting for the owner to be deleted: %v", err)
		}

		timeout := 30*time.Second + gcInformerResyncRetryTimeout
		ginkgo.By(fmt.Sprintf("wait for %s to see if the garbage collector mistakenly deletes the dependent crd\n", timeout))
		gomega.Consistently(ctx, framework.HandleRetry(func(ctx context.Context) (*unstructured.Unstructured, error) {
			return resourceClient.Get(ctx, dependentName, metav1.GetOptions{})
		})).WithTimeout(timeout).WithPolling(5 * time.Second).ShouldNot(gomega.BeNil())
	})

	ginkgo.It("should delete jobs and pods created by cronjob", func(ctx context.Context) {

		ginkgo.By("Create the cronjob")
		cronJob := newCronJob("simple", "*/1 * * * ?")
		cronJob, err := f.ClientSet.BatchV1().CronJobs(f.Namespace.Name).Create(ctx, cronJob, metav1.CreateOptions{})
		framework.ExpectNoError(err, "failed to create cronjob: %+v, in namespace: %s", cronJob, f.Namespace.Name)

		ginkgo.By("Wait for the CronJob to create new Job")
		err = wait.PollUntilContextTimeout(ctx, 500*time.Millisecond, 2*time.Minute, true, func(ctx context.Context) (bool, error) {
			jobs, err := f.ClientSet.BatchV1().Jobs(f.Namespace.Name).List(ctx, metav1.ListOptions{})
			if err != nil {
				return false, fmt.Errorf("failed to list jobs: %w", err)
			}
			return len(jobs.Items) > 0, nil
		})
		if err != nil {
			framework.Failf("Failed to wait for the CronJob to create some Jobs: %v", err)
		}

		ginkgo.By("Delete the cronjob")
		if err := f.ClientSet.BatchV1().CronJobs(f.Namespace.Name).Delete(ctx, cronJob.Name, getBackgroundOptions()); err != nil {
			framework.Failf("Failed to delete the CronJob: %v", err)
		}
		ginkgo.By("Verify if cronjob does not leave jobs nor pods behind")
		err = wait.PollUntilContextTimeout(ctx, 500*time.Millisecond, 1*time.Minute, true, func(ctx context.Context) (bool, error) {
			objects := map[string]int{"CronJobs": 0, "Jobs": 0, "Pods": 0}
			return verifyRemainingObjects(ctx, f, objects)
		})
		if err != nil {
			framework.Failf("Failed to wait for all jobs and pods to be deleted: %v", err)
		}

		gatherMetrics(ctx, f)
	})
})

// TODO(106575): 不再使用通用轮询函数。
func waitForReplicas(ctx context.Context, rc *v1.ReplicationController, rcClient clientv1.ReplicationControllerInterface) {
	var (
		lastObservedRC *v1.ReplicationController
		err            error
	)
	if err := wait.PollUntilContextTimeout(ctx, framework.Poll, replicaSyncTimeout, false, func(ctx context.Context) (bool, error) {
		lastObservedRC, err = rcClient.Get(ctx, rc.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		if lastObservedRC.Status.Replicas == *rc.Spec.Replicas {
			return true, nil
		}
		return false, nil
	}); err != nil {
		if lastObservedRC == nil {
			framework.Failf("Failed to get ReplicationController %q: %v", rc.Name, err)
		} else {
			framework.Failf("failed to wait for the rc.Status.Replicas (%d) to reach rc.Spec.Replicas (%d): %v",
				lastObservedRC.Status.Replicas, *lastObservedRC.Spec.Replicas, err)
		}
	}
}
