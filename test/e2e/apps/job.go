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

package apps

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	schedulingv1alpha3 "k8s.io/api/scheduling/v1alpha3"
	schedulingv1beta1 "k8s.io/api/scheduling/v1beta1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	utilrand "k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/apimachinery/pkg/util/resourceversion"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
	watchtools "k8s.io/client-go/tools/watch"
	"k8s.io/client-go/util/retry"
	"k8s.io/client-go/util/workqueue"
	batchinternal "k8s.io/kubernetes/pkg/apis/batch"
	"k8s.io/kubernetes/pkg/features"
	apimachineryutils "k8s.io/kubernetes/test/e2e/common/apimachinery"
	"k8s.io/kubernetes/test/e2e/framework"
	e2ejob "k8s.io/kubernetes/test/e2e/framework/job"
	e2enode "k8s.io/kubernetes/test/e2e/framework/node"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2eresource "k8s.io/kubernetes/test/e2e/framework/resource"
	"k8s.io/kubernetes/test/e2e/scheduling"
	admissionapi "k8s.io/pod-security-admission/api"
	"k8s.io/utils/ptr"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

type watchEventConfig struct {
	framework           *framework.Framework
	resourceVersion     string
	w                   *cache.ListWatch
	jobName             string
	watchEvent          watch.EventType
	extJob              *batchv1.Job
	updatedMetadataType string
	updatedKey          string
	updatedValue        string
}

var _ = SIGDescribe("Job", func() {
	f := framework.NewDefaultFramework("job")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged

	// 最简单的场景：N 个 pod 成功
	ginkgo.It("should run a job to completion when tasks succeed", func(ctx context.Context) {
		parallelism := int32(2)
		completions := int32(4)
		backoffLimit := int32(6) // default value

		ginkgo.By("Creating a job")
		job := e2ejob.NewTestJob("succeed", "all-succeed", v1.RestartPolicyNever, parallelism, completions, nil, backoffLimit)
		job, err := e2ejob.CreateJob(ctx, f.ClientSet, f.Namespace.Name, job)
		framework.ExpectNoError(err, "failed to create job in namespace: %s", f.Namespace.Name)

		ginkgo.By("Ensuring job reaches completions")
		err = e2ejob.WaitForJobComplete(ctx, f.ClientSet, f.Namespace.Name, job.Name, batchv1.JobReasonCompletionsReached, completions)
		framework.ExpectNoError(err, "failed to ensure job completion in namespace: %s", f.Namespace.Name)

		ginkgo.By("Ensuring pods for job exist")
		pods, err := e2ejob.GetJobPods(ctx, f.ClientSet, f.Namespace.Name, job.Name)
		framework.ExpectNoError(err, "failed to get pod list for job in namespace: %s", f.Namespace.Name)
		successes := int32(0)
		for _, pod := range pods.Items {
			if pod.Status.Phase == v1.PodSucceeded {
				successes++
			}
		}
		gomega.Expect(successes).To(gomega.Equal(completions), "expected %d successful job pods, but got  %d", completions, successes)
	})

	f.It("should compile a Workload and PodGroup for a gang-scheduled Job and wire its pods to the PodGroup", framework.WithFeatureGate(features.WorkloadWithJob), func(ctx context.Context) {
		parallelism := int32(2)
		completions := int32(2)
		backoffLimit := int32(6)

		ginkgo.By("Creating a gang-scheduled job")
		job := e2ejob.NewTestJob("notTerminate", "gang", v1.RestartPolicyNever, parallelism, completions, nil, backoffLimit)
		job.Spec.Scheduling = &batchv1.JobSchedulingConfiguration{
			SchedulingPolicy: &schedulingv1alpha3.WorkloadPodGroupSchedulingPolicy{
				Gang: &schedulingv1alpha3.WorkloadPodGroupGangSchedulingPolicy{MinCount: new(parallelism)},
			},
		}
		job, err := e2ejob.CreateJob(ctx, f.ClientSet, f.Namespace.Name, job)
		framework.ExpectNoError(err, "failed to create job in namespace: %s", f.Namespace.Name)

		ginkgo.By("Waiting for a gang Workload owned by the Job")
		var workload *schedulingv1beta1.Workload
		gomega.Eventually(ctx, func(ctx context.Context) (*schedulingv1beta1.Workload, error) {
			list, err := f.ClientSet.SchedulingV1beta1().Workloads(f.Namespace.Name).List(ctx, metav1.ListOptions{})
			if err != nil {
				return nil, err
			}
			for i := range list.Items {
				if ref := list.Items[i].Spec.ControllerRef; ref != nil && ref.Kind == "Job" && ref.Name == job.Name {
					workload = &list.Items[i]
					return workload, nil
				}
			}
			return nil, nil
		}).WithTimeout(f.Timeouts.PodStart).ShouldNot(gomega.BeNil(), "expected a Workload owned by the Job")

		gomega.Expect(workload.Spec.PodGroupTemplates).To(gomega.HaveLen(1), "expected a single PodGroupTemplate")
		gomega.Expect(workload.Spec.PodGroupTemplates[0].SchedulingPolicy.Gang).NotTo(gomega.BeNil(), "expected a gang PodGroupTemplate")
		gomega.Expect(workload.Spec.PodGroupTemplates[0].SchedulingPolicy.Gang.MinCount).To(gomega.Equal(parallelism), "unexpected gang minCount")

		ginkgo.By("Waiting for the runtime PodGroup referencing the Workload")
		var podGroup *schedulingv1beta1.PodGroup
		gomega.Eventually(ctx, func(ctx context.Context) (*schedulingv1beta1.PodGroup, error) {
			list, err := f.ClientSet.SchedulingV1beta1().PodGroups(f.Namespace.Name).List(ctx, metav1.ListOptions{})
			if err != nil {
				return nil, err
			}
			for i := range list.Items {
				if ref := list.Items[i].Spec.WorkloadRef; ref != nil && ref.WorkloadName == workload.Name {
					podGroup = &list.Items[i]
					return podGroup, nil
				}
			}
			return nil, nil
		}).WithTimeout(f.Timeouts.PodStart).ShouldNot(gomega.BeNil(), "expected a PodGroup referencing the Workload")

		ginkgo.By("Ensuring the Job's pods reference the PodGroup via schedulingGroup")
		gomega.Eventually(ctx, func(ctx context.Context) (int, error) {
			pods, err := e2ejob.GetJobPods(ctx, f.ClientSet, f.Namespace.Name, job.Name)
			if err != nil {
				return 0, err
			}
			return len(pods.Items), nil
		}).WithTimeout(f.Timeouts.PodStart).Should(gomega.Equal(int(parallelism)), "expected %d job pods", parallelism)

		pods, err := e2ejob.GetJobPods(ctx, f.ClientSet, f.Namespace.Name, job.Name)
		framework.ExpectNoError(err, "failed to get pod list for job")
		for _, pod := range pods.Items {
			gomega.Expect(pod.Spec.SchedulingGroup).NotTo(gomega.BeNil(), "pod %s missing schedulingGroup", pod.Name)
			gomega.Expect(pod.Spec.SchedulingGroup.PodGroupName).To(gomega.HaveValue(gomega.Equal(podGroup.Name)), "pod %s schedulingGroup does not reference the PodGroup", pod.Name)
		}
	})

	f.It("should propagate an elastic gang minCount change to the Workload and PodGroup", framework.WithFeatureGate(features.WorkloadWithJob), func(ctx context.Context) {
		parallelism := int32(3)
		completions := int32(3)
		backoffLimit := int32(6)
		initialMinCount := int32(3)
		updatedMinCount := int32(2)

		ginkgo.By("Creating a gang-scheduled job")
		job := e2ejob.NewTestJob("notTerminate", "gang-resize", v1.RestartPolicyNever, parallelism, completions, nil, backoffLimit)
		job.Spec.Scheduling = &batchv1.JobSchedulingConfiguration{
			SchedulingPolicy: &schedulingv1alpha3.WorkloadPodGroupSchedulingPolicy{
				Gang: &schedulingv1alpha3.WorkloadPodGroupGangSchedulingPolicy{MinCount: new(initialMinCount)},
			},
		}
		job, err := e2ejob.CreateJob(ctx, f.ClientSet, f.Namespace.Name, job)
		framework.ExpectNoError(err, "failed to create job in namespace: %s", f.Namespace.Name)

		ginkgo.By("Waiting for a gang Workload owned by the Job with the initial minCount")
		var workloadName string
		gomega.Eventually(ctx, func(ctx context.Context) (*int32, error) {
			list, err := f.ClientSet.SchedulingV1beta1().Workloads(f.Namespace.Name).List(ctx, metav1.ListOptions{})
			if err != nil {
				return nil, err
			}
			for i := range list.Items {
				ref := list.Items[i].Spec.ControllerRef
				if ref == nil || ref.Kind != "Job" || ref.Name != job.Name {
					continue
				}
				if len(list.Items[i].Spec.PodGroupTemplates) == 0 || list.Items[i].Spec.PodGroupTemplates[0].SchedulingPolicy.Gang == nil {
					return nil, nil
				}
				workloadName = list.Items[i].Name
				return new(list.Items[i].Spec.PodGroupTemplates[0].SchedulingPolicy.Gang.MinCount), nil
			}
			return nil, nil
		}).WithTimeout(f.Timeouts.PodStart).Should(gomega.HaveValue(gomega.Equal(initialMinCount)), "expected the Workload gang minCount to match the initial value")

		ginkgo.By("Reducing the Job's gang minCount")
		err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
			cur, err := f.ClientSet.BatchV1().Jobs(f.Namespace.Name).Get(ctx, job.Name, metav1.GetOptions{})
			if err != nil {
				return err
			}
			cur.Spec.Scheduling.SchedulingPolicy.Gang.MinCount = new(updatedMinCount)
			_, err = f.ClientSet.BatchV1().Jobs(f.Namespace.Name).Update(ctx, cur, metav1.UpdateOptions{})
			return err
		})
		framework.ExpectNoError(err, "failed to update job gang minCount")

		ginkgo.By("Waiting for the new minCount to propagate to the Workload template")
		gomega.Eventually(ctx, func(ctx context.Context) (*int32, error) {
			wl, err := f.ClientSet.SchedulingV1beta1().Workloads(f.Namespace.Name).Get(ctx, workloadName, metav1.GetOptions{})
			if err != nil {
				return nil, err
			}
			if len(wl.Spec.PodGroupTemplates) == 0 || wl.Spec.PodGroupTemplates[0].SchedulingPolicy.Gang == nil {
				return nil, nil
			}
			return new(wl.Spec.PodGroupTemplates[0].SchedulingPolicy.Gang.MinCount), nil
		}).WithTimeout(f.Timeouts.PodStart).Should(gomega.HaveValue(gomega.Equal(updatedMinCount)), "expected the Workload gang minCount to be updated")

		ginkgo.By("Waiting for the new minCount to propagate to the runtime PodGroup")
		gomega.Eventually(ctx, func(ctx context.Context) (*int32, error) {
			list, err := f.ClientSet.SchedulingV1beta1().PodGroups(f.Namespace.Name).List(ctx, metav1.ListOptions{})
			if err != nil {
				return nil, err
			}
			for i := range list.Items {
				ref := list.Items[i].Spec.WorkloadRef
				if ref == nil || ref.WorkloadName != workloadName {
					continue
				}
				if list.Items[i].Spec.SchedulingPolicy.Gang == nil {
					return nil, nil
				}
				return new(list.Items[i].Spec.SchedulingPolicy.Gang.MinCount), nil
			}
			return nil, nil
		}).WithTimeout(f.Timeouts.PodStart).Should(gomega.HaveValue(gomega.Equal(updatedMinCount)), "expected the PodGroup gang minCount to be updated")
	})

	/*
		发布版本：v1.31
		测试名称：Verify Pod Failure policy allows to fail job early on exit code.
		描述：创建一个带 pod failure policy 的 job，并恰好有一个
		pod 失败。失败 pod 的退出码匹配触发 Job 失败的
		pod failure policy。
	*/
	framework.ConformanceIt("should allow to use the pod failure policy on exit code to fail the job early", func(ctx context.Context) {

		// 我们只让 Job 的 pod 失败一次，以确保不会触及 backoffLimit，
		// 从而确认 Job 失败是由带 FailJob action 的
		// pod failure policy 导致的。
		// 为确保 Job 的 pod 先失败一次再成功，我们强制
		// Job 的 Pod 调度到单个 Node 上，并使用 hostPath
		// volume 在新 Pod 之间持久化数据。
		ginkgo.By("Looking for a node to schedule job pod")
		node, err := e2enode.GetRandomReadySchedulableNode(ctx, f.ClientSet)
		framework.ExpectNoError(err)

		parallelism := int32(2)
		completions := int32(4)
		backoffLimit := int32(6) // default value
		ginkgo.By("Creating a job")
		job := e2ejob.NewTestJobOnNode("failOnce", "pod-failure-failjob", v1.RestartPolicyNever, parallelism, completions, nil, backoffLimit, node.Name)
		job.Spec.PodFailurePolicy = &batchv1.PodFailurePolicy{
			Rules: []batchv1.PodFailurePolicyRule{
				{
					Action: batchv1.PodFailurePolicyActionFailJob,
					OnExitCodes: &batchv1.PodFailurePolicyOnExitCodesRequirement{
						Operator: batchv1.PodFailurePolicyOnExitCodesOpIn,
						Values:   []int32{1},
					},
				},
			},
		}
		job, err = e2ejob.CreateJob(ctx, f.ClientSet, f.Namespace.Name, job)
		framework.ExpectNoError(err, "failed to create job in namespace: %s", f.Namespace.Name)

		ginkgo.By("Ensuring job fails")
		err = e2ejob.WaitForJobFailed(ctx, f.ClientSet, f.Namespace.Name, job.Name)
		framework.ExpectNoError(err, "failed to ensure job failure in namespace: %s", f.Namespace.Name)
	})

	/*
		测试名称：Ensure pod failure policy allows to ignore failure matching on the exit code
		描述：本测试使用 indexed job。每个 index 对应的 pod
		在宿主机上创建标记文件然后失败。重启后看到标记文件存在则
		成功。这样每个 index 触发一次失败；如果没有按退出码匹配的
		ignore 规则，Job 就会被标记为失败。

	*/
	ginkgo.It("should allow to use a pod failure policy to ignore failure matching on exit code", func(ctx context.Context) {
		// 设置 backoffLimit = numPods-1，以便容忍随机
		// 失败（比如 kubelet 的 OutOfPods）。但如果 pod 失败未被忽略，
		// Job 仍会失败。
		numPods := 3
		parallelism := int32(numPods)
		completions := int32(numPods)
		backoffLimit := int32(numPods) - 1

		ginkgo.By("Looking for a node to schedule job pods")
		node, err := e2enode.GetRandomReadySchedulableNode(ctx, f.ClientSet)
		framework.ExpectNoError(err)

		ginkgo.By("Creating a job")
		job := e2ejob.NewTestJobOnNode("failOncePerIndex", "fail-pod-ignore-on-exit-code", v1.RestartPolicyNever, parallelism, completions, nil, backoffLimit, node.Name)
		job.Spec.CompletionMode = ptr.To(batchv1.IndexedCompletion)
		job.Spec.PodFailurePolicy = &batchv1.PodFailurePolicy{
			Rules: []batchv1.PodFailurePolicyRule{
				{
					Action: batchv1.PodFailurePolicyActionIgnore,
					OnExitCodes: &batchv1.PodFailurePolicyOnExitCodesRequirement{
						Operator: batchv1.PodFailurePolicyOnExitCodesOpIn,
						Values:   []int32{42},
					},
				},
			},
		}
		job, err = e2ejob.CreateJob(ctx, f.ClientSet, f.Namespace.Name, job)
		framework.ExpectNoError(err, "failed to create job in namespace: %s", f.Namespace.Name)

		ginkgo.By("Ensuring job reaches completions")
		err = e2ejob.WaitForJobComplete(ctx, f.ClientSet, f.Namespace.Name, job.Name, batchv1.JobReasonCompletionsReached, completions)
		framework.ExpectNoError(err, "failed to ensure job completion in namespace: %s", f.Namespace.Name)
	})

	/*
		发布版本：v1.32
		测试名称：Ensure pod failure policy allows to ignore failure matching on the DisruptionTarget condition
		描述：本测试使用 indexed job。每个 index 对应的 pod
		在宿主机上创建标记文件，然后一直运行直到被驱逐。标记文件
		创建后，pod 重启看到它就会成功。因此每个 index 因驱逐
		触发一次失败（过程中会添加 DisruptionTarget
		condition）。如果没有按退出码匹配的 ignore 规则，
		Job 就会被标记为失败。
	*/
	framework.ConformanceIt("should allow to use a pod failure policy to ignore failure matching on DisruptionTarget condition", func(ctx context.Context) {
		// 设置 backoffLimit = numPods-1，以便容忍随机
		// 失败（比如 kubelet 的 OutOfPods）。但如果 pod 失败未被忽略，
		// Job 仍会失败。
		numPods := 3
		parallelism := int32(numPods)
		completions := int32(numPods)
		backoffLimit := int32(numPods) - 1

		ginkgo.By("Looking for a node to schedule job pods")
		node, err := e2enode.GetRandomReadySchedulableNode(ctx, f.ClientSet)
		framework.ExpectNoError(err)

		ginkgo.By("Creating a job")
		job := e2ejob.NewTestJobOnNode("notTerminateOncePerIndex", "evicted-pod-ignore-on-disruption-condition", v1.RestartPolicyNever, parallelism, completions, nil, backoffLimit, node.Name)
		job.Spec.CompletionMode = ptr.To(batchv1.IndexedCompletion)
		job.Spec.PodFailurePolicy = &batchv1.PodFailurePolicy{
			Rules: []batchv1.PodFailurePolicyRule{
				{
					// 忽略由 DisruptionTarget condition 触发的
					// 驱逐导致的 pod 失败
					Action: batchv1.PodFailurePolicyActionIgnore,
					OnPodConditions: []batchv1.PodFailurePolicyOnPodConditionsPattern{
						{
							Type:   v1.DisruptionTarget,
							Status: v1.ConditionTrue,
						},
					},
				},
			},
		}
		job, err = e2ejob.CreateJob(ctx, f.ClientSet, f.Namespace.Name, job)
		framework.ExpectNoError(err, "failed to create job in namespace: %s", f.Namespace.Name)

		ginkgo.By("Waiting for all the pods to be ready")
		err = e2ejob.WaitForJobReady(ctx, f.ClientSet, f.Namespace.Name, job.Name, ptr.To(int32(numPods)))
		framework.ExpectNoError(err, "failed to await for all pods to be ready for job: %s/%s", job.Name, job.Namespace)

		ginkgo.By("Fetch all running pods")
		pods, err := e2ejob.GetAllRunningJobPods(ctx, f.ClientSet, f.Namespace.Name, job.Name)
		framework.ExpectNoError(err, "failed to get running pods for the job: %s/%s", job.Name, job.Namespace)
		gomega.Expect(pods).To(gomega.HaveLen(numPods), "Number of running pods doesn't match parallelism")

		ginkgo.By("Evict all the Pods")
		workqueue.ParallelizeUntil(ctx, numPods, numPods, func(index int) {
			defer ginkgo.GinkgoRecover()

			pod := pods[index]
			ginkgo.By(fmt.Sprintf("Evicting the running pod: %s/%s", pod.Name, pod.Namespace))
			evictTarget := &policyv1.Eviction{
				ObjectMeta: metav1.ObjectMeta{
					Name:      pod.Name,
					Namespace: pod.Namespace,
				},
			}
			// 这里显式使用新变量，确保它不会逃逸到 goroutine 的作用域之外
			err := f.ClientSet.CoreV1().Pods(pod.Namespace).EvictV1(ctx, evictTarget)
			framework.ExpectNoError(err, "failed to evict the pod: %s/%s", pod.Name, pod.Namespace)

			ginkgo.By(fmt.Sprintf("Awaiting for the pod: %s/%s to be deleted", pod.Name, pod.Namespace))
			err = e2epod.WaitForPodNotFoundInNamespace(ctx, f.ClientSet, pod.Name, pod.Namespace, f.Timeouts.PodDelete)
			framework.ExpectNoError(err, "failed to await for all pods to be deleted: %s/%s", pod.Name, pod.Namespace)
		})

		ginkgo.By("Ensuring job reaches completions")
		err = e2ejob.WaitForJobComplete(ctx, f.ClientSet, f.Namespace.Name, job.Name, batchv1.JobReasonCompletionsReached, completions)
		framework.ExpectNoError(err, "failed to ensure job completion in namespace: %s", f.Namespace.Name)
	})

	ginkgo.It("should not create pods when created in suspend state", func(ctx context.Context) {
		parallelism := int32(2)
		completions := int32(4)
		backoffLimit := int32(6) // default value

		ginkgo.By("Creating a job with suspend=true")
		job := e2ejob.NewTestJob("succeed", "suspend-true-to-false", v1.RestartPolicyNever, parallelism, completions, nil, backoffLimit)
		job.Spec.Suspend = ptr.To(true)
		job, err := e2ejob.CreateJob(ctx, f.ClientSet, f.Namespace.Name, job)
		framework.ExpectNoError(err, "failed to create job in namespace: %s", f.Namespace.Name)

		ginkgo.By("Checking Job status to observe Suspended state")
		err = e2ejob.WaitForJobSuspend(ctx, f.ClientSet, f.Namespace.Name, job.Name)
		framework.ExpectNoError(err, "failed to observe suspend state: %s", f.Namespace.Name)

		ginkgo.By("Ensuring pods aren't created for job")
		pods, err := e2ejob.GetJobPods(ctx, f.ClientSet, f.Namespace.Name, job.Name)
		framework.ExpectNoError(err, "failed to list pod for a given job %s in namespace %s", job.Name, f.Namespace.Name)
		gomega.Expect(pods.Items).To(gomega.BeEmpty())

		ginkgo.By("Updating the job with suspend=false")
		job, err = f.ClientSet.BatchV1().Jobs(f.Namespace.Name).Get(ctx, job.Name, metav1.GetOptions{})
		framework.ExpectNoError(err, "failed to get job in namespace: %s", f.Namespace.Name)
		job.Spec.Suspend = ptr.To(false)
		job, err = e2ejob.UpdateJob(ctx, f.ClientSet, f.Namespace.Name, job)
		framework.ExpectNoError(err, "failed to update job in namespace: %s", f.Namespace.Name)

		ginkgo.By("Waiting for job to complete")
		err = e2ejob.WaitForJobComplete(ctx, f.ClientSet, f.Namespace.Name, job.Name, batchv1.JobReasonCompletionsReached, completions)
		framework.ExpectNoError(err, "failed to ensure job completion in namespace: %s", f.Namespace.Name)
	})

	ginkgo.It("should delete pods when suspended", func(ctx context.Context) {
		parallelism := int32(2)
		completions := int32(4)
		backoffLimit := int32(6) // default value

		ginkgo.By("Creating a job with suspend=false")
		job := e2ejob.NewTestJob("notTerminate", "suspend-false-to-true", v1.RestartPolicyNever, parallelism, completions, nil, backoffLimit)
		job.Spec.Suspend = ptr.To(false)
		job, err := e2ejob.CreateJob(ctx, f.ClientSet, f.Namespace.Name, job)
		framework.ExpectNoError(err, "failed to create job in namespace: %s", f.Namespace.Name)

		ginkgo.By("Ensure pods equal to parallelism count is attached to the job")
		err = e2ejob.WaitForJobPodsRunning(ctx, f.ClientSet, f.Namespace.Name, job.Name, parallelism)
		framework.ExpectNoError(err, "failed to ensure number of pods associated with job %s is equal to parallelism count in namespace: %s", job.Name, f.Namespace.Name)

		ginkgo.By("Updating the job with suspend=true")
		err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
			job, err = e2ejob.GetJob(ctx, f.ClientSet, f.Namespace.Name, job.Name)
			framework.ExpectNoError(err, "unable to get job %s in namespace %s", job.Name, f.Namespace.Name)
			job.Spec.Suspend = ptr.To(true)
			updatedJob, err := e2ejob.UpdateJob(ctx, f.ClientSet, f.Namespace.Name, job)
			if err == nil {
				job = updatedJob
			}
			return err
		})
		framework.ExpectNoError(err, "failed to update job in namespace: %s", f.Namespace.Name)

		ginkgo.By("Ensuring pods are deleted")
		err = e2ejob.WaitForAllJobPodsGone(ctx, f.ClientSet, f.Namespace.Name, job.Name)
		framework.ExpectNoError(err, "failed to ensure pods are deleted after suspend=true")

		ginkgo.By("Checking Job status to observe Suspended state")
		job, err = e2ejob.GetJob(ctx, f.ClientSet, f.Namespace.Name, job.Name)
		framework.ExpectNoError(err, "failed to retrieve latest job object")
		exists := false
		for _, c := range job.Status.Conditions {
			if c.Type == batchv1.JobSuspended {
				exists = true
				break
			}
		}
		if !exists {
			framework.Failf("Job was expected to be completed or failed")
		}
	})

	ginkgo.It("should recreate pods only after they have failed if pod replacement policy is set to Failed", func(ctx context.Context) {
		ginkgo.By("Creating a job")
		job := e2ejob.NewTestJob("", "pod-recreate-failed", v1.RestartPolicyNever, 1, 1, nil, 1)
		job.Spec.PodReplacementPolicy = ptr.To(batchv1.Failed)
		job.Spec.Template.Spec.Containers[0].Command = []string{"/bin/sh", "-c", `_term(){
	sleep 5
	exit 143
}
trap _term SIGTERM
while true; do
	sleep 1
done`}
		job, err := e2ejob.CreateJob(ctx, f.ClientSet, f.Namespace.Name, job)
		framework.ExpectNoError(err, "failed to create job in namespace: %s", f.Namespace.Name)

		err = e2ejob.WaitForJobPodsRunning(ctx, f.ClientSet, f.Namespace.Name, job.Name, 1)
		framework.ExpectNoError(err, "failed to wait for job pod to become running in namespace: %s", f.Namespace.Name)

		ginkgo.By("Deleting job pod")
		pods, err := e2ejob.GetJobPods(ctx, f.ClientSet, f.Namespace.Name, job.Name)
		framework.ExpectNoError(err, "failed to get pod list for job %s in namespace: %s", job.Name, f.Namespace.Name)

		framework.ExpectNoError(e2epod.DeletePodsWithGracePeriod(ctx, f.ClientSet, pods.Items, 30), "failed to delete pods in namespace: %s", f.Namespace.Name)

		ginkgo.By("Ensuring pod does not get recreated while it is in terminating state")
		err = e2ejob.WaitForJobState(ctx, f.ClientSet, f.Namespace.Name, job.Name, f.Timeouts.PodDelete, func(job *batchv1.Job) string {
			if job.Status.Active == 0 && job.Status.Failed == 0 && *job.Status.Terminating == 1 {
				return ""
			} else {
				return fmt.Sprintf(
					"expected job to have 0 active pod, 0 failed pod and 1 terminating pods, but got %d active pods, %d failed pods and %d terminating pods",
					job.Status.Active,
					job.Status.Failed,
					*job.Status.Terminating,
				)
			}
		})
		framework.ExpectNoError(err, "failed to ensure pod is not recreated while it is in terminating state")

		ginkgo.By("Ensuring pod gets recreated after it has failed")
		err = e2ejob.WaitForJobState(ctx, f.ClientSet, f.Namespace.Name, job.Name, f.Timeouts.PodDelete, func(job *batchv1.Job) string {
			if job.Status.Active == 1 && job.Status.Failed == 1 && *job.Status.Terminating == 0 {
				return ""
			} else {
				return fmt.Sprintf(
					"expected job to have 1 active pods, 1 failed pods and 0 terminating pod, but got %d active pods, %d failed pods and %d terminating pods",
					job.Status.Active,
					job.Status.Failed,
					*job.Status.Terminating,
				)
			}
		})
		framework.ExpectNoError(err, "failed to wait for pod to get recreated")
	})

	/*
		  发布版本：v1.24
			测试名称：Ensure Pods of an Indexed Job get a unique index.
			描述：创建一个 Indexed job。Job 必须成功完成。
			确认创建的 pod 具有 completion index 注解和环境变量。
	*/
	framework.ConformanceIt("should create pods for an Indexed job with completion indexes and specified hostname", func(ctx context.Context) {
		parallelism := int32(2)
		completions := int32(4)
		backoffLimit := int32(6) // default value

		ginkgo.By("Creating Indexed job")
		job := e2ejob.NewTestJob("succeed", "indexed-job", v1.RestartPolicyNever, parallelism, completions, nil, backoffLimit)
		mode := batchv1.IndexedCompletion
		job.Spec.CompletionMode = &mode
		job, err := e2ejob.CreateJob(ctx, f.ClientSet, f.Namespace.Name, job)
		framework.ExpectNoError(err, "failed to create indexed job in namespace %s", f.Namespace.Name)

		ginkgo.By("Ensuring job reaches completions")
		err = e2ejob.WaitForJobComplete(ctx, f.ClientSet, f.Namespace.Name, job.Name, batchv1.JobReasonCompletionsReached, completions)
		framework.ExpectNoError(err, "failed to ensure job completion in namespace: %s", f.Namespace.Name)

		ginkgo.By("Ensuring pods with index for job exist")
		pods, err := e2ejob.GetJobPods(ctx, f.ClientSet, f.Namespace.Name, job.Name)
		framework.ExpectNoError(err, "failed to get pod list for job in namespace: %s", f.Namespace.Name)
		succeededIndexes := sets.NewInt()
		for _, pod := range pods.Items {
			if pod.Status.Phase == v1.PodSucceeded && pod.Annotations != nil {
				ix, err := strconv.Atoi(pod.Annotations[batchv1.JobCompletionIndexAnnotation])
				framework.ExpectNoError(err, "failed obtaining completion index from pod in namespace: %s", f.Namespace.Name)
				succeededIndexes.Insert(ix)
				expectedName := fmt.Sprintf("%s-%d", job.Name, ix)
				gomega.Expect(pod.Spec.Hostname).To(gomega.Equal(expectedName), "expected completed pod with hostname %s, but got %s", expectedName, pod.Spec.Hostname)
			}
		}
		gotIndexes := succeededIndexes.List()
		wantIndexes := []int{0, 1, 2, 3}
		gomega.Expect(gotIndexes).To(gomega.Equal(wantIndexes), "expected completed indexes %s, but got %s", wantIndexes, gotIndexes)
	})

	/*
		  发布版本：v1.32
			测试名称：Ensure Pods of an Indexed Job get a unique index for PodIndexLabel key.
			描述：创建一个 Indexed job。Job 必须成功完成。
			确认创建的 pod 具有 completion index 标签。
	*/
	// TODO: 这个测试稳定后，把功能合并到本文件前面已有的
	// 一致性测试中："should create pods for an Indexed job with completion indexes and specified hostname"。
	framework.It("should create pods with completion indexes for an Indexed Job", func(ctx context.Context) {
		parallelism := int32(2)
		completions := int32(4)
		backoffLimit := int32(6) // default value

		ginkgo.By("Creating Indexed job")
		job := e2ejob.NewTestJob("succeed", "indexed-job", v1.RestartPolicyNever, parallelism, completions, nil, backoffLimit)
		job.Spec.CompletionMode = ptr.To(batchv1.IndexedCompletion)
		job, err := e2ejob.CreateJob(ctx, f.ClientSet, f.Namespace.Name, job)
		framework.ExpectNoError(err, "failed to create indexed job in namespace %s", f.Namespace.Name)

		ginkgo.By("Ensuring job reaches completions")
		err = e2ejob.WaitForJobComplete(ctx, f.ClientSet, f.Namespace.Name, job.Name, batchv1.JobReasonCompletionsReached, completions)
		framework.ExpectNoError(err, "failed to ensure job completion in namespace: %s", f.Namespace.Name)

		ginkgo.By("Ensuring all pods have the required index labels")
		pods, err := e2ejob.GetJobPods(ctx, f.ClientSet, f.Namespace.Name, job.Name)
		framework.ExpectNoError(err, "failed to get pod list for job in namespace: %s", f.Namespace.Name)
		succeededIndexes := sets.NewInt()
		for _, pod := range pods.Items {
			ix, err := strconv.Atoi(pod.Labels[batchv1.JobCompletionIndexAnnotation])
			framework.ExpectNoError(err, "failed obtaining completion index in namespace: %s for pod: %s", pod.Namespace, pod.Name)
			succeededIndexes.Insert(ix)
		}
		gotIndexes := succeededIndexes.List()
		wantIndexes := []int{0, 1, 2, 3}
		gomega.Expect(gotIndexes).To(gomega.Equal(wantIndexes), "expected completed indexes in namespace: %s for job: %s", job.Namespace, job.Name)
	})

	/*
		发布版本：v1.33
		测试名称：Ensure that job with successPolicy succeeded when all indexes succeeded
		描述：创建一个带 successPolicy 的 indexed job。
		验证所有 index 成功后，job 获得 SuccessCriteriaMet（原因为 SuccessPolicy）
		和 Complete condition。
	*/
	framework.ConformanceIt("with successPolicy should succeeded when all indexes succeeded", func(ctx context.Context) {
		parallelism := int32(2)
		completions := int32(2)
		backoffLimit := int32(6) // default value

		ginkgo.By("Creating an indexed job with successPolicy")
		job := e2ejob.NewTestJob("succeeded", "with-success-policy-all-index-succeeded", v1.RestartPolicyNever, parallelism, completions, nil, backoffLimit)
		job.Spec.CompletionMode = ptr.To(batchv1.IndexedCompletion)
		job.Spec.SuccessPolicy = &batchv1.SuccessPolicy{
			Rules: []batchv1.SuccessPolicyRule{{
				SucceededCount: ptr.To[int32](2),
			}},
		}
		job, err := e2ejob.CreateJob(ctx, f.ClientSet, f.Namespace.Name, job)
		framework.ExpectNoError(err, "failed to create job in namespace: %s", f.Namespace.Name)

		ginkgo.By("Awaiting for the job to have the interim SuccessCriteriaMet with SuccessPolicy reason condition")
		err = e2ejob.WaitForJobCondition(ctx, f.ClientSet, f.Namespace.Name, job.Name, batchv1.JobSuccessCriteriaMet, ptr.To(batchv1.JobReasonSuccessPolicy))
		framework.ExpectNoError(err, "failed to ensure that job has SuccessCriteriaMet with SuccessPolicy reason condition")

		ginkgo.By("Ensure that the job reaches completions")
		err = e2ejob.WaitForJobComplete(ctx, f.ClientSet, f.Namespace.Name, job.Name, batchv1.JobReasonSuccessPolicy, completions)
		framework.ExpectNoError(err, "failed to ensure that job completed")

		ginkgo.By("Verifying that the job status to ensure correct final state")
		job, err = e2ejob.GetJob(ctx, f.ClientSet, f.Namespace.Name, job.Name)
		framework.ExpectNoError(err, "failed to get latest job object")
		gomega.Expect(job.Status.Active).Should(gomega.Equal(int32(0)))
		gomega.Expect(job.Status.Ready).Should(gomega.Equal(ptr.To[int32](0)))
		gomega.Expect(job.Status.Terminating).Should(gomega.Equal(ptr.To[int32](0)))
		gomega.Expect(job.Status.Failed).Should(gomega.Equal(int32(0)))
	})

	/*
		发布版本：v1.33
		测试名称：Ensure that job with successPolicy succeededIndexes rule succeeded even when some indexes remain pending
		描述：创建一个带 successPolicy succeededIndexes 规则的 indexed job。
		验证即使部分 index 仍处于 pending，只要满足 successPolicy，
		job 就获得 SuccessCriteriaMet（原因为 SuccessPolicy）和 Complete condition。
	*/
	framework.ConformanceIt("with successPolicy succeededIndexes rule should succeeded even when some indexes remain pending", func(ctx context.Context) {
		parallelism := int32(2)
		completions := int32(5)
		backoffLimit := int32(6) // default value

		ginkgo.By("Creating an indexed job with successPolicy succeededIndexes rule")
		job := e2ejob.NewTestJob("failOddSucceedEven", "with-success-policy-succeeded-indexed", v1.RestartPolicyNever, parallelism, completions, nil, backoffLimit)
		job.Spec.CompletionMode = ptr.To(batchv1.IndexedCompletion)
		job.Spec.SuccessPolicy = &batchv1.SuccessPolicy{
			Rules: []batchv1.SuccessPolicyRule{{
				SucceededIndexes: ptr.To("0"),
			}},
		}
		job, err := e2ejob.CreateJob(ctx, f.ClientSet, f.Namespace.Name, job)
		framework.ExpectNoError(err, "failed to create job in namespace: %s", f.Namespace.Name)

		ginkgo.By("Awaiting for the job to have the interim SuccessCriteriaMet with SuccessPolicy reason condition")
		err = e2ejob.WaitForJobCondition(ctx, f.ClientSet, f.Namespace.Name, job.Name, batchv1.JobSuccessCriteriaMet, ptr.To(batchv1.JobReasonSuccessPolicy))
		framework.ExpectNoError(err, "failed to ensure that job has SuccessCriteriaMet with SuccessPolicy reason condition")

		ginkgo.By("Ensure that the job reaches completions")
		err = e2ejob.WaitForJobComplete(ctx, f.ClientSet, f.Namespace.Name, job.Name, batchv1.JobReasonSuccessPolicy, 1)
		framework.ExpectNoError(err, "failed to ensure that job completed")

		ginkgo.By("Verifying that the only appropriately index succeeded")
		job, err = e2ejob.GetJob(ctx, f.ClientSet, f.Namespace.Name, job.Name)
		framework.ExpectNoError(err, "failed to get the latest job object")
		gomega.Expect(job.Status.CompletedIndexes).Should(gomega.Equal("0"))
		gomega.Expect(job.Status.Active).Should(gomega.Equal(int32(0)))
		gomega.Expect(job.Status.Ready).Should(gomega.Equal(ptr.To[int32](0)))
		gomega.Expect(job.Status.Terminating).Should(gomega.Equal(ptr.To[int32](0)))
	})

	/*
		发布版本：v1.33
		测试名称：Ensure that job with successPolicy succeededCount rule succeeded even when some indexes remain pending
		描述：创建一个带 successPolicy succeededCount 规则的 indexed job。
		验证即使部分 index 仍处于 pending，只要满足 successPolicy，
		job 就获得 SuccessCriteriaMet（原因为 SuccessPolicy）和 Complete condition。
	*/
	framework.ConformanceIt("with successPolicy succeededCount rule should succeeded even when some indexes remain pending", func(ctx context.Context) {
		parallelism := int32(2)
		completions := int32(5)
		backoffLimit := int32(math.MaxInt32)

		ginkgo.By("Creating an indexed job with successPolicy succeededCount rule")
		job := e2ejob.NewTestJob("failOddSucceedEven", "with-success-policy-succeeded-count", v1.RestartPolicyNever, parallelism, completions, nil, backoffLimit)
		job.Spec.CompletionMode = ptr.To(batchv1.IndexedCompletion)
		job.Spec.SuccessPolicy = &batchv1.SuccessPolicy{
			Rules: []batchv1.SuccessPolicyRule{{
				SucceededCount: ptr.To[int32](1),
			}},
		}
		job, err := e2ejob.CreateJob(ctx, f.ClientSet, f.Namespace.Name, job)
		framework.ExpectNoError(err, "failed to create job in namespace: %s", f.Namespace.Name)

		ginkgo.By("Awaiting for the job to have the interim SuccessCriteriaMet condition with SuccessPolicy reason")
		err = e2ejob.WaitForJobCondition(ctx, f.ClientSet, f.Namespace.Name, job.Name, batchv1.JobSuccessCriteriaMet, ptr.To(batchv1.JobReasonSuccessPolicy))
		framework.ExpectNoError(err, "failed to ensure that the job has SuccessCriteriaMet condition with SuccessPolicy rule")

		ginkgo.By("Ensure that the job reaches completions")
		err = e2ejob.WaitForJobComplete(ctx, f.ClientSet, f.Namespace.Name, job.Name, batchv1.JobReasonSuccessPolicy, 1)
		framework.ExpectNoError(err, "failed to ensure that job completed")

		ginkgo.By("Verifying that the job status to ensure correct final state")
		job, err = e2ejob.GetJob(ctx, f.ClientSet, f.Namespace.Name, job.Name)
		framework.ExpectNoError(err, "failed to get the latest job object")
		gomega.Expect(job.Status.CompletedIndexes).Should(gomega.Equal("0"))
		gomega.Expect(job.Status.Active).Should(gomega.Equal(int32(0)))
		gomega.Expect(job.Status.Ready).Should(gomega.Equal(ptr.To[int32](0)))
		gomega.Expect(job.Status.Terminating).Should(gomega.Equal(ptr.To[int32](0)))
	})

	/*
		发布版本：v1.33
		测试名称：Ensure that all indexes are executed for an indexed job with backoffLimitPerIndex despite some failing
		描述：创建一个 indexed job，确保所有 index 都已失败或成功，
		取决于对应 pod 的终态。奇数 index 的 pod 失败，偶数 index 的
		成功。同时验证失败 pod 数量是失败 index 数量的两倍，因为
		backoffLimitPerIndex=1，允许在标记 index 失败前重建一次 pod。
	*/
	framework.ConformanceIt("should execute all indexes despite some failing when using backoffLimitPerIndex", func(ctx context.Context) {
		parallelism := int32(2)
		completions := int32(4)
		backoffLimit := int32(6) // default value

		ginkgo.By("Creating an indexed job with backoffLimit per index and failing pods")
		job := e2ejob.NewTestJob("failOddSucceedEven", "with-backoff-limit-per-index", v1.RestartPolicyNever, parallelism, completions, nil, backoffLimit)
		job.Spec.BackoffLimit = nil
		job.Spec.BackoffLimitPerIndex = ptr.To[int32](1)
		mode := batchv1.IndexedCompletion
		job.Spec.CompletionMode = &mode
		job, err := e2ejob.CreateJob(ctx, f.ClientSet, f.Namespace.Name, job)
		framework.ExpectNoError(err, "failed to create job in namespace: %s", f.Namespace.Name)

		ginkgo.By("Awaiting for the job to fail as there are failed indexes")
		err = e2ejob.WaitForJobFailed(ctx, f.ClientSet, f.Namespace.Name, job.Name)
		framework.ExpectNoError(err, "failed to ensure job completion in namespace: %s", f.Namespace.Name)

		ginkgo.By("Verifying the Job status fields to ensure all indexes were executed")
		job, err = e2ejob.GetJob(ctx, f.ClientSet, f.Namespace.Name, job.Name)
		framework.ExpectNoError(err, "failed to retrieve latest job object")
		gomega.Expect(job.Status.FailedIndexes).Should(gomega.HaveValue(gomega.Equal("1,3")))
		gomega.Expect(job.Status.CompletedIndexes).Should(gomega.Equal("0,2"))
		gomega.Expect(job.Status.Failed).Should(gomega.Equal(int32(4)))
		gomega.Expect(job.Status.Succeeded).Should(gomega.Equal(int32(2)))
	})

	/*
		发布版本：v1.33
		测试名称：Terminate job execution when the maxFailedIndexes is exceeded
		描述：创建一个带 backoffLimitPerIndex 和 maxFailedIndexes 的 indexed job。
		验证失败 index 数一旦超过 maxFailedIndexes，
		job 执行即被终止。
	*/
	framework.ConformanceIt("should terminate job execution when the number of failed indexes exceeds maxFailedIndexes", func(ctx context.Context) {
		// 使用 parallelism=1，确保断言中只创建了一个 pod
		parallelism := int32(1)
		completions := int32(4)
		backoffLimit := int32(6) // default value

		ginkgo.By("Creating an indexed job with backoffLimit per index and maxFailedIndexes")
		job := e2ejob.NewTestJob("fail", "with-max-failed-indexes", v1.RestartPolicyNever, parallelism, completions, nil, backoffLimit)
		job.Spec.BackoffLimit = nil
		job.Spec.BackoffLimitPerIndex = ptr.To[int32](0)
		job.Spec.MaxFailedIndexes = ptr.To[int32](0)

		mode := batchv1.IndexedCompletion
		job.Spec.CompletionMode = &mode
		job, err := e2ejob.CreateJob(ctx, f.ClientSet, f.Namespace.Name, job)
		framework.ExpectNoError(err, "failed to create job in namespace: %s", f.Namespace.Name)

		ginkgo.By("Awaiting for the job to fail as the number of max failed indexes is exceeded")
		err = e2ejob.WaitForJobFailed(ctx, f.ClientSet, f.Namespace.Name, job.Name)
		framework.ExpectNoError(err, "failed to ensure job completion in namespace: %s", f.Namespace.Name)

		ginkgo.By("Verifying the Job status fields to ensure early termination of the job")
		job, err = e2ejob.GetJob(ctx, f.ClientSet, f.Namespace.Name, job.Name)
		framework.ExpectNoError(err, "failed to retrieve latest job object")
		gomega.Expect(job.Status.FailedIndexes).Should(gomega.HaveValue(gomega.Equal("0")))
		gomega.Expect(job.Status.Failed).Should(gomega.Equal(int32(1)))
	})

	/*
		测试名称：Track the failure count per index in Pod annotation when backoffLimitPerIndex is used
		描述：创建一个 indexed job，确认 pod 重建时
		failure-count pod 注解被正确设置，
		反映该 index 到目前为止的失败次数。
	*/
	ginkgo.It("should record the failure-count in the Pod annotation when using backoffLimitPerIndex", func(ctx context.Context) {
		jobName := "e2e-backofflimitperindex-" + utilrand.String(5)
		label := map[string]string{batchv1.JobNameLabel: jobName}
		labelSelector := labels.SelectorFromSet(label).String()

		parallelism := int32(2)
		completions := int32(2)
		backoffLimit := int32(6) // default value

		job := e2ejob.NewTestJob("fail", jobName, v1.RestartPolicyNever, parallelism, completions, nil, backoffLimit)
		job.Spec.BackoffLimit = nil
		job.Spec.BackoffLimitPerIndex = ptr.To[int32](1)
		job.Spec.CompletionMode = ptr.To(batchv1.IndexedCompletion)

		tracker := NewIndexedPodAnnotationTracker(jobName, f.Namespace.Name, labelSelector, batchv1.JobCompletionIndexAnnotation, batchv1.JobIndexFailureCountAnnotation)
		trackerCancel := tracker.Start(ctx, f.ClientSet)
		ginkgo.DeferCleanup(trackerCancel)

		ginkgo.By("Creating an indexed job with backoffLimit per index and failing pods")
		job, err := e2ejob.CreateJob(ctx, f.ClientSet, f.Namespace.Name, job)
		framework.ExpectNoError(err, "failed to create job in namespace: %s", f.Namespace.Name)

		ginkgo.By("Awaiting for the job to fail as there are failed indexes")
		err = e2ejob.WaitForJobFailed(ctx, f.ClientSet, f.Namespace.Name, job.Name)
		framework.ExpectNoError(err, "failed to ensure job completion in namespace: %s", f.Namespace.Name)

		ginkgo.By("Verify the failure-count annotation on Pods")
		// 由于 Job 已经失败，所有相关 Pod 事件都
		// 已在分发中。但 Job controller 收到事件
		// 与测试代码之间仍可能有轻微延迟，
		// 因此需要稍等片刻。
		gomega.Eventually(ctx, tracker.cloneTrackedAnnotations).
			WithTimeout(15 * time.Second).
			WithPolling(500 * time.Millisecond).
			Should(gomega.Equal(map[int][]string{0: {"0", "1"}, 1: {"0", "1"}}))

		ginkgo.By("Verifying the Job status fields")
		job, err = e2ejob.GetJob(ctx, f.ClientSet, f.Namespace.Name, job.Name)
		framework.ExpectNoError(err, "failed to retrieve latest job object")
		gomega.Expect(job.Status.FailedIndexes).Should(gomega.HaveValue(gomega.Equal("0,1")))
		gomega.Expect(job.Status.CompletedIndexes).Should(gomega.Equal(""))
		gomega.Expect(job.Status.Failed).Should(gomega.Equal(int32(4)))
		gomega.Expect(job.Status.Succeeded).Should(gomega.Equal(int32(0)))
	})

	/*
		发布版本：v1.33
		测试名称：Mark indexes as failed when the FailIndex action is matched in podFailurePolicy
		描述：创建一个带 backoffLimitPerIndex 的 indexed job，以及带
		FailIndex action 的 podFailurePolicy。验证匹配 pod failure policy
		的失败 pod 会让对应 index 直接标记为失败而不重启，即使
		backoffLimitPerIndex > 0。
	*/
	framework.ConformanceIt("should mark indexes as failed when the FailIndex action is matched in podFailurePolicy", func(ctx context.Context) {
		parallelism := int32(2)
		completions := int32(2)
		backoffLimit := int32(6) // default value

		ginkgo.By("Creating an indexed job with failing pods matching the FailIndex action")
		job := e2ejob.NewTestJob("failOddSucceedEven", "matching-fail-index-action", v1.RestartPolicyNever, parallelism, completions, nil, backoffLimit)
		job.Spec.BackoffLimit = nil
		job.Spec.BackoffLimitPerIndex = ptr.To[int32](1)
		job.Spec.PodFailurePolicy = &batchv1.PodFailurePolicy{
			Rules: []batchv1.PodFailurePolicyRule{
				{
					Action: batchv1.PodFailurePolicyActionFailIndex,
					OnExitCodes: &batchv1.PodFailurePolicyOnExitCodesRequirement{
						Operator: batchv1.PodFailurePolicyOnExitCodesOpIn,
						Values:   []int32{1},
					},
				},
			},
		}
		mode := batchv1.IndexedCompletion
		job.Spec.CompletionMode = &mode
		job, err := e2ejob.CreateJob(ctx, f.ClientSet, f.Namespace.Name, job)
		framework.ExpectNoError(err, "failed to create job in namespace: %s", f.Namespace.Name)

		ginkgo.By("Awaiting for the job to fail as all indexes are failed")
		err = e2ejob.WaitForJobFailed(ctx, f.ClientSet, f.Namespace.Name, job.Name)
		framework.ExpectNoError(err, "failed to ensure job completion in namespace: %s", f.Namespace.Name)

		ginkgo.By("Verifying the Job status fields to ensure the upper indexes didn't execute")
		job, err = e2ejob.GetJob(ctx, f.ClientSet, f.Namespace.Name, job.Name)
		framework.ExpectNoError(err, "failed to retrieve latest job object")
		gomega.Expect(job.Status.FailedIndexes).Should(gomega.HaveValue(gomega.Equal("1")))
		gomega.Expect(job.Status.CompletedIndexes).Should(gomega.Equal("0"))
		gomega.Expect(job.Status.Failed).Should(gomega.Equal(int32(1)))
		gomega.Expect(job.Status.Succeeded).Should(gomega.Equal(int32(1)))
	})

	/*
		测试名称：Ensure that the pods associated with the job are removed once the job is deleted
		描述：创建一个 job，确认相关 pod 数等于 parallelism。删除
		job，确认相关 pod 已被移除
	*/
	ginkgo.It("should remove pods when job is deleted", func(ctx context.Context) {
		parallelism := int32(2)
		completions := int32(4)
		backoffLimit := int32(6) // default value

		ginkgo.By("Creating a job")
		job := e2ejob.NewTestJob("notTerminate", "all-pods-removed", v1.RestartPolicyNever, parallelism, completions, nil, backoffLimit)
		job, err := e2ejob.CreateJob(ctx, f.ClientSet, f.Namespace.Name, job)
		framework.ExpectNoError(err, "failed to create job in namespace: %s", f.Namespace.Name)

		ginkgo.By("Ensure pods equal to parallelism count is attached to the job")
		err = e2ejob.WaitForJobPodsRunning(ctx, f.ClientSet, f.Namespace.Name, job.Name, parallelism)
		framework.ExpectNoError(err, "failed to ensure number of pods associated with job %s is equal to parallelism count in namespace: %s", job.Name, f.Namespace.Name)

		ginkgo.By("Delete the job")
		err = e2eresource.DeleteResourceAndWaitForGC(ctx, f.ClientSet, batchinternal.Kind("Job"), f.Namespace.Name, job.Name)
		framework.ExpectNoError(err, "failed to delete the job in namespace: %s", f.Namespace.Name)

		ginkgo.By("Ensure the pods associated with the job are also deleted")
		err = e2ejob.WaitForAllJobPodsGone(ctx, f.ClientSet, f.Namespace.Name, job.Name)
		framework.ExpectNoError(err, "failed to get PodList for job %s in namespace: %s", job.Name, f.Namespace.Name)
	})

	/*
		发布版本：v1.16
		测试名称：Jobs, completion after task failure
		描述：显式让任务初次失败一次。重启后，Job 必须
		执行直到完成。
	*/
	framework.ConformanceIt("should run a job to completion when tasks sometimes fail and are locally restarted", func(ctx context.Context) {
		parallelism := int32(2)
		completions := int32(4)
		backoffLimit := int32(6) // default value

		ginkgo.By("Creating a job")
		// 先失败一次，再成功，本地重启。
		// 不能用随机失败的方式，因为 kubelet 会
		// 对同一 pod 中频繁失败的容器进行限流，
		// 重启间隔最长增至 5 分钟，在合理的测试超时下
		// 连续失败太容易导致超时。
		job := e2ejob.NewTestJob("failOnce", "fail-once-local", v1.RestartPolicyOnFailure, parallelism, completions, nil, backoffLimit)
		job, err := e2ejob.CreateJob(ctx, f.ClientSet, f.Namespace.Name, job)
		framework.ExpectNoError(err, "failed to create job in namespace: %s", f.Namespace.Name)

		ginkgo.By("Ensuring job reaches completions")
		err = e2ejob.WaitForJobComplete(ctx, f.ClientSet, f.Namespace.Name, job.Name, batchv1.JobReasonCompletionsReached, completions)
		framework.ExpectNoError(err, "failed to ensure job completion in namespace: %s", f.Namespace.Name)
	})

	// pod 有时会失败，但 pod 重启后最终会成功
	ginkgo.It("should run a job to completion when tasks sometimes fail and are not locally restarted", func(ctx context.Context) {
		// 先失败一次，再成功，不做本地重启。
		// 不能用随机失败的方式，因为 JobController
		// 会对同一 Job 中频繁失败的 Pod 进行限流，
		// 重启间隔最长增至 6 分钟，导致连续失败时超时。
		// successive failures.
		// 改为强制 Job 的 Pod 调度到单个 Node，
		// 并用 hostPath volume 在新 Pod 之间持久化数据。
		ginkgo.By("Looking for a node to schedule job pod")
		node, err := e2enode.GetRandomReadySchedulableNode(ctx, f.ClientSet)
		framework.ExpectNoError(err)

		parallelism := int32(2)
		completions := int32(4)
		backoffLimit := int32(6) // default value

		ginkgo.By("Creating a job")
		job := e2ejob.NewTestJobOnNode("failOnce", "fail-once-non-local", v1.RestartPolicyNever, parallelism, completions, nil, backoffLimit, node.Name)
		job, err = e2ejob.CreateJob(ctx, f.ClientSet, f.Namespace.Name, job)
		framework.ExpectNoError(err, "failed to create job in namespace: %s", f.Namespace.Name)

		ginkgo.By("Awaiting for the job to have the interim success condition")
		err = e2ejob.WaitForJobCondition(ctx, f.ClientSet, f.Namespace.Name, job.Name, batchv1.JobSuccessCriteriaMet, ptr.To(batchv1.JobReasonCompletionsReached))
		framework.ExpectNoError(err, "failed to ensure job has the interim success condition: %s", f.Namespace.Name)

		ginkgo.By("Ensuring job reaches completions")
		err = e2ejob.WaitForJobComplete(ctx, f.ClientSet, f.Namespace.Name, job.Name, batchv1.JobReasonCompletionsReached, *job.Spec.Completions)
		framework.ExpectNoError(err, "failed to ensure job completion in namespace: %s", f.Namespace.Name)

		ginkgo.By("Verifying the Job status fields to ensure correct final state")
		job, err = e2ejob.GetJob(ctx, f.ClientSet, f.Namespace.Name, job.Name)
		framework.ExpectNoError(err, "failed to retrieve latest job object")
		gomega.Expect(job.Status.Active).Should(gomega.Equal(int32(0)))
		gomega.Expect(job.Status.Ready).Should(gomega.Equal(ptr.To[int32](0)))
		gomega.Expect(job.Status.Terminating).Should(gomega.Equal(ptr.To[int32](0)))
	})

	ginkgo.It("should fail when exceeds active deadline", func(ctx context.Context) {
		activeDeadlineSeconds := int64(1)
		parallelism := int32(2)
		completions := int32(4)
		backoffLimit := int32(6) // default value

		ginkgo.By("Creating a job")
		job := e2ejob.NewTestJob("notTerminate", "exceed-active-deadline", v1.RestartPolicyNever, parallelism, completions, &activeDeadlineSeconds, backoffLimit)
		job, err := e2ejob.CreateJob(ctx, f.ClientSet, f.Namespace.Name, job)
		framework.ExpectNoError(err, "failed to create job in namespace: %s", f.Namespace.Name)

		ginkgo.By("Awaiting for the job to have the interim failure condition")
		err = e2ejob.WaitForJobCondition(ctx, f.ClientSet, f.Namespace.Name, job.Name, batchv1.JobFailureTarget, ptr.To(batchv1.JobReasonDeadlineExceeded))
		framework.ExpectNoError(err, "failed to ensure job has the interim failure condition: %s", f.Namespace.Name)

		ginkgo.By("Ensuring job past active deadline")
		err = e2ejob.WaitForJobCondition(ctx, f.ClientSet, f.Namespace.Name, job.Name, batchv1.JobFailed, ptr.To(batchv1.JobReasonDeadlineExceeded))
		framework.ExpectNoError(err, "failed to ensure job past active deadline in namespace: %s", f.Namespace.Name)

		ginkgo.By("Verifying the Job status fields to ensure correct final state")
		job, err = e2ejob.GetJob(ctx, f.ClientSet, f.Namespace.Name, job.Name)
		framework.ExpectNoError(err, "failed to retrieve latest job object")
		gomega.Expect(job.Status.Active).Should(gomega.Equal(int32(0)))
		gomega.Expect(job.Status.Ready).Should(gomega.Equal(ptr.To[int32](0)))
		gomega.Expect(job.Status.Terminating).Should(gomega.Equal(ptr.To[int32](0)))
	})

	/*
		发布版本：v1.15
		测试名称：Jobs, active pods, graceful termination
		描述：创建一个 job。确认 active pod 数量反映 namespace 内的 parallelism 并删除 job。Job 必须删除成功。
	*/
	framework.ConformanceIt("should delete a job", func(ctx context.Context) {
		parallelism := int32(2)
		completions := int32(4)
		backoffLimit := int32(6) // default value

		ginkgo.By("Creating a job")
		job := e2ejob.NewTestJob("notTerminate", "foo", v1.RestartPolicyNever, parallelism, completions, nil, backoffLimit)
		job, err := e2ejob.CreateJob(ctx, f.ClientSet, f.Namespace.Name, job)
		framework.ExpectNoError(err, "failed to create job in namespace: %s", f.Namespace.Name)

		ginkgo.By("Ensuring active pods == parallelism")
		err = e2ejob.WaitForJobPodsRunning(ctx, f.ClientSet, f.Namespace.Name, job.Name, parallelism)
		framework.ExpectNoError(err, "failed to ensure active pods == parallelism in namespace: %s", f.Namespace.Name)

		ginkgo.By("delete a job")
		framework.ExpectNoError(e2eresource.DeleteResourceAndWaitForGC(ctx, f.ClientSet, batchinternal.Kind("Job"), f.Namespace.Name, job.Name))

		ginkgo.By("Ensuring job was deleted")
		_, err = e2ejob.GetJob(ctx, f.ClientSet, f.Namespace.Name, job.Name)
		gomega.Expect(err).To(gomega.MatchError(apierrors.IsNotFound, fmt.Sprintf("failed to ensure job %s was deleted in namespace: %s", job.Name, f.Namespace.Name)))
	})

	/*
		发布版本：v1.16
		测试名称：Jobs, orphan pods, re-adoption
		描述：创建一个并行 job。Pod 数量必须等于 parallelism 级别。
		修改某个 Pod 的 owner reference 使其成为孤儿。Job 必须重新收养该孤儿 pod。
		修改 Job 某个 Pod 的标签。Job 必须释放该 Pod。
	*/
	framework.ConformanceIt("should adopt matching orphans and release non-matching pods", func(ctx context.Context) {
		parallelism := int32(2)
		completions := int32(4)
		backoffLimit := int32(6) // default value

		ginkgo.By("Creating a job")
		job := e2ejob.NewTestJob("notTerminate", "adopt-release", v1.RestartPolicyNever, parallelism, completions, nil, backoffLimit)
		// 用 Create() 返回的 job 替换原对象，以获取 UID。
		// 保存 Kind，因为返回的 job 中不会填充它。
		kind := job.Kind
		job, err := e2ejob.CreateJob(ctx, f.ClientSet, f.Namespace.Name, job)
		framework.ExpectNoError(err, "failed to create job in namespace: %s", f.Namespace.Name)
		job.Kind = kind

		ginkgo.By("Ensuring active pods == parallelism")
		err = e2ejob.WaitForJobPodsRunning(ctx, f.ClientSet, f.Namespace.Name, job.Name, parallelism)
		framework.ExpectNoError(err, "failed to ensure active pods == parallelism in namespace: %s", f.Namespace.Name)

		ginkgo.By("Orphaning one of the Job's Pods")
		pods, err := e2ejob.GetJobPods(ctx, f.ClientSet, f.Namespace.Name, job.Name)
		framework.ExpectNoError(err, "failed to get PodList for job %s in namespace: %s", job.Name, f.Namespace.Name)
		gomega.Expect(pods.Items).To(gomega.HaveLen(int(parallelism)))
		pod := pods.Items[0]
		e2epod.NewPodClient(f).Update(ctx, pod.Name, func(pod *v1.Pod) {
			pod.OwnerReferences = nil
		})

		ginkgo.By("Checking that the Job readopts the Pod")
		gomega.Expect(e2epod.WaitForPodCondition(ctx, f.ClientSet, pod.Namespace, pod.Name, "adopted", e2ejob.JobTimeout,
			func(pod *v1.Pod) (bool, error) {
				controllerRef := metav1.GetControllerOf(pod)
				if controllerRef == nil {
					return false, nil
				}
				if controllerRef.Kind != job.Kind || controllerRef.Name != job.Name || controllerRef.UID != job.UID {
					return false, fmt.Errorf("pod has wrong controllerRef: got %v, want %v", controllerRef, job)
				}
				return true, nil
			},
		)).To(gomega.Succeed(), "wait for pod %q to be readopted", pod.Name)

		ginkgo.By("Removing the labels from the Job's Pod")
		e2epod.NewPodClient(f).Update(ctx, pod.Name, func(pod *v1.Pod) {
			pod.Labels = nil
		})

		ginkgo.By("Checking that the Job releases the Pod")
		gomega.Expect(e2epod.WaitForPodCondition(ctx, f.ClientSet, pod.Namespace, pod.Name, "released", e2ejob.JobTimeout,
			func(pod *v1.Pod) (bool, error) {
				controllerRef := metav1.GetControllerOf(pod)
				if controllerRef != nil {
					return false, nil
				}
				return true, nil
			},
		)).To(gomega.Succeed(), "wait for pod %q to be released", pod.Name)
	})

	ginkgo.It("should fail to exceed backoffLimit", func(ctx context.Context) {
		ginkgo.By("Creating a job")
		backoff := 1
		job := e2ejob.NewTestJob("fail", "backofflimit", v1.RestartPolicyNever, 1, 1, nil, int32(backoff))
		job, err := e2ejob.CreateJob(ctx, f.ClientSet, f.Namespace.Name, job)
		framework.ExpectNoError(err, "failed to create job in namespace: %s", f.Namespace.Name)

		ginkgo.By("Awaiting for the job to have the interim failure condition")
		err = e2ejob.WaitForJobCondition(ctx, f.ClientSet, f.Namespace.Name, job.Name, batchv1.JobFailureTarget, ptr.To(batchv1.JobReasonBackoffLimitExceeded))
		framework.ExpectNoError(err, "failed to ensure job has the interim failure condition: %s", f.Namespace.Name)

		ginkgo.By("Ensuring job exceed backofflimit")
		err = e2ejob.WaitForJobCondition(ctx, f.ClientSet, f.Namespace.Name, job.Name, batchv1.JobFailed, ptr.To(batchv1.JobReasonBackoffLimitExceeded))
		framework.ExpectNoError(err, "failed to ensure job exceed backofflimit in namespace: %s", f.Namespace.Name)

		ginkgo.By(fmt.Sprintf("Checking that %d pod created and status is failed", backoff+1))
		pods, err := e2ejob.GetJobPods(ctx, f.ClientSet, f.Namespace.Name, job.Name)
		framework.ExpectNoError(err, "failed to get PodList for job %s in namespace: %s", job.Name, f.Namespace.Name)
		gomega.Expect(pods.Items).To(gomega.HaveLen(backoff + 1))
		for _, pod := range pods.Items {
			gomega.Expect(pod.Status.Phase).To(gomega.Equal(v1.PodFailed))
		}

		ginkgo.By("Verifying the Job status fields to ensure correct final state")
		job, err = e2ejob.GetJob(ctx, f.ClientSet, f.Namespace.Name, job.Name)
		framework.ExpectNoError(err, "failed to retrieve latest job object")
		gomega.Expect(job.Status.Active).Should(gomega.Equal(int32(0)))
		gomega.Expect(job.Status.Ready).Should(gomega.Equal(ptr.To[int32](0)))
		gomega.Expect(job.Status.Terminating).Should(gomega.Equal(ptr.To[int32](0)))
	})

	f.It("should run a job to completion with CPU requests", f.WithSerial(), func(ctx context.Context) {
		ginkgo.By("Creating a job that with CPU requests")

		testNodeName := scheduling.GetNodeThatCanRunPod(ctx, f)
		targetNode, err := f.ClientSet.CoreV1().Nodes().Get(ctx, testNodeName, metav1.GetOptions{})
		framework.ExpectNoError(err, "unable to get node object for node %v", testNodeName)

		cpu, ok := targetNode.Status.Allocatable[v1.ResourceCPU]
		if !ok {
			framework.Failf("Unable to get node's %q cpu", targetNode.Name)
		}

		cpuRequest := fmt.Sprint(int64(0.2 * float64(cpu.Value())))

		parallelism := int32(90)
		completions := int32(90)
		backoffLimit := int32(0)

		ginkgo.By("Creating a job")
		job := e2ejob.NewTestJob("succeed", "all-succeed", v1.RestartPolicyNever, parallelism, completions, nil, backoffLimit)
		for i := range job.Spec.Template.Spec.Containers {
			job.Spec.Template.Spec.Containers[i].Resources = v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceCPU: resource.MustParse(cpuRequest),
				},
			}
			job.Spec.Template.Spec.NodeSelector = map[string]string{"kubernetes.io/hostname": testNodeName}
		}

		framework.Logf("Creating job %q with a node hostname selector %q with cpu request %q", job.Name, testNodeName, cpuRequest)
		job, err = e2ejob.CreateJob(ctx, f.ClientSet, f.Namespace.Name, job)
		framework.ExpectNoError(err, "failed to create job in namespace: %s", f.Namespace.Name)

		ginkgo.By("Ensuring job reaches completions")
		err = e2ejob.WaitForJobComplete(ctx, f.ClientSet, f.Namespace.Name, job.Name, batchv1.JobReasonCompletionsReached, completions)
		framework.ExpectNoError(err, "failed to ensure job completion in namespace: %s", f.Namespace.Name)

		ginkgo.By("Ensuring pods for job exist")
		pods, err := e2ejob.GetJobPods(ctx, f.ClientSet, f.Namespace.Name, job.Name)
		framework.ExpectNoError(err, "failed to get pod list for job in namespace: %s", f.Namespace.Name)
		successes := int32(0)
		for _, pod := range pods.Items {
			if pod.Status.Phase == v1.PodSucceeded {
				successes++
			}
		}
		gomega.Expect(successes).To(gomega.Equal(completions), "expected %d successful job pods, but got  %d", completions, successes)
	})

	/*
		发布版本：v1.24
		测试名称：Jobs, apply changes to status
		描述：尝试创建一个运行中的 Job，必须成功。
		尝试 patch Job status，必须成功。
		必须能找到被 patch 的 job 注解。
		尝试用 update 替换 job status，必须成功。
		尝试读取其 status 子资源，必须成功
	*/
	framework.ConformanceIt("should apply changes to a job status", func(ctx context.Context) {

		ns := f.Namespace.Name
		jClient := f.ClientSet.BatchV1().Jobs(ns)

		parallelism := int32(2)
		completions := int32(4)
		backoffLimit := int32(6) // default value

		ginkgo.By("Creating a job")
		job := e2ejob.NewTestJob("notTerminate", "suspend-false-to-true", v1.RestartPolicyNever, parallelism, completions, nil, backoffLimit)
		job, err := e2ejob.CreateJob(ctx, f.ClientSet, f.Namespace.Name, job)
		framework.ExpectNoError(err, "failed to create job in namespace: %s", f.Namespace.Name)

		ginkgo.By("Ensure pods equal to parallelism count is attached to the job")
		err = e2ejob.WaitForJobPodsRunning(ctx, f.ClientSet, f.Namespace.Name, job.Name, parallelism)
		framework.ExpectNoError(err, "failed to ensure number of pods associated with job %s is equal to parallelism count in namespace: %s", job.Name, f.Namespace.Name)

		customConditionType := batchv1.JobConditionType("CustomConditionType")
		// /status 子资源操作
		ginkgo.By("patching /status")
		// 需要使用 RFC3339 版本，因为网络传输的转换会截掉纳秒
		now1 := metav1.Now().Rfc3339Copy()
		jStatus := batchv1.JobStatus{
			Conditions: []batchv1.JobCondition{
				{
					Type:               customConditionType,
					Status:             v1.ConditionTrue,
					LastTransitionTime: now1,
				},
			},
		}

		jStatusJSON, err := json.Marshal(jStatus)
		framework.ExpectNoError(err)
		patchedStatus, err := jClient.Patch(ctx, job.Name, types.MergePatchType,
			[]byte(`{"metadata":{"annotations":{"patchedstatus":"true"}},"status":`+string(jStatusJSON)+`}`),
			metav1.PatchOptions{}, "status")
		framework.ExpectNoError(err)
		if condition := findConditionByType(patchedStatus.Status.Conditions, customConditionType); condition != nil {
			if !condition.LastTransitionTime.Equal(&now1) {
				framework.Failf("patched object should have the applied condition with LastTransitionTime %#v, got %#v instead", now1, condition.LastTransitionTime)
			}
		} else {
			framework.Failf("patched object does not have the required condition %v", customConditionType)
		}
		gomega.Expect(patchedStatus.Annotations).To(gomega.HaveKeyWithValue("patchedstatus", "true"), "patched object should have the applied annotation")

		ginkgo.By("updating /status")
		// 需要使用 RFC3339 版本，因为网络传输的转换会截掉纳秒
		now2 := metav1.Now().Rfc3339Copy()
		var statusToUpdate, updatedStatus *batchv1.Job
		err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
			statusToUpdate, err = jClient.Get(ctx, job.Name, metav1.GetOptions{})
			if err != nil {
				return err
			}
			if condition := findConditionByType(statusToUpdate.Status.Conditions, customConditionType); condition != nil {
				condition.LastTransitionTime = now2
			} else {
				framework.Failf("patched object does not have the required condition %v", customConditionType)
			}
			updatedStatus, err = jClient.UpdateStatus(ctx, statusToUpdate, metav1.UpdateOptions{})
			return err
		})
		framework.ExpectNoError(err)
		if condition := findConditionByType(updatedStatus.Status.Conditions, customConditionType); condition != nil {
			if !condition.LastTransitionTime.Equal(&now2) {
				framework.Failf("patched object should have the applied condition with LastTransitionTime %#v, got %#v instead", now2, condition.LastTransitionTime)
			}
		} else {
			framework.Failf("patched object does not have the required condition %v", customConditionType)
		}

		ginkgo.By("get /status")
		jResource := schema.GroupVersionResource{Group: "batch", Version: "v1", Resource: "jobs"}
		gottenStatus, err := f.DynamicClient.Resource(jResource).Namespace(ns).Get(ctx, job.Name, metav1.GetOptions{}, "status")
		framework.ExpectNoError(err)
		statusUID, _, err := unstructured.NestedFieldCopy(gottenStatus.Object, "metadata", "uid")
		framework.ExpectNoError(err)
		gomega.Expect(string(job.UID)).To(gomega.Equal(statusUID), fmt.Sprintf("job.UID: %v expected to match statusUID: %v ", job.UID, statusUID))
	})

	/*
		发布版本：v1.25
		测试名称：Jobs, manage lifecycle
		描述：尝试创建一个挂起的 Job，必须成功。
		尝试 patch Job 添加新标签，必须成功。
		必须能找到该标签。尝试替换 Job 添加
		新注解，必须成功。必须能找到该注解。
		尝试用 label selector 列出所有 namespace，必须
		成功。必须找到一个列表项。必须能通过 label selector
		成功删除一组 job。
	*/
	framework.ConformanceIt("should manage the lifecycle of a job", func(ctx context.Context) {
		jobName := "e2e-" + utilrand.String(5)
		label := map[string]string{"e2e-job-label": jobName}
		labelSelector := labels.SelectorFromSet(label).String()

		ns := f.Namespace.Name
		jobClient := f.ClientSet.BatchV1().Jobs(ns)

		w := &cache.ListWatch{
			WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
				options.LabelSelector = labelSelector
				return jobClient.Watch(ctx, options)
			},
		}
		jobsList, err := jobClient.List(ctx, metav1.ListOptions{LabelSelector: labelSelector})
		framework.ExpectNoError(err, "failed to list Job")

		parallelism := int32(2)
		completions := int32(4)
		backoffLimit := int32(6) // default value

		ginkgo.By("Creating a suspended job")
		job := e2ejob.NewTestJob("succeed", jobName, v1.RestartPolicyNever, parallelism, completions, nil, backoffLimit)
		job.Labels = label
		job.Spec.Suspend = ptr.To(true)
		job, err = e2ejob.CreateJob(ctx, f.ClientSet, ns, job)
		framework.ExpectNoError(err, "failed to create job in namespace: %s", ns)
		gomega.Expect(job).To(apimachineryutils.HaveValidResourceVersion())

		ginkgo.By("Patching the Job")
		payload := "{\"metadata\":{\"labels\":{\"" + jobName + "\":\"patched\"}}}"
		patchedJob, err := f.ClientSet.BatchV1().Jobs(ns).Patch(ctx, jobName, types.StrategicMergePatchType, []byte(payload), metav1.PatchOptions{})
		framework.ExpectNoError(err, "failed to patch Job %s in namespace %s", jobName, ns)
		gomega.Expect(resourceversion.CompareResourceVersion(job.ResourceVersion, patchedJob.ResourceVersion)).To(gomega.BeNumerically("==", -1), "patched object should have a larger resource version")

		ginkgo.By("Watching for Job to be patched")
		c := watchEventConfig{
			framework:           f,
			resourceVersion:     jobsList.ResourceVersion,
			w:                   w,
			jobName:             jobName,
			watchEvent:          watch.Modified,
			extJob:              patchedJob,
			updatedMetadataType: "label",
			updatedKey:          jobName,
			updatedValue:        "patched",
		}
		waitForJobEvent(ctx, c)
		gomega.Expect(patchedJob.Labels).To(gomega.HaveKeyWithValue(jobName, "patched"), "Did not find job label for this job. Current labels: %v", patchedJob.Labels)

		ginkgo.By("Updating the job")
		var updatedJob *batchv1.Job

		err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
			patchedJob, err = jobClient.Get(ctx, jobName, metav1.GetOptions{})
			framework.ExpectNoError(err, "Unable to get job %s", jobName)
			patchedJob.Spec.Suspend = ptr.To(false)
			if patchedJob.Annotations == nil {
				patchedJob.Annotations = map[string]string{}
			}
			patchedJob.Annotations["updated"] = "true"
			updatedJob, err = e2ejob.UpdateJob(ctx, f.ClientSet, ns, patchedJob)
			return err
		})
		framework.ExpectNoError(err, "failed to update job in namespace: %s", ns)

		ginkgo.By("Watching for Job to be updated")
		c = watchEventConfig{
			framework:           f,
			resourceVersion:     patchedJob.ResourceVersion,
			w:                   w,
			jobName:             jobName,
			watchEvent:          watch.Modified,
			extJob:              updatedJob,
			updatedMetadataType: "annotation",
			updatedKey:          "updated",
			updatedValue:        "true",
		}
		waitForJobEvent(ctx, c)
		gomega.Expect(updatedJob.Annotations).To(gomega.HaveKeyWithValue("updated", "true"), "updated Job should have the applied annotation")
		framework.Logf("Found Job annotations: %#v", patchedJob.Annotations)

		ginkgo.By("Listing all Jobs with LabelSelector")
		jobs, err := f.ClientSet.BatchV1().Jobs("").List(ctx, metav1.ListOptions{LabelSelector: labelSelector})
		framework.ExpectNoError(err, "Failed to list job. %v", err)
		gomega.Expect(jobs.Items).To(gomega.HaveLen(1), "Failed to find job %v", jobName)
		testJob := jobs.Items[0]
		framework.Logf("Job: %v as labels: %v", testJob.Name, testJob.Labels)

		ginkgo.By("Waiting for job to complete")
		err = e2ejob.WaitForJobComplete(ctx, f.ClientSet, ns, jobName, batchv1.JobReasonCompletionsReached, completions)
		framework.ExpectNoError(err, "failed to ensure job completion in namespace: %s", ns)

		ginkgo.By("Delete a job collection with a labelselector")
		propagationPolicy := metav1.DeletePropagationBackground
		err = f.ClientSet.BatchV1().Jobs(ns).DeleteCollection(ctx, metav1.DeleteOptions{PropagationPolicy: &propagationPolicy}, metav1.ListOptions{LabelSelector: labelSelector})
		framework.ExpectNoError(err, "failed to delete job %s in namespace: %s", job.Name, ns)

		ginkgo.By("Watching for Job to be deleted")
		c = watchEventConfig{
			framework:           f,
			resourceVersion:     updatedJob.ResourceVersion,
			w:                   w,
			jobName:             jobName,
			watchEvent:          watch.Deleted,
			extJob:              &testJob,
			updatedMetadataType: "label",
			updatedKey:          "e2e-job-label",
			updatedValue:        jobName,
		}
		waitForJobEvent(ctx, c)

		ginkgo.By("Relist jobs to confirm deletion")
		jobs, err = f.ClientSet.BatchV1().Jobs("").List(ctx, metav1.ListOptions{LabelSelector: labelSelector})
		framework.ExpectNoError(err, "Failed to list job. %v", err)
		gomega.Expect(jobs.Items).To(gomega.BeEmpty(), "Found job %v", jobName)
	})

	ginkgo.It("should update the status ready field", func(ctx context.Context) {
		parallelism := int32(2)
		completions := int32(4)
		backoffLimit := int32(6) // default value

		ginkgo.By("Creating a job with suspend=true")
		job := e2ejob.NewTestJob("notTerminate", "all-ready", v1.RestartPolicyNever, parallelism, completions, nil, backoffLimit)
		job.Spec.Suspend = ptr.To[bool](true)
		job, err := e2ejob.CreateJob(ctx, f.ClientSet, f.Namespace.Name, job)
		framework.ExpectNoError(err, "failed to create job in namespace: %s", f.Namespace.Name)

		ginkgo.By("Ensure the job controller updates the status.ready field")
		err = e2ejob.WaitForJobReady(ctx, f.ClientSet, f.Namespace.Name, job.Name, ptr.To[int32](0))
		framework.ExpectNoError(err, "failed to ensure job status ready field in namespace: %s", f.Namespace.Name)

		ginkgo.By("Updating the job with suspend=false")
		err = updateJobSuspendWithRetries(ctx, f, job, ptr.To[bool](false))
		framework.ExpectNoError(err, "failed to update job in namespace: %s", f.Namespace.Name)

		ginkgo.By("Ensure the job controller updates the status.ready field")
		err = e2ejob.WaitForJobReady(ctx, f.ClientSet, f.Namespace.Name, job.Name, &parallelism)
		framework.ExpectNoError(err, "failed to ensure job status ready field in namespace: %s", f.Namespace.Name)

		ginkgo.By("Updating the job with suspend=true")
		err = updateJobSuspendWithRetries(ctx, f, job, ptr.To[bool](true))
		framework.ExpectNoError(err, "failed to update job in namespace: %s", f.Namespace.Name)

		ginkgo.By("Ensure the job controller updates the status.ready field")
		err = e2ejob.WaitForJobReady(ctx, f.ClientSet, f.Namespace.Name, job.Name, ptr.To[int32](0))
		framework.ExpectNoError(err, "failed to ensure job status ready field in namespace: %s", f.Namespace.Name)
	})

	/*
		测试名称：Jobs, managed-by mechanism
		描述：本测试验证内置 Job controller 不会
		协调该 Job，从而把协调工作交给
		外部 controller。
	*/
	ginkgo.It("should allow to delegate reconciliation to external controller", func(ctx context.Context) {
		parallelism := int32(2)
		completions := int32(4)
		backoffLimit := int32(6)

		ginkgo.By("Creating a job with a custom managed-by field")
		job := e2ejob.NewTestJob("succeed", "managed-by", v1.RestartPolicyNever, parallelism, completions, nil, backoffLimit)
		job.Spec.ManagedBy = ptr.To("example.com/custom-job-controller")
		job, err := e2ejob.CreateJob(ctx, f.ClientSet, f.Namespace.Name, job)
		framework.ExpectNoError(err, "failed to create job in namespace: %s/%s", job.Namespace, job.Name)

		ginkgo.By(fmt.Sprintf("Verify the Job %s/%s status isn't updated by the built-in controller", job.Namespace, job.Name))
		// 这个 get 函数使用 HandleRetry 对瞬时 API 错误进行重试
		get := framework.GetObject(f.ClientSet.BatchV1().Jobs(f.Namespace.Name).Get, job.Name, metav1.GetOptions{})
		gomega.Consistently(ctx, get).
			WithPolling(time.Second).WithTimeout(3 * time.Second).
			Should(gomega.HaveField("Status", gomega.BeEquivalentTo(batchv1.JobStatus{})))
	})

	framework.It("containers restarted by container restart policy should not trigger PodFailurePolicy", framework.WithNodeConformance(), framework.WithFeatureGate(features.ContainerRestartRules), func(ctx context.Context) {
		parallelism := int32(1)
		completions := int32(1)
		backoffLimit := int32(1)
		containerRestartPolicyOnFailure := v1.ContainerRestartPolicyOnFailure

		ginkgo.By("Looking for a node to schedule job pod")
		node, err := e2enode.GetRandomReadySchedulableNode(ctx, f.ClientSet)
		framework.ExpectNoError(err)

		ginkgo.By("Creating a job with container-level RestartPolicy and PodFailurePolicy")
		job := e2ejob.NewTestJobOnNode("failOnce", "managed-by", v1.RestartPolicyNever, parallelism, completions, nil, backoffLimit, node.Name)
		container := job.Spec.Template.Spec.Containers[0]
		container.RestartPolicy = &containerRestartPolicyOnFailure
		job.Spec.Template.Spec.Containers[0] = container
		job.Spec.PodFailurePolicy = &batchv1.PodFailurePolicy{
			Rules: []batchv1.PodFailurePolicyRule{{
				Action: batchv1.PodFailurePolicyActionFailJob,
				OnExitCodes: &batchv1.PodFailurePolicyOnExitCodesRequirement{
					ContainerName: &container.Name,
					Operator:      batchv1.PodFailurePolicyOnExitCodesOpIn,
					Values:        []int32{1},
				},
			}},
		}
		job, err = e2ejob.CreateJob(ctx, f.ClientSet, f.Namespace.Name, job)
		framework.ExpectNoError(err, "failed to create job in namespace: %s/%s", job.Namespace, job.Name)

		ginkgo.By("Waiting for job to complete")
		err = e2ejob.WaitForJobComplete(ctx, f.ClientSet, f.Namespace.Name, job.Name, batchv1.JobReasonCompletionsReached, completions)
		framework.ExpectNoError(err, "failed to ensure job completion in namespace: %s", f.Namespace.Name)

		ginkgo.By("Ensuring job succeeded")
		job, err = e2ejob.GetJob(ctx, f.ClientSet, f.Namespace.Name, job.Name)
		framework.ExpectNoError(err, "failed to get job")
		for _, cond := range job.Status.Conditions {
			if cond.Type == batchv1.JobComplete {
				gomega.Expect(cond.Status).Should(gomega.Equal(v1.ConditionTrue))
			}
		}
		gomega.Expect(job.Status.Active).Should(gomega.Equal(int32(0)))
		gomega.Expect(job.Status.Ready).Should(gomega.Equal(ptr.To[int32](0)))
		gomega.Expect(job.Status.Terminating).Should(gomega.Equal(ptr.To[int32](0)))
	})

	framework.It("should create Workload and PodGroup for gang-eligible Job",
		framework.WithFeatureGate(features.GenericWorkload),
		framework.WithFeatureGate(features.WorkloadWithJob),
		func(ctx context.Context) {
			parallelism := int32(4)
			completions := int32(4)
			backoffLimit := int32(6)

			ginkgo.By("Creating an indexed job with parallelism=completions")
			job := e2ejob.NewTestJob("succeed", "gang-create", v1.RestartPolicyNever,
				parallelism, completions, nil, backoffLimit)
			job.Spec.CompletionMode = ptr.To(batchv1.IndexedCompletion)
			job, err := e2ejob.CreateJob(ctx, f.ClientSet, f.Namespace.Name, job)
			framework.ExpectNoError(err, "failed to create job in namespace: %s/%s", job.Namespace, job.Name)

			ginkgo.By("Waiting for Workload to be created")
			var workload *schedulingv1beta1.Workload
			gomega.Eventually(ctx, func(ctx context.Context) error {
				workloads, listErr := f.ClientSet.SchedulingV1beta1().Workloads(f.Namespace.Name).List(ctx, metav1.ListOptions{})
				if listErr != nil {
					return listErr
				}
				for i := range workloads.Items {
					for _, ref := range workloads.Items[i].OwnerReferences {
						if ref.UID == job.UID && ref.Kind == "Job" {
							workload = &workloads.Items[i]
							return nil
						}
					}
				}
				return fmt.Errorf("workload not found for job %s", job.Name)
			}).WithTimeout(30 * time.Second).WithPolling(time.Second).Should(gomega.Succeed())

			ginkgo.By("Waiting for PodGroup to be created")
			var podGroup *schedulingv1beta1.PodGroup
			gomega.Eventually(ctx, func(ctx context.Context) error {
				podGroups, listErr := f.ClientSet.SchedulingV1beta1().PodGroups(f.Namespace.Name).List(ctx, metav1.ListOptions{})
				if listErr != nil {
					return listErr
				}
				for i := range podGroups.Items {
					for _, ref := range podGroups.Items[i].OwnerReferences {
						if ref.UID == job.UID && ref.Kind == "Job" {
							podGroup = &podGroups.Items[i]
							return nil
						}
					}
				}
				return fmt.Errorf("podgroup not found for job %s", job.Name)
			}).WithTimeout(30 * time.Second).WithPolling(time.Second).Should(gomega.Succeed())

			ginkgo.By("Verifying PodGroup ownerReferences")
			gomega.Expect(podGroup.OwnerReferences).To(gomega.HaveLen(2))
			var hasJobOwner, hasWorkloadOwner bool
			for _, ref := range podGroup.OwnerReferences {
				if ref.UID == job.UID && ref.Kind == "Job" {
					hasJobOwner = true
				}
				if ref.UID == workload.UID && ref.Kind == "Workload" {
					hasWorkloadOwner = true
				}
			}
			gomega.Expect(hasJobOwner).To(gomega.BeTrueBecause("PodGroup should have Job ownerReference with UID %s", job.UID))
			gomega.Expect(hasWorkloadOwner).To(gomega.BeTrueBecause("PodGroup should have Workload ownerReference with UID %s", workload.UID))

			ginkgo.By("Verifying pods have schedulingGroup set")
			gomega.Eventually(ctx, func(ctx context.Context) error {
				pods, listErr := e2ejob.GetJobPods(ctx, f.ClientSet, f.Namespace.Name, job.Name)
				if listErr != nil {
					return listErr
				}
				if len(pods.Items) < int(parallelism) {
					return fmt.Errorf("expected %d pods, got %d", parallelism, len(pods.Items))
				}
				for _, pod := range pods.Items {
					if pod.Spec.SchedulingGroup == nil || pod.Spec.SchedulingGroup.PodGroupName == nil {
						return fmt.Errorf("pod %s missing schedulingGroup", pod.Name)
					}
					if *pod.Spec.SchedulingGroup.PodGroupName != podGroup.Name {
						return fmt.Errorf("pod %s schedulingGroup = %q, want %q", pod.Name, *pod.Spec.SchedulingGroup.PodGroupName, podGroup.Name)
					}
				}
				return nil
			}).WithTimeout(framework.PodStartTimeout).WithPolling(time.Second).Should(gomega.Succeed())

			ginkgo.By("Waiting for job to complete")
			err = e2ejob.WaitForJobComplete(ctx, f.ClientSet, f.Namespace.Name, job.Name, batchv1.JobReasonCompletionsReached, completions)
			framework.ExpectNoError(err, "failed to ensure job completion in namespace: %s", f.Namespace.Name)

			ginkgo.By("Ensuring job has no active pods")
			job, err = e2ejob.GetJob(ctx, f.ClientSet, f.Namespace.Name, job.Name)
			framework.ExpectNoError(err, "failed to get job")
			gomega.Expect(job.Status.Active).Should(gomega.Equal(int32(0)))
			gomega.Expect(job.Status.Ready).Should(gomega.Equal(ptr.To[int32](0)))
			gomega.Expect(job.Status.Terminating).Should(gomega.Equal(ptr.To[int32](0)))
		})

	/*
		测试名称：Allow updating pod resources for suspended Jobs
		描述：创建一个带初始容器资源的挂起 Job。
		在 job 挂起期间更新容器资源。
		取消挂起 job，验证创建的 pod 使用了更新后的资源。
		这验证了 KEP-5440: Mutable Job Pod Resource Updates。
	*/
	framework.It("should allow updating pod resources for a suspended job", framework.WithFeatureGate(features.MutablePodResourcesForSuspendedJobs), func(ctx context.Context) {
		jobName := "e2e-mutable-resources" + utilrand.String(5)

		parallelism := int32(1)
		completions := int32(1)
		backoffLimit := int32(6)

		initialCPU := resource.MustParse("100m")
		initialMemory := resource.MustParse("128Mi")
		updatedCPU := resource.MustParse("200m")
		updatedMemory := resource.MustParse("256Mi")

		ginkgo.By("Creating a suspended job with initial resources")
		job := e2ejob.NewTestJob("succeed", jobName, v1.RestartPolicyNever, parallelism, completions, nil, backoffLimit)
		job.Spec.Suspend = ptr.To(true)
		for i := range job.Spec.Template.Spec.Containers {
			job.Spec.Template.Spec.Containers[i].Resources = v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceCPU:    initialCPU,
					v1.ResourceMemory: initialMemory,
				},
			}
		}
		job, err := e2ejob.CreateJob(ctx, f.ClientSet, f.Namespace.Name, job)
		framework.ExpectNoError(err, "failed to create job in namespace: %s", f.Namespace.Name)

		ginkgo.By("Verifying job is suspended and no pods are created")
		err = e2ejob.WaitForJobSuspend(ctx, f.ClientSet, f.Namespace.Name, jobName)
		framework.ExpectNoError(err, "failed to verify job is suspended")
		pods, err := e2ejob.GetJobPods(ctx, f.ClientSet, f.Namespace.Name, jobName)
		framework.ExpectNoError(err, "failed to get pods for job")
		gomega.Expect(pods.Items).To(gomega.BeEmpty(), "expected no pods while job is suspended")

		ginkgo.By("Updating container resources while job is suspended")
		err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
			job, err = e2ejob.GetJob(ctx, f.ClientSet, f.Namespace.Name, jobName)
			if err != nil {
				return err
			}
			for i := range job.Spec.Template.Spec.Containers {
				job.Spec.Template.Spec.Containers[i].Resources = v1.ResourceRequirements{
					Requests: v1.ResourceList{
						v1.ResourceCPU:    updatedCPU,
						v1.ResourceMemory: updatedMemory,
					},
				}
			}
			job, err = e2ejob.UpdateJob(ctx, f.ClientSet, f.Namespace.Name, job)
			return err
		})
		framework.ExpectNoError(err, "failed to update job resources")

		ginkgo.By("Unsuspending the job")
		err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
			job, err = e2ejob.GetJob(ctx, f.ClientSet, f.Namespace.Name, jobName)
			if err != nil {
				return err
			}
			job.Spec.Suspend = ptr.To(false)
			job, err = e2ejob.UpdateJob(ctx, f.ClientSet, f.Namespace.Name, job)
			return err
		})
		framework.ExpectNoError(err, "failed to unsuspend job")

		ginkgo.By("Waiting for job to complete")
		err = e2ejob.WaitForJobComplete(ctx, f.ClientSet, f.Namespace.Name, jobName, batchv1.JobReasonCompletionsReached, completions)
		framework.ExpectNoError(err, "failed to wait for job completion")

		ginkgo.By("Verifying pods were created with updated resources")
		pods, err = e2ejob.GetJobPods(ctx, f.ClientSet, f.Namespace.Name, jobName)
		framework.ExpectNoError(err, "failed to get pods for job")
		gomega.Expect(pods.Items).NotTo(gomega.BeEmpty(), "expected at least one pod")

		for _, pod := range pods.Items {
			for _, container := range pod.Spec.Containers {
				cpuRequest := container.Resources.Requests[v1.ResourceCPU]
				memoryRequest := container.Resources.Requests[v1.ResourceMemory]
				gomega.Expect(cpuRequest.Equal(updatedCPU)).To(gomega.BeTrueBecause(
					"expected CPU request %v, got %v", updatedCPU.String(), cpuRequest.String()))
				gomega.Expect(memoryRequest.Equal(updatedMemory)).To(gomega.BeTrueBecause(
					"expected memory request %v, got %v", updatedMemory.String(), memoryRequest.String()))
			}
		}
	})

	/*
		测试名称：Allow updating pod resources for a job that started and was suspended
		描述：创建一个开始运行的 job，将其挂起，在挂起期间更新
		容器资源，然后取消挂起，验证新创建的
		pod 使用了更新后的资源。这验证了即使 job 之前
		已经启动过，也允许更新资源。
	*/
	framework.It("should allow updating pod resources for a job that started and then was suspended", framework.WithFeatureGate(features.MutablePodResourcesForSuspendedJobs), func(ctx context.Context) {
		jobName := "e2e-start-suspend" + utilrand.String(5)

		parallelism := int32(2)
		completions := int32(4)
		backoffLimit := int32(6)

		initialCPU := resource.MustParse("100m")
		initialMemory := resource.MustParse("128Mi")
		updatedCPU := resource.MustParse("200m")
		updatedMemory := resource.MustParse("256Mi")

		ginkgo.By("Creating a running job with initial resources")
		job := e2ejob.NewTestJob("notTerminate", jobName, v1.RestartPolicyNever, parallelism, completions, nil, backoffLimit)
		job.Spec.Suspend = ptr.To(false)
		for i := range job.Spec.Template.Spec.Containers {
			job.Spec.Template.Spec.Containers[i].Resources = v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceCPU:    initialCPU,
					v1.ResourceMemory: initialMemory,
				},
			}
		}
		job, err := e2ejob.CreateJob(ctx, f.ClientSet, f.Namespace.Name, job)
		framework.ExpectNoError(err, "failed to create job in namespace: %s", f.Namespace.Name)

		ginkgo.By("Waiting for pods to be running")
		err = e2ejob.WaitForJobPodsRunning(ctx, f.ClientSet, f.Namespace.Name, jobName, parallelism)
		framework.ExpectNoError(err, "failed to wait for job pods to be running")

		ginkgo.By("Suspending the running job")
		err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
			job, err = e2ejob.GetJob(ctx, f.ClientSet, f.Namespace.Name, jobName)
			if err != nil {
				return err
			}
			job.Spec.Suspend = ptr.To(true)
			job, err = e2ejob.UpdateJob(ctx, f.ClientSet, f.Namespace.Name, job)
			return err
		})
		framework.ExpectNoError(err, "failed to suspend job")

		ginkgo.By("Waiting for all pods to be deleted after suspension")
		err = e2ejob.WaitForAllJobPodsGone(ctx, f.ClientSet, f.Namespace.Name, jobName)
		framework.ExpectNoError(err, "failed to wait for pods to be deleted after suspension")

		ginkgo.By("Updating container resources while job is suspended")
		err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
			job, err = e2ejob.GetJob(ctx, f.ClientSet, f.Namespace.Name, jobName)
			if err != nil {
				return err
			}
			for i := range job.Spec.Template.Spec.Containers {
				job.Spec.Template.Spec.Containers[i].Resources = v1.ResourceRequirements{
					Requests: v1.ResourceList{
						v1.ResourceCPU:    updatedCPU,
						v1.ResourceMemory: updatedMemory,
					},
				}
			}
			job, err = e2ejob.UpdateJob(ctx, f.ClientSet, f.Namespace.Name, job)
			return err
		})
		framework.ExpectNoError(err, "failed to update job resources")

		ginkgo.By("Unsuspending the job")
		err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
			job, err = e2ejob.GetJob(ctx, f.ClientSet, f.Namespace.Name, jobName)
			if err != nil {
				return err
			}
			job.Spec.Suspend = ptr.To(false)
			job, err = e2ejob.UpdateJob(ctx, f.ClientSet, f.Namespace.Name, job)
			return err
		})
		framework.ExpectNoError(err, "failed to unsuspend job")

		ginkgo.By("Waiting for new pods to be running with updated resources")
		err = e2ejob.WaitForJobPodsRunning(ctx, f.ClientSet, f.Namespace.Name, jobName, parallelism)
		framework.ExpectNoError(err, "failed to wait for job pods to be running after unsuspending")

		ginkgo.By("Verifying newly created pods have updated resources")
		pods, err := e2ejob.GetJobPods(ctx, f.ClientSet, f.Namespace.Name, jobName)
		framework.ExpectNoError(err, "failed to get pods for job")
		gomega.Expect(pods.Items).NotTo(gomega.BeEmpty(), "expected at least one pod")

		for _, pod := range pods.Items {
			for _, container := range pod.Spec.Containers {
				cpuRequest := container.Resources.Requests[v1.ResourceCPU]
				memoryRequest := container.Resources.Requests[v1.ResourceMemory]
				gomega.Expect(cpuRequest.Equal(updatedCPU)).To(gomega.BeTrueBecause(
					"expected CPU request %v, got %v", updatedCPU.String(), cpuRequest.String()))
				gomega.Expect(memoryRequest.Equal(updatedMemory)).To(gomega.BeTrueBecause(
					"expected memory request %v, got %v", updatedMemory.String(), memoryRequest.String()))
			}
		}
	})
})

func updateJobSuspendWithRetries(ctx context.Context, f *framework.Framework, job *batchv1.Job, suspend *bool) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		job, err := e2ejob.GetJob(ctx, f.ClientSet, f.Namespace.Name, job.Name)
		framework.ExpectNoError(err, "unable to get job %s in namespace %s", job.Name, f.Namespace.Name)
		job.Spec.Suspend = suspend
		_, err = e2ejob.UpdateJob(ctx, f.ClientSet, f.Namespace.Name, job)
		return err
	})
}

// waitForJobEvent 用于跟踪并记录 Job 事件。
// 由于事件投递实际上没有保障，
// 如果漏掉了所需事件，我们不会返回错误。
func waitForJobEvent(ctx context.Context, config watchEventConfig) {
	f := config.framework
	ctx, cancel := context.WithTimeout(ctx, f.Timeouts.PodStartShort)
	defer cancel()
	_, err := watchtools.Until(ctx, config.resourceVersion, config.w, func(event watch.Event) (bool, error) {
		if job, ok := event.Object.(*batchv1.Job); ok {

			var key string
			switch config.updatedMetadataType {
			case "annotation":
				key = job.Annotations[config.updatedKey]
			case "label":
				key = job.Labels[config.updatedKey]
			}

			found := job.ObjectMeta.Name == config.extJob.ObjectMeta.Name &&
				job.ObjectMeta.Namespace == f.Namespace.Name &&
				key == config.updatedValue &&
				event.Type == config.watchEvent
			if !found {
				framework.Logf("Event %v observed for Job %v in namespace %v with labels: %v and annotations: %v", event.Type, job.ObjectMeta.Name, job.ObjectMeta.Namespace, job.Labels, job.Annotations)
				return false, nil
			}
			framework.Logf("Event %v found for Job %v in namespace %v with labels: %v and annotations: %v", event.Type, job.ObjectMeta.Name, job.ObjectMeta.Namespace, job.Labels, job.Annotations)
			return found, nil
		}
		framework.Logf("Observed event: %+v", event.Object)
		return false, nil
	})
	if err != nil {
		j, _ := f.ClientSet.BatchV1().Jobs(f.Namespace.Name).Get(ctx, config.jobName, metav1.GetOptions{})
		framework.Logf("We missed the %v event. Job details: %+v", config.watchEvent, j)
	}
}

func findConditionByType(list []batchv1.JobCondition, cType batchv1.JobConditionType) *batchv1.JobCondition {
	for i := range list {
		if list[i].Type == cType {
			return &list[i]
		}
	}
	return nil
}
