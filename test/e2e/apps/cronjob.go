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

package apps

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/onsi/gomega/format"

	batchv1 "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/resourceversion"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/util/retry"
	batchinternal "k8s.io/kubernetes/pkg/apis/batch"
	jobutil "k8s.io/kubernetes/pkg/controller/job/util"
	apimachineryutils "k8s.io/kubernetes/test/e2e/common/apimachinery"
	"k8s.io/kubernetes/test/e2e/framework"
	e2ejob "k8s.io/kubernetes/test/e2e/framework/job"
	e2eresource "k8s.io/kubernetes/test/e2e/framework/resource"
	imageutils "k8s.io/kubernetes/test/utils/image"
	admissionapi "k8s.io/pod-security-admission/api"
)

const (
	// 等待 cronjob 的超时时间
	cronJobTimeout = 5 * time.Minute
)

var _ = SIGDescribe("CronJob", func() {
	f := framework.NewDefaultFramework("cronjob")
	f.NamespacePodSecurityLevel = admissionapi.LevelBaseline

	sleepCommand := []string{"sleep", "300"}

	// Pod 会立即完成
	successCommand := []string{"/bin/true"}
	failureCommand := []string{"/bin/false"}

	/*
	   发布版本：v1.21
	   测试名称：CronJob AllowConcurrent
	   描述：CronJob 必须支持 AllowConcurrent 策略，允许同时运行多个 job。
	*/
	framework.ConformanceIt("should schedule multiple jobs concurrently", func(ctx context.Context) {
		ginkgo.By("Creating a cronjob")
		cronJob := newTestCronJob("concurrent", "*/1 * * * ?", batchv1.AllowConcurrent,
			sleepCommand, nil, nil)
		cronJob, err := createCronJob(ctx, f.ClientSet, f.Namespace.Name, cronJob)
		framework.ExpectNoError(err, "Failed to create CronJob in namespace %s", f.Namespace.Name)

		ginkgo.By("Ensuring more than one job is running at a time")
		err = waitForActiveJobs(ctx, f.ClientSet, f.Namespace.Name, cronJob.Name, 2)
		framework.ExpectNoError(err, "Failed to wait for active jobs in CronJob %s in namespace %s", cronJob.Name, f.Namespace.Name)

		ginkgo.By("Ensuring at least two running jobs exists by listing jobs explicitly")
		jobs, err := f.ClientSet.BatchV1().Jobs(f.Namespace.Name).List(ctx, metav1.ListOptions{})
		framework.ExpectNoError(err, "Failed to list jobs in namespace %s", f.Namespace.Name)
		activeJobs, _ := filterActiveJobs(jobs)
		gomega.Expect(len(activeJobs)).To(gomega.BeNumerically(">=", 2))

		ginkgo.By("Removing cronjob")
		err = deleteCronJob(ctx, f.ClientSet, f.Namespace.Name, cronJob.Name)
		framework.ExpectNoError(err, "Failed to delete CronJob %s in namespace %s", cronJob.Name, f.Namespace.Name)
	})

	/*
	   发布版本：v1.21
	   测试名称：CronJob Suspend
	   描述：CronJob 必须支持暂停，暂停期间不再创建新 job。
	*/
	framework.ConformanceIt("should not schedule jobs when suspended", f.WithSlow(), func(ctx context.Context) {
		ginkgo.By("Creating a suspended cronjob")
		cronJob := newTestCronJob("suspended", "*/1 * * * ?", batchv1.AllowConcurrent,
			sleepCommand, nil, nil)
		t := true
		cronJob.Spec.Suspend = &t
		cronJob, err := createCronJob(ctx, f.ClientSet, f.Namespace.Name, cronJob)
		framework.ExpectNoError(err, "Failed to create CronJob in namespace %s", f.Namespace.Name)

		ginkgo.By("Ensuring no jobs are scheduled")
		gomega.Consistently(ctx, framework.GetObject(f.ClientSet.BatchV1().CronJobs(f.Namespace.Name).Get, cronJob.Name, metav1.GetOptions{})).WithPolling(framework.Poll).WithTimeout(cronJobTimeout).
			Should(gomega.HaveField("Status.Active", gomega.BeEmpty()))

		ginkgo.By("Ensuring no job exists by listing jobs explicitly")
		jobs, err := f.ClientSet.BatchV1().Jobs(f.Namespace.Name).List(ctx, metav1.ListOptions{})
		framework.ExpectNoError(err, "Failed to list jobs in namespace %s", f.Namespace.Name)
		gomega.Expect(jobs.Items).To(gomega.BeEmpty())

		ginkgo.By("Removing cronjob")
		err = deleteCronJob(ctx, f.ClientSet, f.Namespace.Name, cronJob.Name)
		framework.ExpectNoError(err, "Failed to delete CronJob %s in namespace %s", cronJob.Name, f.Namespace.Name)
	})

	/*
	   发布版本：v1.21
	   测试名称：CronJob ForbidConcurrent
	   描述：CronJob 必须支持 ForbidConcurrent 策略，同一时间只允许运行一个先前的 job。
	*/
	framework.ConformanceIt("should not schedule new jobs when ForbidConcurrent", f.WithSlow(), func(ctx context.Context) {
		ginkgo.By("Creating a ForbidConcurrent cronjob")
		cronJob := newTestCronJob("forbid", "*/1 * * * ?", batchv1.ForbidConcurrent,
			sleepCommand, nil, nil)
		cronJob, err := createCronJob(ctx, f.ClientSet, f.Namespace.Name, cronJob)
		framework.ExpectNoError(err, "Failed to create CronJob in namespace %s", f.Namespace.Name)

		ginkgo.By("Ensuring a job is scheduled")
		err = waitForActiveJobs(ctx, f.ClientSet, f.Namespace.Name, cronJob.Name, 1)
		framework.ExpectNoError(err, "Failed to schedule CronJob %s", cronJob.Name)

		ginkgo.By("Ensuring exactly one is scheduled")
		cronJob, err = getCronJob(ctx, f.ClientSet, f.Namespace.Name, cronJob.Name)
		framework.ExpectNoError(err, "Failed to get CronJob %s", cronJob.Name)
		gomega.Expect(cronJob.Status.Active).Should(gomega.HaveLen(1))

		ginkgo.By("Ensuring exactly one running job exists by listing jobs explicitly")
		jobs, err := f.ClientSet.BatchV1().Jobs(f.Namespace.Name).List(ctx, metav1.ListOptions{})
		framework.ExpectNoError(err, "Failed to list jobs in namespace %s", f.Namespace.Name)
		activeJobs, _ := filterActiveJobs(jobs)
		gomega.Expect(activeJobs).To(gomega.HaveLen(1))

		ginkgo.By("Ensuring no more jobs are scheduled")
		gomega.Eventually(ctx, framework.GetObject(f.ClientSet.BatchV1().CronJobs(f.Namespace.Name).Get, cronJob.Name, metav1.GetOptions{})).WithPolling(framework.Poll).WithTimeout(cronJobTimeout).
			Should(framework.MakeMatcher(func(cj *batchv1.CronJob) (func() string, error) {
				if len(cj.Status.Active) < 2 {
					return nil, nil
				}
				return func() string {
					return fmt.Sprintf("unexpect active job number: %d\n", len(cj.Status.Active))
				}, nil
			}))

		ginkgo.By("Removing cronjob")
		err = deleteCronJob(ctx, f.ClientSet, f.Namespace.Name, cronJob.Name)
		framework.ExpectNoError(err, "Failed to delete CronJob %s in namespace %s", cronJob.Name, f.Namespace.Name)
	})

	/*
	   发布版本：v1.21
	   测试名称：CronJob ReplaceConcurrent
	   描述：CronJob 必须支持 ReplaceConcurrent 策略，同一时间只允许运行一个较新的 job。
	*/
	framework.ConformanceIt("should replace jobs when ReplaceConcurrent", func(ctx context.Context) {
		ginkgo.By("Creating a ReplaceConcurrent cronjob")
		cronJob := newTestCronJob("replace", "*/1 * * * ?", batchv1.ReplaceConcurrent,
			sleepCommand, nil, nil)
		cronJob, err := createCronJob(ctx, f.ClientSet, f.Namespace.Name, cronJob)
		framework.ExpectNoError(err, "Failed to create CronJob in namespace %s", f.Namespace.Name)

		ginkgo.By("Ensuring a job is scheduled")
		err = waitForActiveJobs(ctx, f.ClientSet, f.Namespace.Name, cronJob.Name, 1)
		framework.ExpectNoError(err, "Failed to schedule CronJob %s in namespace %s", cronJob.Name, f.Namespace.Name)

		ginkgo.By("Ensuring exactly one is scheduled")
		cronJob, err = getCronJob(ctx, f.ClientSet, f.Namespace.Name, cronJob.Name)
		framework.ExpectNoError(err, "Failed to get CronJob %s", cronJob.Name)
		gomega.Expect(cronJob.Status.Active).Should(gomega.HaveLen(1))

		ginkgo.By("Ensuring exactly one running job exists by listing jobs explicitly")
		jobs, err := f.ClientSet.BatchV1().Jobs(f.Namespace.Name).List(ctx, metav1.ListOptions{})
		framework.ExpectNoError(err, "Failed to list the jobs in namespace %s", f.Namespace.Name)
		activeJobs, _ := filterActiveJobs(jobs)
		gomega.Expect(activeJobs).To(gomega.HaveLen(1))

		ginkgo.By("Ensuring the job is replaced with a new one")
		err = waitForJobReplaced(ctx, f.ClientSet, f.Namespace.Name, jobs.Items[0].Name)
		framework.ExpectNoError(err, "Failed to replace CronJob %s in namespace %s", jobs.Items[0].Name, f.Namespace.Name)

		ginkgo.By("Removing cronjob")
		err = deleteCronJob(ctx, f.ClientSet, f.Namespace.Name, cronJob.Name)
		framework.ExpectNoError(err, "Failed to delete CronJob %s in namespace %s", cronJob.Name, f.Namespace.Name)
	})

	ginkgo.It("should be able to schedule after more than 100 missed schedule", func(ctx context.Context) {
		ginkgo.By("Creating a cronjob")
		cronJob := newTestCronJob("concurrent", "*/1 * * * ?", batchv1.ForbidConcurrent,
			sleepCommand, nil, nil)
		creationTime := time.Now().Add(-99 * 24 * time.Hour)
		lastScheduleTime := creationTime.Add(1 * 24 * time.Hour)
		cronJob.CreationTimestamp = metav1.Time{Time: creationTime}
		cronJob.Status.LastScheduleTime = &metav1.Time{Time: lastScheduleTime}
		cronJob, err := createCronJob(ctx, f.ClientSet, f.Namespace.Name, cronJob)
		framework.ExpectNoError(err, "Failed to create CronJob in namespace %s", f.Namespace.Name)

		ginkgo.By("Ensuring one job is running")
		err = waitForActiveJobs(ctx, f.ClientSet, f.Namespace.Name, cronJob.Name, 1)
		framework.ExpectNoError(err, "Failed to wait for active jobs in CronJob %s in namespace %s", cronJob.Name, f.Namespace.Name)

		ginkgo.By("Ensuring at least one running jobs exists by listing jobs explicitly")
		jobs, err := f.ClientSet.BatchV1().Jobs(f.Namespace.Name).List(ctx, metav1.ListOptions{})
		framework.ExpectNoError(err, "Failed to list jobs in namespace %s", f.Namespace.Name)
		activeJobs, _ := filterActiveJobs(jobs)
		gomega.Expect(activeJobs).ToNot(gomega.BeEmpty())

		ginkgo.By("Removing cronjob")
		err = deleteCronJob(ctx, f.ClientSet, f.Namespace.Name, cronJob.Name)
		framework.ExpectNoError(err, "Failed to delete CronJob %s in namespace %s", cronJob.Name, f.Namespace.Name)
	})

	// 不应出现意外的 warning
	ginkgo.It("should not emit unexpected warnings", func(ctx context.Context) {
		ginkgo.By("Creating a cronjob")
		cronJob := newTestCronJob("concurrent", "*/1 * * * ?", batchv1.AllowConcurrent,
			nil, nil, nil)
		cronJob, err := createCronJob(ctx, f.ClientSet, f.Namespace.Name, cronJob)
		framework.ExpectNoError(err, "Failed to create CronJob in namespace %s", f.Namespace.Name)

		ginkgo.By("Ensuring at least two jobs and at least one finished job exists by listing jobs explicitly")
		err = waitForJobsAtLeast(ctx, f.ClientSet, f.Namespace.Name, 2)
		framework.ExpectNoError(err, "Failed to ensure at least two job exists in namespace %s", f.Namespace.Name)
		err = waitForAnyFinishedJob(ctx, f.ClientSet, f.Namespace.Name)
		framework.ExpectNoError(err, "Failed to ensure at least on finished job exists in namespace %s", f.Namespace.Name)

		ginkgo.By("Ensuring no unexpected event has happened")
		gomega.Eventually(ctx, framework.HandleRetry(func(ctx context.Context) (*v1.EventList, error) {
			sj, err := getCronJob(ctx, f.ClientSet, f.Namespace.Name, cronJob.Name)
			if err != nil {
				return nil, err
			}
			return f.ClientSet.CoreV1().Events(f.Namespace.Name).Search(scheme.Scheme, sj)
		})).WithPolling(framework.Poll).WithTimeout(30 * time.Second).Should(framework.MakeMatcher(func(actual *v1.EventList) (failure func() string, err error) {
			for _, e := range actual.Items {
				for _, reason := range []string{"MissingJob", "UnexpectedJob"} {
					if e.Reason == reason {
						return func() string {
							return fmt.Sprintf("unexpected event: %s\n", reason)
						}, nil
					}
				}
			}
			return nil, nil
		}))

		ginkgo.By("Removing cronjob")
		err = deleteCronJob(ctx, f.ClientSet, f.Namespace.Name, cronJob.Name)
		framework.ExpectNoError(err, "Failed to delete CronJob %s in namespace %s", cronJob.Name, f.Namespace.Name)
	})

	ginkgo.It("should set the cronjob-scheduled-timestamp annotation on a job", func(ctx context.Context) {
		ginkgo.By("Creating a cronjob")
		cronJob := newTestCronJob("concurrent", "*/1 * * * ?", batchv1.AllowConcurrent,
			nil, nil, nil)
		cronJob, err := createCronJob(ctx, f.ClientSet, f.Namespace.Name, cronJob)
		framework.ExpectNoError(err, "Failed to create CronJob in namespace %s", f.Namespace.Name)

		ginkgo.By("Ensuring CronJobsScheduledAnnotation annotation exists on the newely created job")
		err = waitForJobsAtLeast(ctx, f.ClientSet, f.Namespace.Name, 1)
		framework.ExpectNoError(err, "Failed to ensure a job exists in namespace %s", f.Namespace.Name)

		jobs, err := f.ClientSet.BatchV1().Jobs(f.Namespace.Name).List(ctx, metav1.ListOptions{})
		framework.ExpectNoError(err, "Failed to list jobs in namespace %s", f.Namespace.Name)
		gomega.Expect(jobs.Items[0].Annotations).Should(gomega.HaveKey(batchv1.CronJobScheduledTimestampAnnotation),
			"missing cronjob-scheduled-timestamp annotation")
		ginkgo.By("Ensuring CronJobsScheduledAnnotation annotation parses to RFC3339")
		timeAnnotation := jobs.Items[0].Annotations[batchv1.CronJobScheduledTimestampAnnotation]
		_, err = time.Parse(time.RFC3339, timeAnnotation)
		framework.ExpectNoError(err, "Failed to parse cronjob-scheduled-timestamp annotation: %q", timeAnnotation)

		ginkgo.By("Removing cronjob")
		err = deleteCronJob(ctx, f.ClientSet, f.Namespace.Name, cronJob.Name)
		framework.ExpectNoError(err, "Failed to delete CronJob %s in namespace %s", cronJob.Name, f.Namespace.Name)
	})

	// 已删除的 job 应从 active 列表中移除
	ginkgo.It("should remove from active list jobs that have been deleted", func(ctx context.Context) {
		ginkgo.By("Creating a ForbidConcurrent cronjob")
		cronJob := newTestCronJob("forbid", "*/1 * * * ?", batchv1.ForbidConcurrent,
			sleepCommand, nil, nil)
		cronJob, err := createCronJob(ctx, f.ClientSet, f.Namespace.Name, cronJob)
		framework.ExpectNoError(err, "Failed to create CronJob in namespace %s", f.Namespace.Name)

		ginkgo.By("Ensuring a job is scheduled")
		err = waitForActiveJobs(ctx, f.ClientSet, f.Namespace.Name, cronJob.Name, 1)
		framework.ExpectNoError(err, "Failed to ensure a %s cronjob is scheduled in namespace %s", cronJob.Name, f.Namespace.Name)

		ginkgo.By("Ensuring exactly one is scheduled")
		cronJob, err = getCronJob(ctx, f.ClientSet, f.Namespace.Name, cronJob.Name)
		framework.ExpectNoError(err, "Failed to ensure exactly one %s cronjob is scheduled in namespace %s", cronJob.Name, f.Namespace.Name)
		gomega.Expect(cronJob.Status.Active).Should(gomega.HaveLen(1))

		ginkgo.By("Deleting the job")
		job := cronJob.Status.Active[0]
		framework.ExpectNoError(e2eresource.DeleteResourceAndWaitForGC(ctx, f.ClientSet, batchinternal.Kind("Job"), f.Namespace.Name, job.Name))

		ginkgo.By("Ensuring job was deleted")
		_, err = e2ejob.GetJob(ctx, f.ClientSet, f.Namespace.Name, job.Name)
		gomega.Expect(err).To(gomega.MatchError(apierrors.IsNotFound, fmt.Sprintf("Failed to delete %s cronjob in namespace %s", cronJob.Name, f.Namespace.Name)))

		ginkgo.By("Ensuring the job is not in the cronjob active list")
		err = waitForJobNotActive(ctx, f.ClientSet, f.Namespace.Name, cronJob.Name, job.Name)
		framework.ExpectNoError(err, "Failed to ensure the %s cronjob is not in active list in namespace %s", cronJob.Name, f.Namespace.Name)

		ginkgo.By("Ensuring MissingJob event has occurred")
		err = waitForEventWithReason(ctx, f.ClientSet, f.Namespace.Name, cronJob.Name, []string{"MissingJob"})
		framework.ExpectNoError(err, "Failed to ensure missing job event has occurred for %s cronjob in namespace %s", cronJob.Name, f.Namespace.Name)

		ginkgo.By("Removing cronjob")
		err = deleteCronJob(ctx, f.ClientSet, f.Namespace.Name, cronJob.Name)
		framework.ExpectNoError(err, "Failed to remove %s cronjob in namespace %s", cronJob.Name, f.Namespace.Name)
	})

	// 清理已成功完成的 job，最多保留一个成功 job
	ginkgo.It("should delete successful finished jobs with limit of one successful job", func(ctx context.Context) {
		ginkgo.By("Creating an AllowConcurrent cronjob with custom history limit")
		successLimit := int32(1)
		failedLimit := int32(0)
		cronJob := newTestCronJob("successful-jobs-history-limit", "*/1 * * * ?", batchv1.AllowConcurrent,
			successCommand, &successLimit, &failedLimit)

		ensureHistoryLimits(ctx, f.ClientSet, f.Namespace.Name, cronJob)
	})

	// 清理已失败的 job，最多保留一个失败 job
	ginkgo.It("should delete failed finished jobs with limit of one job", func(ctx context.Context) {
		ginkgo.By("Creating an AllowConcurrent cronjob with custom history limit")
		successLimit := int32(0)
		failedLimit := int32(1)
		cronJob := newTestCronJob("failed-jobs-history-limit", "*/1 * * * ?", batchv1.AllowConcurrent,
			failureCommand, &successLimit, &failedLimit)

		ensureHistoryLimits(ctx, f.ClientSet, f.Namespace.Name, cronJob)
	})

	ginkgo.It("should support timezone", func(ctx context.Context) {
		ginkgo.By("Creating a cronjob with TimeZone")
		cronJob := newTestCronJob("cronjob-with-timezone", "*/1 * * * ?", batchv1.AllowConcurrent,
			failureCommand, nil, nil)
		badTimeZone := "bad-time-zone"
		cronJob.Spec.TimeZone = &badTimeZone
		_, err := createCronJob(ctx, f.ClientSet, f.Namespace.Name, cronJob)
		gomega.Expect(err).To(gomega.MatchError(apierrors.IsInvalid, "Failed to create CronJob, invalid time zone."))
	})

	/*
	   发布版本：v1.21
	   测试名称：CronJob API Operations
	   描述：
	   CronJob 必须支持 create、get、list、watch、update、patch、delete 和 deletecollection。
	   CronJob/status 必须支持 get、update 和 patch。
	*/
	framework.ConformanceIt("should support CronJob API operations", func(ctx context.Context) {
		ginkgo.By("Creating a cronjob")
		successLimit := int32(1)
		failedLimit := int32(0)
		cjTemplate := newTestCronJob("test-api", "* */1 * * ?", batchv1.AllowConcurrent,
			successCommand, &successLimit, &failedLimit)
		cjTemplate.Labels = map[string]string{
			"special-label": f.UniqueName,
		}

		ns := f.Namespace.Name
		cjVersion := "v1"
		cjClient := f.ClientSet.BatchV1().CronJobs(ns)

		ginkgo.By("creating")
		createdCronJob, err := cjClient.Create(ctx, cjTemplate, metav1.CreateOptions{})
		framework.ExpectNoError(err)
		gomega.Expect(createdCronJob).To(apimachineryutils.HaveValidResourceVersion())

		ginkgo.By("getting")
		gottenCronJob, err := cjClient.Get(ctx, createdCronJob.Name, metav1.GetOptions{})
		framework.ExpectNoError(err)
		gomega.Expect(gottenCronJob.UID).To(gomega.Equal(createdCronJob.UID))

		ginkgo.By("listing")
		cjs, err := cjClient.List(ctx, metav1.ListOptions{LabelSelector: "special-label=" + f.UniqueName})
		framework.ExpectNoError(err)
		gomega.Expect(cjs.Items).To(gomega.HaveLen(1), "filtered list should have 1 item")

		ginkgo.By("watching")
		framework.Logf("starting watch")
		cjWatch, err := cjClient.Watch(ctx, metav1.ListOptions{ResourceVersion: cjs.ResourceVersion, LabelSelector: "special-label=" + f.UniqueName})
		framework.ExpectNoError(err)

		// 测试集群范围的 list 和 watch
		clusterCJClient := f.ClientSet.BatchV1().CronJobs("")
		ginkgo.By("cluster-wide listing")
		clusterCJs, err := clusterCJClient.List(ctx, metav1.ListOptions{LabelSelector: "special-label=" + f.UniqueName})
		framework.ExpectNoError(err)
		gomega.Expect(clusterCJs.Items).To(gomega.HaveLen(1), "filtered list should have 1 item")

		ginkgo.By("cluster-wide watching")
		framework.Logf("starting watch")
		_, err = clusterCJClient.Watch(ctx, metav1.ListOptions{ResourceVersion: cjs.ResourceVersion, LabelSelector: "special-label=" + f.UniqueName})
		framework.ExpectNoError(err)

		ginkgo.By("patching")
		patchedCronJob, err := cjClient.Patch(ctx, createdCronJob.Name, types.MergePatchType,
			[]byte(`{"metadata":{"annotations":{"patched":"true"}}}`), metav1.PatchOptions{})
		framework.ExpectNoError(err)
		gomega.Expect(patchedCronJob.Annotations).To(gomega.HaveKeyWithValue("patched", "true"), "patched object should have the applied annotation")
		gomega.Expect(resourceversion.CompareResourceVersion(createdCronJob.ResourceVersion, patchedCronJob.ResourceVersion)).To(gomega.BeNumerically("==", -1), "patched object should have a larger resource version")

		ginkgo.By("updating")
		var cjToUpdate, updatedCronJob *batchv1.CronJob
		err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
			cjToUpdate, err = cjClient.Get(ctx, createdCronJob.Name, metav1.GetOptions{})
			if err != nil {
				return err
			}
			cjToUpdate.Annotations["updated"] = "true"
			updatedCronJob, err = cjClient.Update(ctx, cjToUpdate, metav1.UpdateOptions{})
			return err
		})
		framework.ExpectNoError(err)
		gomega.Expect(updatedCronJob.Annotations).To(gomega.HaveKeyWithValue("updated", "true"), "updated object should have the applied annotation")

		framework.Logf("waiting for watch events with expected annotations")
		for sawAnnotations := false; !sawAnnotations; {
			select {
			case evt, ok := <-cjWatch.ResultChan():

				if !ok {
					framework.Fail("Watch channel is closed.")
				}
				gomega.Expect(evt.Type).To(gomega.Equal(watch.Modified))
				watchedCronJob, isCronJob := evt.Object.(*batchv1.CronJob)
				if !isCronJob {
					framework.Failf("expected CronJob, got %T", evt.Object)
				}
				if watchedCronJob.Annotations["patched"] == "true" {
					framework.Logf("saw patched and updated annotations")
					sawAnnotations = true
					cjWatch.Stop()
				} else {
					framework.Logf("missing expected annotations, waiting: %#v", watchedCronJob.Annotations)
				}
			case <-time.After(wait.ForeverTestTimeout):
				framework.Fail("timed out waiting for watch event")
			}
		}

		// /status 子资源操作
		ginkgo.By("patching /status")
		// 需要使用 RFC3339 版本，因为网络传输的转换会截掉纳秒
		now1 := metav1.Now().Rfc3339Copy()
		cjStatus := batchv1.CronJobStatus{
			LastScheduleTime: &now1,
		}
		cjStatusJSON, err := json.Marshal(cjStatus)
		framework.ExpectNoError(err)
		patchedStatus, err := cjClient.Patch(ctx, createdCronJob.Name, types.MergePatchType,
			[]byte(`{"metadata":{"annotations":{"patchedstatus":"true"}},"status":`+string(cjStatusJSON)+`}`),
			metav1.PatchOptions{}, "status")
		framework.ExpectNoError(err)
		if !patchedStatus.Status.LastScheduleTime.Equal(&now1) {
			framework.Failf("patched object should have the applied lastScheduleTime %#v, got %#v instead", cjStatus.LastScheduleTime, patchedStatus.Status.LastScheduleTime)
		}
		gomega.Expect(patchedStatus.Annotations).To(gomega.HaveKeyWithValue("patchedstatus", "true"), "patched object should have the applied annotation")

		ginkgo.By("updating /status")
		// 需要使用 RFC3339 版本，因为网络传输的转换会截掉纳秒
		now2 := metav1.Now().Rfc3339Copy()
		var statusToUpdate, updatedStatus *batchv1.CronJob
		err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
			statusToUpdate, err = cjClient.Get(ctx, createdCronJob.Name, metav1.GetOptions{})
			if err != nil {
				return err
			}
			statusToUpdate.Status.LastScheduleTime = &now2
			updatedStatus, err = cjClient.UpdateStatus(ctx, statusToUpdate, metav1.UpdateOptions{})
			return err
		})
		framework.ExpectNoError(err)

		if !updatedStatus.Status.LastScheduleTime.Equal(&now2) {
			framework.Failf("updated object status expected to have updated lastScheduleTime %#v, got %#v", statusToUpdate.Status.LastScheduleTime, updatedStatus.Status.LastScheduleTime)
		}

		ginkgo.By("get /status")
		cjResource := schema.GroupVersionResource{Group: "batch", Version: cjVersion, Resource: "cronjobs"}
		gottenStatus, err := f.DynamicClient.Resource(cjResource).Namespace(ns).Get(ctx, createdCronJob.Name, metav1.GetOptions{}, "status")
		framework.ExpectNoError(err)
		statusUID, _, err := unstructured.NestedFieldCopy(gottenStatus.Object, "metadata", "uid")
		framework.ExpectNoError(err)
		gomega.Expect(string(createdCronJob.UID)).To(gomega.Equal(statusUID), "createdCronJob.UID: %v expected to match statusUID: %v ", createdCronJob.UID, statusUID)

		// CronJob 资源删除操作
		expectFinalizer := func(cj *batchv1.CronJob, msg string) {
			gomega.Expect(cj.DeletionTimestamp).NotTo(gomega.BeNil(), fmt.Sprintf("expected deletionTimestamp, got nil on step: %q, cronjob: %+v", msg, cj))
			gomega.Expect(cj.Finalizers).ToNot(gomega.BeEmpty(), "expected finalizers on cronjob, got none on step: %q, cronjob: %+v", msg, cj)
		}

		ginkgo.By("deleting")
		cjTemplate.Name = "for-removal"
		forRemovalCronJob, err := cjClient.Create(ctx, cjTemplate, metav1.CreateOptions{})
		framework.ExpectNoError(err)
		err = cjClient.Delete(ctx, forRemovalCronJob.Name, metav1.DeleteOptions{})
		framework.ExpectNoError(err)
		cj, err := cjClient.Get(ctx, forRemovalCronJob.Name, metav1.GetOptions{})
		// 如果 controller 不支持 finalizer，预期返回 404；否则验证 finalizer 行为。
		if err == nil {
			expectFinalizer(cj, "deleting cronjob")
		} else if !apierrors.IsNotFound(err) {
			framework.Failf("expected 404, got %v", err)
		}

		ginkgo.By("deleting a collection")
		err = cjClient.DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{LabelSelector: "special-label=" + f.UniqueName})
		framework.ExpectNoError(err)
		cjs, err = cjClient.List(ctx, metav1.ListOptions{LabelSelector: "special-label=" + f.UniqueName})
		framework.ExpectNoError(err)
		// 由于某些 cronjob 可能因 finalizer 尚未删除，剩余项应 <= 2
		gomega.Expect(len(cjs.Items)).To(gomega.BeNumerically("<=", 2), "filtered list length should be <= 2, got:\n%s", format.Object(cjs.Items, 1))
		// 验证 finalizer
		for _, cj := range cjs.Items {
			expectFinalizer(&cj, "deleting cronjob collection")
		}
	})

})

func ensureHistoryLimits(ctx context.Context, c clientset.Interface, ns string, cronJob *batchv1.CronJob) {
	cronJob, err := createCronJob(ctx, c, ns, cronJob)
	framework.ExpectNoError(err, "Failed to create allowconcurrent cronjob with custom history limits in namespace %s", ns)

	// Job 会立即完成，很可能捕捉不到 active 状态，
	// 因此不检查 active job。

	ginkgo.By("Ensuring a finished job exists")
	err = waitForAnyFinishedJob(ctx, c, ns)
	framework.ExpectNoError(err, "Failed to ensure a finished cronjob exists in namespace %s", ns)

	ginkgo.By("Ensuring a finished job exists by listing jobs explicitly")
	jobs, err := c.BatchV1().Jobs(ns).List(ctx, metav1.ListOptions{})
	framework.ExpectNoError(err, "Failed to ensure a finished cronjob exists by listing jobs explicitly in namespace %s", ns)
	activeJobs, finishedJobs := filterActiveJobs(jobs)
	if len(finishedJobs) != 1 {
		framework.Logf("Expected one finished job in namespace %s; activeJobs=%v; finishedJobs=%v", ns, activeJobs, finishedJobs)
		gomega.Expect(finishedJobs).To(gomega.HaveLen(1))
	}

	// 下一分钟下一个 job 完成时，Job 应被删除
	ginkgo.By("Ensuring this job and its pods does not exist anymore")
	err = waitForJobToDisappear(ctx, c, ns, finishedJobs[0])
	framework.ExpectNoError(err, "Failed to ensure that job does not exists anymore in namespace %s", ns)
	err = waitForJobsPodToDisappear(ctx, c, ns, finishedJobs[0])
	framework.ExpectNoError(err, "Failed to ensure that pods for job does not exists anymore in namespace %s", ns)

	ginkgo.By("Ensuring there is 1 finished job by listing jobs explicitly")
	jobs, err = c.BatchV1().Jobs(ns).List(ctx, metav1.ListOptions{})
	framework.ExpectNoError(err, "Failed to ensure there is one finished job by listing job explicitly in namespace %s", ns)
	activeJobs, finishedJobs = filterActiveJobs(jobs)
	if len(finishedJobs) != 1 {
		framework.Logf("Expected one finished job in namespace %s; activeJobs=%v; finishedJobs=%v", ns, activeJobs, finishedJobs)
		gomega.Expect(finishedJobs).To(gomega.HaveLen(1))
	}

	ginkgo.By("Removing cronjob")
	err = deleteCronJob(ctx, c, ns, cronJob.Name)
	framework.ExpectNoError(err, "Failed to remove the %s cronjob in namespace %s", cronJob.Name, ns)
}

// newTestCronJob 返回一个执行某种测试行为的 cronjob。
func newTestCronJob(name, schedule string, concurrencyPolicy batchv1.ConcurrencyPolicy,
	command []string, successfulJobsHistoryLimit *int32, failedJobsHistoryLimit *int32) *batchv1.CronJob {
	parallelism := int32(1)
	completions := int32(1)
	backofflimit := int32(1)
	sj := &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		TypeMeta: metav1.TypeMeta{
			Kind: "CronJob",
		},
		Spec: batchv1.CronJobSpec{
			Schedule:          schedule,
			ConcurrencyPolicy: concurrencyPolicy,
			JobTemplate: batchv1.JobTemplateSpec{
				Spec: batchv1.JobSpec{
					Parallelism:  &parallelism,
					Completions:  &completions,
					BackoffLimit: &backofflimit,
					Template: v1.PodTemplateSpec{
						Spec: v1.PodSpec{
							RestartPolicy: v1.RestartPolicyOnFailure,
							Volumes: []v1.Volume{
								{
									Name: "data",
									VolumeSource: v1.VolumeSource{
										EmptyDir: &v1.EmptyDirVolumeSource{},
									},
								},
							},
							Containers: []v1.Container{
								{
									Name:  "c",
									Image: imageutils.GetE2EImage(imageutils.BusyBox),
									VolumeMounts: []v1.VolumeMount{
										{
											MountPath: "/data",
											Name:      "data",
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}
	sj.Spec.SuccessfulJobsHistoryLimit = successfulJobsHistoryLimit
	sj.Spec.FailedJobsHistoryLimit = failedJobsHistoryLimit
	if command != nil {
		sj.Spec.JobTemplate.Spec.Template.Spec.Containers[0].Command = command
	}
	return sj
}

func createCronJob(ctx context.Context, c clientset.Interface, ns string, cronJob *batchv1.CronJob) (*batchv1.CronJob, error) {
	return c.BatchV1().CronJobs(ns).Create(ctx, cronJob, metav1.CreateOptions{})
}

func getCronJob(ctx context.Context, c clientset.Interface, ns, name string) (*batchv1.CronJob, error) {
	return c.BatchV1().CronJobs(ns).Get(ctx, name, metav1.GetOptions{})
}

func deleteCronJob(ctx context.Context, c clientset.Interface, ns, name string) error {
	propagationPolicy := metav1.DeletePropagationBackground // Also delete jobs and pods related to cronjob
	return c.BatchV1().CronJobs(ns).Delete(ctx, name, metav1.DeleteOptions{PropagationPolicy: &propagationPolicy})
}

// 等待至少指定数量的 active job。
func waitForActiveJobs(ctx context.Context, c clientset.Interface, ns, cronJobName string, active int) error {
	return wait.PollUntilContextTimeout(ctx, framework.Poll, cronJobTimeout, false, func(ctx context.Context) (bool, error) {
		curr, err := getCronJob(ctx, c, ns, cronJobName)
		if err != nil {
			return false, err
		}
		return len(curr.Status.Active) >= active, nil
	})
}

// 等待给定 job 真正从 cronjob 的 Active 列表中消失
func waitForJobNotActive(ctx context.Context, c clientset.Interface, ns, cronJobName, jobName string) error {
	return wait.PollUntilContextTimeout(ctx, framework.Poll, cronJobTimeout, false, func(ctx context.Context) (bool, error) {
		curr, err := getCronJob(ctx, c, ns, cronJobName)
		if err != nil {
			return false, err
		}

		for _, j := range curr.Status.Active {
			if j.Name == jobName {
				return false, nil
			}
		}
		return true, nil
	})
}

// 通过显式列举 job，等待某个 job 消失。
func waitForJobToDisappear(ctx context.Context, c clientset.Interface, ns string, targetJob *batchv1.Job) error {
	return wait.PollUntilContextTimeout(ctx, framework.Poll, cronJobTimeout, false, func(ctx context.Context) (bool, error) {
		jobs, err := c.BatchV1().Jobs(ns).List(ctx, metav1.ListOptions{})
		if err != nil {
			return false, err
		}
		_, finishedJobs := filterActiveJobs(jobs)
		for _, job := range finishedJobs {
			if targetJob.Namespace == job.Namespace && targetJob.Name == job.Name {
				return false, nil
			}
		}
		return true, nil
	})
}

// 通过显式列举 pod，等待某个 pod 消失。
func waitForJobsPodToDisappear(ctx context.Context, c clientset.Interface, ns string, targetJob *batchv1.Job) error {
	return wait.PollUntilContextTimeout(ctx, framework.Poll, cronJobTimeout, false, func(ctx context.Context) (bool, error) {
		options := metav1.ListOptions{LabelSelector: fmt.Sprintf("controller-uid=%s", targetJob.UID)}
		pods, err := c.CoreV1().Pods(ns).List(ctx, options)
		if err != nil {
			return false, err
		}
		return len(pods.Items) == 0, nil
	})
}

// 等待某个 job 被新 job 替换。
func waitForJobReplaced(ctx context.Context, c clientset.Interface, ns, previousJobName string) error {
	return wait.PollUntilContextTimeout(ctx, framework.Poll, cronJobTimeout, false, func(ctx context.Context) (bool, error) {
		jobs, err := c.BatchV1().Jobs(ns).List(ctx, metav1.ListOptions{})
		if err != nil {
			return false, err
		}
		// 忽略待删除的 Job，因为 Job 删除现在是异步的。
		aliveJobs := filterNotDeletedJobs(jobs)
		if len(aliveJobs) > 1 {
			return false, fmt.Errorf("more than one job is running %+v", jobs.Items)
		} else if len(aliveJobs) == 0 {
			framework.Logf("Warning: Found 0 jobs in namespace %v", ns)
			return false, nil
		}
		return aliveJobs[0].Name != previousJobName, nil
	})
}

// waitForJobsAtLeast 等待至少出现指定数量的 job。
func waitForJobsAtLeast(ctx context.Context, c clientset.Interface, ns string, atLeast int) error {
	return wait.PollUntilContextTimeout(ctx, framework.Poll, cronJobTimeout, false, func(ctx context.Context) (bool, error) {
		jobs, err := c.BatchV1().Jobs(ns).List(ctx, metav1.ListOptions{})
		if err != nil {
			return false, err
		}
		return len(jobs.Items) >= atLeast, nil
	})
}

// waitForAnyFinishedJob 等待任意已完成 job 出现。
func waitForAnyFinishedJob(ctx context.Context, c clientset.Interface, ns string) error {
	return wait.PollUntilContextTimeout(ctx, framework.Poll, cronJobTimeout, false, func(ctx context.Context) (bool, error) {
		jobs, err := c.BatchV1().Jobs(ns).List(ctx, metav1.ListOptions{})
		if err != nil {
			return false, err
		}
		for i := range jobs.Items {
			if jobutil.IsJobFinished(&jobs.Items[i]) {
				return true, nil
			}
		}
		return false, nil
	})
}

// waitForEventWithReason 等待列表中出现带指定 reason 的事件
func waitForEventWithReason(ctx context.Context, c clientset.Interface, ns, cronJobName string, reasons []string) error {
	return wait.PollUntilContextTimeout(ctx, framework.Poll, 30*time.Second, false, func(ctx context.Context) (bool, error) {
		sj, err := getCronJob(ctx, c, ns, cronJobName)
		if err != nil {
			return false, err
		}
		events, err := c.CoreV1().Events(ns).Search(scheme.Scheme, sj)
		if err != nil {
			return false, err
		}
		for _, e := range events.Items {
			for _, reason := range reasons {
				if e.Reason == reason {
					return true, nil
				}
			}
		}
		return false, nil
	})
}

// filterNotDeletedJobs 返回剔除待删除 job 后的列表。
func filterNotDeletedJobs(jobs *batchv1.JobList) []*batchv1.Job {
	var alive []*batchv1.Job
	for i := range jobs.Items {
		job := &jobs.Items[i]
		if job.DeletionTimestamp == nil {
			alive = append(alive, job)
		}
	}
	return alive
}

func filterActiveJobs(jobs *batchv1.JobList) (active []*batchv1.Job, finished []*batchv1.Job) {
	for i := range jobs.Items {
		j := jobs.Items[i]
		if !jobutil.IsJobFinished(&j) {
			active = append(active, &j)
		} else {
			finished = append(finished, &j)
		}
	}
	return
}
