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

package cronjob

import (
	"context"
	"sync"

	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
)

// cjControlInterface 定义更新 CronJob 状态所需的能力。
// 抽象成接口是为了在测试中注入假实现，便于隔离控制器逻辑。
type cjControlInterface interface {
	// UpdateStatus 更新给定 CronJob 的状态。
	UpdateStatus(ctx context.Context, cj *batchv1.CronJob) (*batchv1.CronJob, error)
	// GetCronJob 获取指定命名空间和名称的 CronJob。
	GetCronJob(ctx context.Context, namespace, name string) (*batchv1.CronJob, error)
}

// realCJControl 是 cjControlInterface 的默认实现，直接调用 clientset 访问 API Server。
type realCJControl struct {
	KubeClient clientset.Interface
}

// GetCronJob 通过 clientset 从 API Server 读取 CronJob。
func (c *realCJControl) GetCronJob(ctx context.Context, namespace, name string) (*batchv1.CronJob, error) {
	return c.KubeClient.BatchV1().CronJobs(namespace).Get(ctx, name, metav1.GetOptions{})
}

var _ cjControlInterface = &realCJControl{}

// UpdateStatus 通过 clientset 将 CronJob 的状态写回 API Server。
func (c *realCJControl) UpdateStatus(ctx context.Context, cj *batchv1.CronJob) (*batchv1.CronJob, error) {
	return c.KubeClient.BatchV1().CronJobs(cj.Namespace).UpdateStatus(ctx, cj, metav1.UpdateOptions{})
}

// fakeCJControl 是 cjControlInterface 的测试实现，用内存中的对象替代真实 API 调用。
type fakeCJControl struct {
	CronJob *batchv1.CronJob
	Updates []batchv1.CronJob
}

// GetCronJob 仅在命名空间和名称都匹配时返回内存中的 CronJob，否则返回 NotFound。
func (c *fakeCJControl) GetCronJob(ctx context.Context, namespace, name string) (*batchv1.CronJob, error) {
	if name == c.CronJob.Name && namespace == c.CronJob.Namespace {
		return c.CronJob, nil
	}
	return nil, errors.NewNotFound(schema.GroupResource{
		Group:    "v1beta1",
		Resource: "cronjobs",
	}, name)
}

var _ cjControlInterface = &fakeCJControl{}

// UpdateStatus 记录状态更新调用而不真正写 API，供测试断言使用情况。
func (c *fakeCJControl) UpdateStatus(ctx context.Context, cj *batchv1.CronJob) (*batchv1.CronJob, error) {
	c.Updates = append(c.Updates, *cj)
	return cj, nil
}

// ------------------------------------------------------------------ //

// jobControlInterface 定义增删 Job 所需的能力。
// 抽象成接口是为了在测试中注入假实现，便于隔离控制器逻辑。
type jobControlInterface interface {
	// GetJob 获取指定命名空间和名称的 Job。
	GetJob(namespace, name string) (*batchv1.Job, error)
	// CreateJob 根据给定的 spec 创建 Job。
	CreateJob(namespace string, job *batchv1.Job) (*batchv1.Job, error)
	// DeleteJob 删除指定名称的 Job。
	// TODO: 改为按 UID 删除？
	DeleteJob(namespace string, name string) error
}

// realJobControl 是 jobControlInterface 的默认实现，直接调用 clientset 访问 API Server。
type realJobControl struct {
	KubeClient clientset.Interface
	Recorder   record.EventRecorder
}

var _ jobControlInterface = &realJobControl{}

// GetJob 通过 clientset 从 API Server 读取 Job。
func (r realJobControl) GetJob(namespace, name string) (*batchv1.Job, error) {
	return r.KubeClient.BatchV1().Jobs(namespace).Get(context.TODO(), name, metav1.GetOptions{})
}

// CreateJob 通过 clientset 在 API Server 中创建 Job。
func (r realJobControl) CreateJob(namespace string, job *batchv1.Job) (*batchv1.Job, error) {
	return r.KubeClient.BatchV1().Jobs(namespace).Create(context.TODO(), job, metav1.CreateOptions{})
}

// DeleteJob 通过 clientset 删除 Job，并指定后台传播策略以级联清理其依赖资源。
func (r realJobControl) DeleteJob(namespace string, name string) error {
	background := metav1.DeletePropagationBackground
	return r.KubeClient.BatchV1().Jobs(namespace).Delete(context.TODO(), name, metav1.DeleteOptions{PropagationPolicy: &background})
}

// fakeJobControl 是 jobControlInterface 的测试实现，用内存记录代替真实 API 调用。
// 内嵌 Mutex 保护并发访问，字段记录每次调用的参数与预设的错误。
type fakeJobControl struct {
	sync.Mutex
	Job             *batchv1.Job
	Jobs            []batchv1.Job
	DeleteJobName   []string
	Err             error
	CreateErr       error
	UpdateJobName   []string
	PatchJobName    []string
	Patches         [][]byte
	GetJobNamespace []string
}

var _ jobControlInterface = &fakeJobControl{}

// CreateJob 记录创建的 Job；若预设 CreateErr 则返回错误，否则为新 Job 分配测试 UID。
func (f *fakeJobControl) CreateJob(namespace string, job *batchv1.Job) (*batchv1.Job, error) {
	f.Lock()
	defer f.Unlock()
	if f.CreateErr != nil {
		return nil, f.CreateErr
	}
	f.Jobs = append(f.Jobs, *job)
	job.UID = "test-uid"
	return job, nil
}

// GetJob 记录查询的命名空间并返回内存中的 Job；Err 非空时返回预设错误。
func (f *fakeJobControl) GetJob(namespace, name string) (*batchv1.Job, error) {
	f.Lock()
	defer f.Unlock()
	if f.Err != nil {
		return nil, f.Err
	}
	f.GetJobNamespace = append(f.GetJobNamespace, namespace)
	return f.Job, nil
}

// DeleteJob 记录被删除的 Job 名称；Err 非空时返回预设错误而不做记录。
func (f *fakeJobControl) DeleteJob(namespace string, name string) error {
	f.Lock()
	defer f.Unlock()
	if f.Err != nil {
		return f.Err
	}
	f.DeleteJobName = append(f.DeleteJobName, name)
	return nil
}
