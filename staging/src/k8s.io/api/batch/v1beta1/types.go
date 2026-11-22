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

package v1beta1

import (
	batchv1 "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// JobTemplateSpec 描述从模板创建 Job 时该 Job 应具备的数据。
type JobTemplateSpec struct {
	// 从该模板创建的 Job 的标准对象元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	// +k8s:opaqueType
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// 期望的 Job 行为规格。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	Spec batchv1.JobSpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.8
// +k8s:prerelease-lifecycle-gen:deprecated=1.21
// +k8s:prerelease-lifecycle-gen:removed=1.25
// +k8s:prerelease-lifecycle-gen:replacement=batch,v1,CronJob

// CronJob 表示单个定时 Job 的配置。
// +k8s:supportsSubresource="/status"
type CronJob struct {
	metav1.TypeMeta `json:""`
	// 标准对象元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// 定时 Job 的期望行为规格，包括调度计划。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +required
	Spec CronJobSpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`

	// 定时 Job 的当前状态。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	Status CronJobStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.8
// +k8s:prerelease-lifecycle-gen:deprecated=1.21
// +k8s:prerelease-lifecycle-gen:removed=1.25
// +k8s:prerelease-lifecycle-gen:replacement=batch,v1,CronJobList

// CronJobList 是定时 Job 的集合。
type CronJobList struct {
	metav1.TypeMeta `json:""`

	// 标准列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// items 是 CronJob 列表。
	Items []CronJob `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// CronJobSpec 描述 Job 的执行方式与实际运行时间。
type CronJobSpec struct {

	// Cron 格式的调度计划，参见 https://en.wikipedia.org/wiki/Cron。
	// +required
	// +k8s:beta(since: "1.37")=+k8s:required
	Schedule string `json:"schedule" protobuf:"bytes,1,opt,name=schedule"`

	// 调度计划所使用的时区名称，参见 https://en.wikipedia.org/wiki/List_of_tz_database_time_zones。
	// 未指定时默认为 kube-controller-manager 进程的时区。
	// API 服务器在校验 CronJob、控制器管理器在执行期间，会从系统时区数据库加载有效时区名及偏移量集合。
	// 找不到系统时区数据库时，会改用内置的数据库版本。
	// 若时区名在 CronJob 生命周期内因主机配置变更而失效，控制器将停止创建新 Job，
	// 并产生原因为 UnknownTimeZone 的系统事件。
	// 更多信息：https://kubernetes.io/docs/concepts/workloads/controllers/cron-jobs/#time-zones
	// +optional
	TimeZone *string `json:"timeZone,omitempty" protobuf:"bytes,8,opt,name=timeZone"`

	// 可选的截止时长（秒）：因任何原因错过调度时间后，仍可启动 Job 的宽限时间。
	// 错过的 Job 执行会被计为失败。
	// +optional
	StartingDeadlineSeconds *int64 `json:"startingDeadlineSeconds,omitempty" protobuf:"varint,2,opt,name=startingDeadlineSeconds"`

	// 指定如何处理 Job 的并发执行。
	// 有效值为：
	//
	// - "Allow"（默认）：允许 CronJob 并发运行；
	// - "Forbid": 禁止并发运行，若上一次运行尚未结束则跳过下一次；
	// - "Replace": 取消当前正在运行的 Job 并用新 Job 替换。
	// +optional
	ConcurrencyPolicy ConcurrencyPolicy `json:"concurrencyPolicy,omitempty" protobuf:"bytes,3,opt,name=concurrencyPolicy,casttype=ConcurrencyPolicy"`

	// 该标志告知控制器暂停后续执行，对已开始的执行无效。默认为 false。
	// +optional
	Suspend *bool `json:"suspend,omitempty" protobuf:"varint,4,opt,name=suspend"`

	// 指定执行 CronJob 时将创建的 Job。
	JobTemplate JobTemplateSpec `json:"jobTemplate" protobuf:"bytes,5,opt,name=jobTemplate"`

	// 保留的成功结束 Job 数量。
	// 使用指针以区分显式设置为 0 和未指定。默认为 3。
	// +optional
	SuccessfulJobsHistoryLimit *int32 `json:"successfulJobsHistoryLimit,omitempty" protobuf:"varint,6,opt,name=successfulJobsHistoryLimit"`

	// 保留的失败结束 Job 数量。
	// 使用指针以区分显式设置为 0 和未指定。默认为 1。
	// +optional
	FailedJobsHistoryLimit *int32 `json:"failedJobsHistoryLimit,omitempty" protobuf:"varint,7,opt,name=failedJobsHistoryLimit"`
}

// ConcurrencyPolicy 描述 Job 的处理方式。
// 只能指定以下并发策略之一。若未指定任何策略，默认为 AllowConcurrent。
type ConcurrencyPolicy string

const (
	// AllowConcurrent 允许 CronJob 并发运行。
	AllowConcurrent ConcurrencyPolicy = "Allow"

	// ForbidConcurrent 禁止并发运行：若上一次运行尚未结束则跳过下一次。
	ForbidConcurrent ConcurrencyPolicy = "Forbid"

	// ReplaceConcurrent 取消当前正在运行的 Job 并用新 Job 替换。
	ReplaceConcurrent ConcurrencyPolicy = "Replace"
)

// CronJobStatus 表示定时 Job 的当前状态。
type CronJobStatus struct {
	// 指向当前正在运行的 Job 的引用列表。
	// +optional
	// +listType=atomic
	Active []v1.ObjectReference `json:"active,omitempty" protobuf:"bytes,1,rep,name=active"`

	// 上一次成功调度 Job 的时间信息。
	// +optional
	LastScheduleTime *metav1.Time `json:"lastScheduleTime,omitempty" protobuf:"bytes,4,opt,name=lastScheduleTime"`

	// 上一次 Job 成功完成的时间信息。
	// +optional
	LastSuccessfulTime *metav1.Time `json:"lastSuccessfulTime,omitempty" protobuf:"bytes,5,opt,name=lastSuccessfulTime"`
}
