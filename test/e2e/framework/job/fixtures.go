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

package job

import (
	batchv1 "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/kubernetes/test/e2e/framework"
	imageutils "k8s.io/kubernetes/test/utils/image"
	"k8s.io/utils/ptr"
)

// NewTestJob 返回一个执行若干测试行为之一的 Job。notTerminate 启动一个将
// 实际永久运行的 Job。fail 启动立即失败的 Job。succeed 启动立即成功的
// Job。randomlySucceedOrFail 启动随机成功或失败的 Job。failOnce 让 Job
// 第一次运行时失败，之后成功。name 是 Job 的名称。RestartPolicy 表示 Job 中
// 运行 Pod 的容器的重启策略。Parallelism 是 Job 的并行度，completions 是
// Job 所需的完成数量。
func NewTestJob(behavior, name string, rPol v1.RestartPolicy, parallelism, completions int32, activeDeadlineSeconds *int64, backoffLimit int32) *batchv1.Job {
	anyNode := ""
	return NewTestJobOnNode(behavior, name, rPol, parallelism, completions, activeDeadlineSeconds, backoffLimit, anyNode)
}

// NewTestJobOnNode 类似 NewTestJob，但支持指定 Job 的 Pod 运行所在的节点。
// nodeName 为空表示没有节点选择约束。
func NewTestJobOnNode(behavior, name string, rPol v1.RestartPolicy, parallelism, completions int32, activeDeadlineSeconds *int64, backoffLimit int32, nodeName string) *batchv1.Job {
	manualSelector := false
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		TypeMeta: metav1.TypeMeta{
			Kind: "Job",
		},
		Spec: batchv1.JobSpec{
			ActiveDeadlineSeconds: activeDeadlineSeconds,
			Parallelism:           &parallelism,
			Completions:           &completions,
			BackoffLimit:          &backoffLimit,
			ManualSelector:        &manualSelector,
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{JobSelectorKey: name},
				},
				Spec: v1.PodSpec{
					RestartPolicy: rPol,
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
							Name:    "c",
							Image:   imageutils.GetE2EImage(imageutils.BusyBox),
							Command: []string{},
							VolumeMounts: []v1.VolumeMount{
								{
									MountPath: "/data",
									Name:      "data",
								},
							},
							SecurityContext: &v1.SecurityContext{},
						},
					},
				},
			},
		},
	}
	if len(nodeName) > 0 {
		job.Spec.Template.Spec.NodeSelector = map[string]string{
			"kubernetes.io/hostname": nodeName,
		}
	}
	switch behavior {
	case "neverTerminate":
		// 此 job 用于升级测试，参见 test/e2e/upgrades/apps/job.go
		// 不应优化它，因为它在升级期间必须重启
		// 并继续运行
		job.Spec.Template.Spec.Containers[0].Command = []string{"sleep", "1000000"}
		job.Spec.Template.Spec.TerminationGracePeriodSeconds = ptr.To(int64(1))
	case "notTerminate":
		job.Spec.Template.Spec.Containers[0].Image = imageutils.GetPauseImageName()
	case "fail":
		job.Spec.Template.Spec.Containers[0].Command = []string{"/bin/sh", "-c", "exit 1"}
	case "failOddSucceedEven":
		job.Spec.Template.Spec.Containers[0].Command = []string{"sh", "-c"}
		job.Spec.Template.Spec.Containers[0].Args = []string{`
			if [ $(expr ${JOB_COMPLETION_INDEX} % 2) -ne 0 ]; then
				exit 1
			else
				exit 0
			fi
			`,
		}
	case "succeed":
		job.Spec.Template.Spec.Containers[0].Command = []string{"/bin/sh", "-c", "exit 0"}
	case "randomlySucceedOrFail":
		// Bash 的 $RANDOM 生成 0 - 32767 范围内的伪随机整数。
		// 除以 16384 可得到大致 50/50 的成功概率。
		job.Spec.Template.Spec.Containers[0].Command = []string{"/bin/sh", "-c", "exit $(( $RANDOM / 16384 ))"}
	case "failOnce":
		// Pod 的容器第一次运行时失败，
		// 第二次成功。检查数据卷上的文件。
		// 如果存在则成功；如果不存在则创建但失败。
		// 如果 RestartPolicy 为 Never，应设置 nodeName，
		// 以确保所有 job Pod 运行在同一个节点上，volume
		// 将从 hostPath 挂载。
		setupHostPathDirectory(job)
		job.Spec.Template.Spec.Containers[0].Command = []string{"/bin/sh", "-c"}
		job.Spec.Template.Spec.Containers[0].Args = []string{`
			if [[ -r /data/foo ]]
			then
				exit 0
			else
				touch /data/foo
				exit 1
			fi
		`}
	case "failOncePerIndex":
		// 按索引使用标记文件。如果给定的标记文件已存在，
		// 则成功退出；否则创建标记文件并
		// 以退出码 42 失败。
		setupHostPathDirectory(job)
		job.Spec.Template.Spec.Containers[0].Command = []string{"/bin/sh", "-c"}
		job.Spec.Template.Spec.Containers[0].Args = []string{`
			if [[ -r /data/foo-$JOB_COMPLETION_INDEX ]]
			then
				exit 0
			else
				touch /data/foo-$JOB_COMPLETION_INDEX
				exit 42
			fi
		`}
	case "notTerminateOncePerIndex":
		// 按索引使用标记文件。如果给定的标记文件已存在，
		// 则成功退出；否则创建标记文件并
		// 长期休眠，等待删除请求。
		setupHostPathDirectory(job)
		job.Spec.Template.Spec.TerminationGracePeriodSeconds = ptr.To(int64(1))
		job.Spec.Template.Spec.Containers[0].Command = []string{"/bin/sh", "-c"}
		job.Spec.Template.Spec.Containers[0].Args = []string{`
			if [[ -r /data/foo-$JOB_COMPLETION_INDEX ]]
			then
				exit 0
			else
				touch /data/foo-$JOB_COMPLETION_INDEX
				sleep 1000000
			fi
		`}
		// 添加 readiness probe，让测试客户端可以在驱逐 Pod 前检查标记
		// 文件是否已创建。
		job.Spec.Template.Spec.Containers[0].ReadinessProbe = &v1.Probe{
			PeriodSeconds: 1,
			ProbeHandler: v1.ProbeHandler{
				Exec: &v1.ExecAction{
					Command: []string{"/bin/sh", "-c", "cat /data/foo-$JOB_COMPLETION_INDEX"},
				},
			},
		}
	}
	return job
}

// 设置 host path 目录，在 Pod 重启之间传递信息
func setupHostPathDirectory(job *batchv1.Job) {
	if _, nodeNameSpecified := job.Spec.Template.Spec.NodeSelector["kubernetes.io/hostname"]; nodeNameSpecified {
		randomDir := "/tmp/job-e2e/" + rand.String(10)
		hostPathType := v1.HostPathDirectoryOrCreate
		job.Spec.Template.Spec.Volumes[0].VolumeSource = v1.VolumeSource{HostPath: &v1.HostPathVolumeSource{Path: randomDir, Type: &hostPathType}}
		// 涉及 hostPath volume 读写操作的测试，在启用 SELinux 的发行版上需要以
		// 特权模式运行，而 Windows 平台
		// 既不支持也不需要特权模式。
		privileged := !framework.NodeOSDistroIs("windows")
		job.Spec.Template.Spec.Containers[0].SecurityContext.Privileged = &privileged
	}
}
