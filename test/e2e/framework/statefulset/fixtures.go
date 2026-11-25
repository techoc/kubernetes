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

package statefulset

import (
	"context"
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strconv"

	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubectl/pkg/util/podutils"
	"k8s.io/kubernetes/test/e2e/framework"
	e2epodoutput "k8s.io/kubernetes/test/e2e/framework/pod/output"
	imageutils "k8s.io/kubernetes/test/utils/image"
	"k8s.io/utils/ptr"
)

// NewStatefulSet 创建一个用于测试的 Webserver StatefulSet，名称为 name，位于命名空间 ns。
// statefulPodsMounts 是由 PV 支撑的挂载点，podsMounts 是直接挂载到 Pod 的挂载点，
// labels 是 StatefulSet selector 使用的标签。
func NewStatefulSet(name, ns, governingSvcName string, replicas int32, statefulPodMounts []v1.VolumeMount, podMounts []v1.VolumeMount, labels map[string]string) *appsv1.StatefulSet {
	mounts := append(statefulPodMounts, podMounts...)
	claims := []v1.PersistentVolumeClaim{}
	for _, m := range statefulPodMounts {
		claims = append(claims, NewStatefulSetPVC(m.Name))
	}

	vols := []v1.Volume{}
	for _, m := range podMounts {
		vols = append(vols, v1.Volume{
			Name: m.Name,
			VolumeSource: v1.VolumeSource{
				HostPath: &v1.HostPathVolumeSource{
					Path: fmt.Sprintf("/tmp/%v", m.Name),
				},
			},
		})
	}

	return &appsv1.StatefulSet{
		TypeMeta: metav1.TypeMeta{
			Kind:       "StatefulSet",
			APIVersion: "apps/v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
		},
		Spec: appsv1.StatefulSetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Replicas: ptr.To[int32](replicas),
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      labels,
					Annotations: map[string]string{},
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name:         "webserver",
							Image:        imageutils.GetE2EImage(imageutils.AgnhostPrev),
							VolumeMounts: mounts,
							Args:         []string{"test-webserver"},
						},
					},
					Volumes: vols,
				},
			},
			UpdateStrategy:       appsv1.StatefulSetUpdateStrategy{Type: appsv1.RollingUpdateStatefulSetStrategyType},
			VolumeClaimTemplates: claims,
			ServiceName:          governingSvcName,
		},
	}
}

// NewStatefulSetPVC 返回名为 name 的 PVC，供 StatefulSet 测试使用。
func NewStatefulSetPVC(name string) v1.PersistentVolumeClaim {
	return v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: v1.PersistentVolumeClaimSpec{
			AccessModes: []v1.PersistentVolumeAccessMode{
				v1.ReadWriteOnce,
			},
			Resources: v1.VolumeResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceStorage: resource.MustParse("10Gi"),
				},
			},
		},
	}
}

func hasPauseProbe(pod *v1.Pod) bool {
	probe := pod.Spec.Containers[0].ReadinessProbe
	return probe != nil && reflect.DeepEqual(probe.Exec.Command, pauseProbe.Exec.Command)
}

var pauseProbe = &v1.Probe{
	ProbeHandler: v1.ProbeHandler{
		Exec: &v1.ExecAction{Command: []string{"test", "-f", "/data/statefulset-continue"}},
	},
	PeriodSeconds:    1,
	SuccessThreshold: 1,
	FailureThreshold: 1,
}

type statefulPodsByOrdinal []v1.Pod

func (sp statefulPodsByOrdinal) Len() int {
	return len(sp)
}

func (sp statefulPodsByOrdinal) Swap(i, j int) {
	sp[i], sp[j] = sp[j], sp[i]
}

func (sp statefulPodsByOrdinal) Less(i, j int) bool {
	return getStatefulPodOrdinal(&sp[i]) < getStatefulPodOrdinal(&sp[j])
}

// PauseNewPods 向 StatefulSet PodTemplate 添加一个必然失败的 ReadinessProbe，
// 使所有新建 Pod 保持未 Ready，直到通过 ResumeNextPod() 手动恢复。
// 注意它不能与 SetHTTPProbe() 同时使用。
func PauseNewPods(ss *appsv1.StatefulSet) {
	ss.Spec.Template.Spec.Containers[0].ReadinessProbe = pauseProbe
}

// ResumeNextPod 移除 PauseNewPods() 添加的 ReadinessProbe（若仍存在），让 StatefulSet 中的下一个 Pod 继续。
// 若发现非 Running 的 Pod，或同时存在多个暂停的 Pod，则测试失败。
// 没有暂停的 Pod 时该操作为空操作。
func ResumeNextPod(ctx context.Context, c clientset.Interface, ss *appsv1.StatefulSet) {
	podList, err := GetPodList(ctx, c, ss)
	framework.ExpectNoError(err)

	resumedPod := ""
	for _, pod := range podList.Items {
		if pod.Status.Phase != v1.PodRunning {
			framework.Failf("Found pod in phase %q, cannot resume", pod.Status.Phase)
		}
		if podutils.IsPodReady(&pod) || !hasPauseProbe(&pod) {
			continue
		}
		if resumedPod != "" {
			framework.Failf("Found multiple paused stateful pods: %v and %v", pod.Name, resumedPod)
		}
		_, err := e2epodoutput.RunHostCmdWithRetries(pod.Namespace, pod.Name, "dd if=/dev/zero of=/data/statefulset-continue bs=1 count=1 conv=fsync", StatefulSetPoll, StatefulPodTimeout)
		framework.ExpectNoError(err)
		framework.Logf("Resumed pod %v", pod.Name)
		resumedPod = pod.Name
	}
}

// SortStatefulPods 按序号对 Pod 排序
func SortStatefulPods(pods *v1.PodList) {
	sort.Sort(statefulPodsByOrdinal(pods.Items))
}

var statefulPodRegex = regexp.MustCompile("(.*)-([0-9]+)$")

func getStatefulPodOrdinal(pod *v1.Pod) int {
	ordinal := -1
	subMatches := statefulPodRegex.FindStringSubmatch(pod.Name)
	if len(subMatches) < 3 {
		return ordinal
	}
	if i, err := strconv.ParseInt(subMatches[2], 10, 32); err == nil {
		ordinal = int(i)
	}
	return ordinal
}
