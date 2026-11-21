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

package sliceutils

import (
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
)

// 构造三个创建时间依次递增一小时的 Pod，供排序接口测试复用。
func buildPodsByCreationTime() PodsByCreationTime {
	return []*v1.Pod{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "foo1",
				Namespace: v1.NamespaceDefault,
				CreationTimestamp: metav1.Time{
					Time: time.Now(),
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "foo2",
				Namespace: v1.NamespaceDefault,
				CreationTimestamp: metav1.Time{
					Time: time.Now().Add(time.Hour * 1),
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "foo3",
				Namespace: v1.NamespaceDefault,
				CreationTimestamp: metav1.Time{
					Time: time.Now().Add(time.Hour * 2),
				},
			},
		},
	}
}

// TestPodsByCreationTimeLen 验证 Len 返回底层切片长度，覆盖空切片、nil 与含 nil 元素的切片。
func TestPodsByCreationTimeLen(t *testing.T) {
	fooTests := []struct {
		pods PodsByCreationTime
		el   int
	}{
		{[]*v1.Pod{}, 0},
		{buildPodsByCreationTime(), 3},
		{[]*v1.Pod{nil, {}}, 2},
		{nil, 0},
	}

	for _, fooTest := range fooTests {
		r := fooTest.pods.Len()
		if r != fooTest.el {
			t.Errorf("returned %d but expected %d for the len of PodsByCreationTime=%s", r, fooTest.el, fooTest.pods)
		}
	}
}

// TestPodsByCreationTimeSwap 验证 Swap 会交换指定下标上的元素。
func TestPodsByCreationTimeSwap(t *testing.T) {
	fooTests := []struct {
		pods PodsByCreationTime
		i    int
		j    int
	}{
		{buildPodsByCreationTime(), 0, 1},
		{buildPodsByCreationTime(), 2, 1},
	}

	for _, fooTest := range fooTests {
		// 先记录交换前的元素，交换后再按名字确认两个位置确实对调。
		fooi := fooTest.pods[fooTest.i]
		fooj := fooTest.pods[fooTest.j]
		fooTest.pods.Swap(fooTest.i, fooTest.j)
		if fooi.GetName() != fooTest.pods[fooTest.j].GetName() || fooj.GetName() != fooTest.pods[fooTest.i].GetName() {
			t.Errorf("failed to swap for %v", fooTest)
		}
	}
}

// TestPodsByCreationTimeLess 验证 Less 按创建时间升序比较，供排序使用。
func TestPodsByCreationTimeLess(t *testing.T) {
	fooTests := []struct {
		pods PodsByCreationTime
		i    int
		j    int
		er   bool
	}{
		// 升序场景：foo1 早于 foo3，因此 Less 应为 true。
		{buildPodsByCreationTime(), 0, 2, true},
		{buildPodsByCreationTime(), 1, 0, false},
	}

	for _, fooTest := range fooTests {
		r := fooTest.pods.Less(fooTest.i, fooTest.j)
		if r != fooTest.er {
			t.Errorf("returned %t but expected %t for the foo=%s", r, fooTest.er, fooTest.pods)
		}
	}
}

// 构造四个镜像，Size 依次为 1/2/3/3；第 3、4 个大小相同，用于验证相等元素的比较。
func buildByImageSize() ByImageSize {
	return []kubecontainer.Image{
		{
			ID:          "1",
			RepoTags:    []string{"foo-tag11", "foo-tag12"},
			RepoDigests: []string{"foo-rd11", "foo-rd12"},
			Size:        1,
		},
		{
			ID:          "2",
			RepoTags:    []string{"foo-tag21", "foo-tag22"},
			RepoDigests: []string{"foo-rd21", "foo-rd22"},
			Size:        2,
		},
		{
			ID:          "3",
			RepoTags:    []string{"foo-tag31", "foo-tag32"},
			RepoDigests: []string{"foo-rd31", "foo-rd32"},
			Size:        3,
		},
		{
			ID:          "4",
			RepoTags:    []string{"foo-tag41", "foo-tag42"},
			RepoDigests: []string{"foo-rd41", "foo-rd42"},
			Size:        3,
		},
	}
}

// TestByImageSizeLen 验证 Len 返回镜像切片长度。
func TestByImageSizeLen(t *testing.T) {
	fooTests := []struct {
		images ByImageSize
		el     int
	}{
		{[]kubecontainer.Image{}, 0},
		{buildByImageSize(), 4},
		{nil, 0},
	}

	for _, fooTest := range fooTests {
		r := fooTest.images.Len()
		if r != fooTest.el {
			t.Errorf("returned %d but expected %d for the len of ByImageSize=%v", r, fooTest.el, fooTest.images)
		}
	}
}

// TestByImageSizeSwap 验证 Swap 会交换指定下标上的镜像。
func TestByImageSizeSwap(t *testing.T) {
	fooTests := []struct {
		images ByImageSize
		i      int
		j      int
	}{
		{buildByImageSize(), 0, 1},
		{buildByImageSize(), 2, 1},
	}

	for _, fooTest := range fooTests {
		// 先记录交换前的镜像，交换后按 ID 确认两个位置确实对调。
		fooi := fooTest.images[fooTest.i]
		fooj := fooTest.images[fooTest.j]
		fooTest.images.Swap(fooTest.i, fooTest.j)
		if fooi.ID != fooTest.images[fooTest.j].ID || fooj.ID != fooTest.images[fooTest.i].ID {
			t.Errorf("failed to swap for %v", fooTest)
		}
	}
}

// TestByImageSizeLess 验证 Less 按镜像大小降序比较，使大镜像在 GC 时排在前面。
func TestByImageSizeLess(t *testing.T) {
	fooTests := []struct {
		images ByImageSize
		i      int
		j      int
		er     bool
	}{
		// 降序场景：大镜像应排在前面，因此 Less 对 (0,2) 为 false、对 (1,0) 为 true。
		{buildByImageSize(), 0, 2, false},
		{buildByImageSize(), 1, 0, true},
		{buildByImageSize(), 3, 2, true},
	}

	for _, fooTest := range fooTests {
		r := fooTest.images.Less(fooTest.i, fooTest.j)
		if r != fooTest.er {
			t.Errorf("returned %t but expected %t for the foo=%v", r, fooTest.er, fooTest.images)
		}
	}
}
