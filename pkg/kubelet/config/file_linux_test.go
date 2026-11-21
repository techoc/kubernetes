//go:build linux

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

package config

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	clientscheme "k8s.io/client-go/kubernetes/scheme"
	api "k8s.io/kubernetes/pkg/apis/core"
	k8s_api_v1 "k8s.io/kubernetes/pkg/apis/core/v1"
	"k8s.io/kubernetes/pkg/apis/core/validation"
	kubetypes "k8s.io/kubernetes/pkg/kubelet/types"
	"k8s.io/kubernetes/pkg/securitycontext"
	"k8s.io/kubernetes/test/utils/ktesting"
)

// TestExtractFromNonExistentFile 验证从不存在文件对应的 sourceFile 执行 doWatch 会返回错误。
func TestExtractFromNonExistentFile(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	ch := make(chan sourceUpdate, 1)
	lw := newSourceFile("/some/fake/file", "localhost", time.Millisecond, ch)
	err := lw.doWatch(logger)
	if err == nil {
		t.Errorf("Expected error")
	}
}

// TestUpdateOnNonExistentFile 验证监听不存在的文件路径时会收到一个空的 Pod 更新。
func TestUpdateOnNonExistentFile(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	ch := make(chan sourceUpdate)
	NewSourceFile(logger, "random_non_existent_path", "localhost", time.Millisecond, ch)
	select {
	case update := <-ch:
		expected := createSourceUpdate() // 期望收到空更新。
		if !apiequality.Semantic.DeepDerivative(expected, update) {
			t.Fatalf("expected %#v, Got %#v", expected, update)
		}

	case <-time.After(wait.ForeverTestTimeout):
		t.Fatalf("expected update, timeout instead")
	}
}

// TestReadPodsFromFileExistAlready 验证启动时已存在的 Pod 清单文件能被正确解析、
// 通过合法性校验，并生成期望的 sourceUpdate。
func TestReadPodsFromFileExistAlready(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	hostname := types.NodeName("random-test-hostname")
	var testCases = getTestCases(hostname)

	for _, testCase := range testCases {
		func() {
			dirName, err := mkTempDir("file-test")
			if err != nil {
				t.Fatalf("unable to create temp dir: %v", err)
			}
			defer os.RemoveAll(dirName)
			file := testCase.writeToFile(dirName, "test_pod_manifest", t)

			ch := make(chan sourceUpdate)
			NewSourceFile(logger, file, hostname, time.Millisecond, ch)
			select {
			case update := <-ch:
				for _, pod := range update.Pods {
					// TODO: 当校验直接支持版本化对象后，移除这里的转换。
					internalPod := &api.Pod{}
					if err := k8s_api_v1.Convert_v1_Pod_To_core_Pod(pod, internalPod, nil); err != nil {
						t.Fatalf("%s: Cannot convert pod %#v, %#v", testCase.desc, pod, err)
					}
					if errs := validation.ValidatePodCreate(internalPod, validation.PodValidationOptions{}); len(errs) > 0 {
						t.Fatalf("%s: Invalid pod %#v, %#v", testCase.desc, internalPod, errs)
					}
				}
				// 解析结果必须与期望的更新完全一致。
				if !apiequality.Semantic.DeepEqual(testCase.expected, update) {
					t.Fatalf("%s: Expected %#v, Got %#v", testCase.desc, testCase.expected, update)
				}
			case <-time.After(wait.ForeverTestTimeout):
				t.Fatalf("%s: Expected update, timeout instead", testCase.desc)
			}
		}()
	}
}

var (
	testCases = []struct {
		watchDir bool
		symlink  bool
		period   time.Duration
	}{
		// 周期要足够长，以便文件内容被修改；又要足够短，以便能及时触发事件。
		{true, true, 3 * time.Second},

		// 周期设置得足够长，避免周期性的 PodUpdate 事件干扰断言。
		{true, false, 60 * time.Second},
		{false, true, 60 * time.Second},
		{false, false, 60 * time.Second},
	}
)

// TestWatchFileAdded 验证在监听目录或文件、使用或不使用符号链接的各种组合下，
// 新增清单文件都能触发 Pod 更新。
func TestWatchFileAdded(t *testing.T) {
	for _, testCase := range testCases {
		watchFileAdded(testCase.watchDir, testCase.symlink, t)
	}
}

// TestWatchFileChanged 验证清单文件内容修改与文件重命名都能触发相应的 Pod 更新事件。
func TestWatchFileChanged(t *testing.T) {
	for _, testCase := range testCases {
		watchFileChanged(testCase.watchDir, testCase.symlink, testCase.period, t)
	}
}

// testCase 描述单个 Pod 清单文件解析测试用例。
type testCase struct {
	lock     *sync.Mutex
	desc     string
	pod      runtime.Object
	expected sourceUpdate
}

// getTestCases 构造以给定节点名解析 Pod 清单的测试用例集合。
func getTestCases(hostname types.NodeName) []*testCase {
	grace := int64(30)
	enableServiceLinks := v1.DefaultEnableServiceLinks
	return []*testCase{
		{
			lock: &sync.Mutex{},
			desc: "Simple pod",
			pod: &v1.Pod{
				TypeMeta: metav1.TypeMeta{
					Kind:       "Pod",
					APIVersion: "",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test",
					UID:       "12345",
					Namespace: "mynamespace",
				},
				Spec: v1.PodSpec{
					Containers:      []v1.Container{{Name: "image", Image: "test/image", SecurityContext: securitycontext.ValidSecurityContextWithContainerDefaults()}},
					SecurityContext: &v1.PodSecurityContext{},
					SchedulerName:   v1.DefaultSchedulerName,
				},
				Status: v1.PodStatus{
					Phase: v1.PodPending,
				},
			},
			expected: createSourceUpdate(&v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "test-" + string(hostname),
					UID:         "12345",
					Namespace:   "mynamespace",
					Annotations: map[string]string{kubetypes.ConfigHashAnnotationKey: "12345"},
				},
				Spec: v1.PodSpec{
					NodeName:                      string(hostname),
					RestartPolicy:                 v1.RestartPolicyAlways,
					DNSPolicy:                     v1.DNSClusterFirst,
					TerminationGracePeriodSeconds: &grace,
					Tolerations: []v1.Toleration{{
						Operator: "Exists",
						Effect:   "NoExecute",
					}},
					Containers: []v1.Container{{
						Name:                     "image",
						Image:                    "test/image",
						TerminationMessagePath:   "/dev/termination-log",
						ImagePullPolicy:          "Always",
						SecurityContext:          securitycontext.ValidSecurityContextWithContainerDefaults(),
						TerminationMessagePolicy: v1.TerminationMessageReadFile,
					}},
					SecurityContext:    &v1.PodSecurityContext{},
					SchedulerName:      v1.DefaultSchedulerName,
					EnableServiceLinks: &enableServiceLinks,
				},
				Status: v1.PodStatus{
					Phase: v1.PodPending,
				},
			}),
		},
	}
}

// writeToFile 将测试用例中的 Pod 编码后写入指定目录下的文件，返回文件路径。
func (tc *testCase) writeToFile(dir, name string, t *testing.T) string {
	fileContents, err := runtime.Encode(clientscheme.Codecs.LegacyCodec(v1.SchemeGroupVersion), tc.pod)
	if err != nil {
		t.Fatalf("%s: error in encoding the pod: %v", tc.desc, err)
	}

	fileName := filepath.Join(dir, name)
	if err := writeFile(fileName, []byte(fileContents)); err != nil {
		t.Fatalf("unable to write test file %#v", err)
	}
	return fileName
}

// createSymbolicLink 在 link 目录下创建指向 target 目录同名文件的符号链接。
func createSymbolicLink(link, target, name string, t *testing.T) string {
	linkName := filepath.Join(link, name)
	linkedFile := filepath.Join(target, name)

	err := os.Symlink(linkedFile, linkName)
	if err != nil {
		t.Fatalf("unexpected error when create symbolic link: %v", err)
	}
	return linkName
}

// watchFileAdded 是 TestWatchFileAdded 的具体执行逻辑：
// 启动文件监听后新增清单文件，并断言收到对应的更新。
func watchFileAdded(watchDir bool, symlink bool, t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	hostname := types.NodeName("random-test-hostname")
	var testCases = getTestCases(hostname)

	fileNamePre := "test_pod_manifest"
	for index, testCase := range testCases {
		func() {
			dirName, err := mkTempDir("dir-test")
			if err != nil {
				t.Fatalf("unable to create temp dir: %v", err)
			}
			defer removeAll(dirName, t)

			fileName := fmt.Sprintf("%s_%d", fileNamePre, index)
			var linkedDirName string
			if symlink {
				linkedDirName, err = mkTempDir("linked-dir-test")
				if err != nil {
					t.Fatalf("unable to create temp dir for linked files: %v", err)
				}
				defer removeAll(linkedDirName, t)
				// 通过符号链接写入文件，验证监听逻辑能跟随链接找到真实文件。
				createSymbolicLink(dirName, linkedDirName, fileName, t)
			}

			ch := make(chan sourceUpdate)
			if watchDir {
				NewSourceFile(logger, dirName, hostname, 100*time.Millisecond, ch)
			} else {
				NewSourceFile(logger, filepath.Join(dirName, fileName), hostname, 100*time.Millisecond, ch)
			}
			expectEmptyUpdate(t, ch)

			addFile := func() {
				// 新增一个清单文件。
				if symlink {
					testCase.writeToFile(linkedDirName, fileName, t)
					return
				}

				testCase.writeToFile(dirName, fileName, t)
			}

			go addFile()

			// 非目录监听：期望由 SourceFile.reloadConfig() 触发一次更新。
			// 目录监听：期望至少收到一次来自 CREATE 与 MODIFY inotify 事件的更新。
			// 不应期望 CREATE 与 MODIFY 各触发一次，因为 CREATE 时文件未必已写完，
			// 那种情况下 CREATE 事件不会发出更新。
			expectUpdate(t, ch, testCase)
		}()
	}
}

// watchFileChanged 是 TestWatchFileChanged 的具体执行逻辑：
// 先写入初始清单文件，再修改内容并重命名，验证各类事件触发的更新。
func watchFileChanged(watchDir bool, symlink bool, period time.Duration, t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	hostname := types.NodeName("random-test-hostname")
	var testCases = getTestCases(hostname)

	fileNamePre := "test_pod_manifest"
	for index, testCase := range testCases {
		func() {
			dirName, err := mkTempDir("dir-test")
			fileName := fmt.Sprintf("%s_%d", fileNamePre, index)
			if err != nil {
				t.Fatalf("unable to create temp dir: %v", err)
			}
			defer removeAll(dirName, t)

			var linkedDirName string
			if symlink {
				linkedDirName, err = mkTempDir("linked-dir-test")
				if err != nil {
					t.Fatalf("unable to create temp dir for linked files: %v", err)
				}
				defer removeAll(linkedDirName, t)
				createSymbolicLink(dirName, linkedDirName, fileName, t)
			}

			// 初始写入清单文件；加锁避免与监听协程并发读写同一文件。
			var file string
			ch := make(chan sourceUpdate)
			func() {
				testCase.lock.Lock()
				defer testCase.lock.Unlock()

				if symlink {
					file = testCase.writeToFile(linkedDirName, fileName, t)
					return
				}

				file = testCase.writeToFile(dirName, fileName, t)
			}()

			if watchDir {
				NewSourceFile(logger, dirName, hostname, period, ch)
			} else {
				NewSourceFile(logger, file, hostname, period, ch)
			}

			// 等待 fsnotify 就绪，避免初始事件丢失导致测试不稳定。
			time.Sleep(time.Second)

			// 期望由 SourceFile.resetStoreFromPath() 触发一次更新。
			expectUpdate(t, ch, testCase)

			pod := testCase.pod.(*v1.Pod)
			pod.Spec.Containers[0].Name = "image2"

			testCase.expected.Pods[0].Spec.Containers[0].Name = "image2"
			changeFile := func() {
				// 修改文件内容。
				if symlink {
					file = testCase.writeToFile(linkedDirName, fileName, t)
					return
				}

				file = testCase.writeToFile(dirName, fileName, t)
			}

			go changeFile()
			// 期望由 MODIFY inotify 事件触发一次更新。
			expectUpdate(t, ch, testCase)

			if watchDir {
				go changeFileName(dirName, fileName, fileName+"_ch", t)
				// 文件重命名会产生 MOVED_FROM 事件，此时期望一个空更新。
				expectEmptyUpdate(t, ch)
				// 随后产生 MOVED_TO 事件，期望收到完整的 Pod 更新。
				expectUpdate(t, ch, testCase)
			}
		}()
	}
}

// expectUpdate 等待并校验一次非空的 Pod 更新，超时则判定测试失败。
func expectUpdate(t *testing.T, ch chan sourceUpdate, testCase *testCase) {
	timer := time.After(5 * time.Second)
	for {
		select {
		case update := <-ch:
			if len(update.Pods) == 0 {
				// 过滤掉读取不存在路径产生的空更新。
				continue
			}
			for _, pod := range update.Pods {
				// TODO: remove the conversion when validation is performed on versioned objects.
				internalPod := &api.Pod{}
				if err := k8s_api_v1.Convert_v1_Pod_To_core_Pod(pod, internalPod, nil); err != nil {
					t.Fatalf("%s: Cannot convert pod %#v, %#v", testCase.desc, pod, err)
				}
				if errs := validation.ValidatePodCreate(internalPod, validation.PodValidationOptions{}); len(errs) > 0 {
					t.Fatalf("%s: Invalid pod %#v, %#v", testCase.desc, internalPod, errs)
				}
			}

			if !apiequality.Semantic.DeepEqual(testCase.expected, update) {
				t.Fatalf("%s: Expected: %#v, Got: %#v", testCase.desc, testCase.expected, update)
			}
			return
		case <-timer:
			t.Fatalf("%s: Expected update, timeout instead", testCase.desc)
		}
	}
}

// expectEmptyUpdate 等待并校验一次空的 Pod 更新，超时则判定测试失败。
func expectEmptyUpdate(t *testing.T, ch chan sourceUpdate) {
	timer := time.After(5 * time.Second)
	for {
		select {
		case update := <-ch:
			if len(update.Pods) != 0 {
				t.Fatalf("expected empty update, got %#v", update)
			}
			return
		case <-timer:
			t.Fatalf("expected empty update, timeout instead")
		}
	}
}

// writeFile 将数据写入文件，并检查短写与关闭错误。
func writeFile(filename string, data []byte) error {
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	n, err := f.Write(data)
	if err == nil && n < len(data) {
		err = io.ErrShortWrite
	}
	if err1 := f.Close(); err == nil {
		err = err1
	}
	return err
}

// changeFileName 重命名目录下的文件，用于模拟文件变更事件。
func changeFileName(dir, from, to string, t *testing.T) {
	fromPath := filepath.Join(dir, from)
	toPath := filepath.Join(dir, to)
	if err := os.Rename(fromPath, toPath); err != nil {
		t.Errorf("Fail to change file name: %s", err)
	}
}
