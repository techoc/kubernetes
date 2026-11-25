/*
Copyright 2022 The Kubernetes Authors.

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

package e2enode

import (
	"archive/tar"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/kubernetes/test/e2e/feature"
	"k8s.io/kubernetes/test/e2e/framework"
	e2emetrics "k8s.io/kubernetes/test/e2e/framework/metrics"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	testutils "k8s.io/kubernetes/test/utils"
	imageutils "k8s.io/kubernetes/test/utils/image"
	admissionapi "k8s.io/pod-security-admission/api"

	"github.com/onsi/gomega"
)

const (
	// 代理请求的超时时间。
	proxyTimeout = 2 * time.Minute
)

type checkpointResult struct {
	Items []string `json:"items"`
}

// proxyPostRequest 根据节点名和 rest client 对节点代理端点执行 POST 请求。
func proxyPostRequest(ctx context.Context, c clientset.Interface, node, endpoint string, port int) (restclient.Result, error) {
	// Node 未就绪时代理请求有时会挂起，因此为该调用加上人为的超时限制。#22165
	var result restclient.Result
	finished := make(chan struct{}, 1)
	go func() {
		result = c.CoreV1().RESTClient().Post().
			Resource("nodes").
			SubResource("proxy").
			Name(fmt.Sprintf("%v:%v", node, port)).
			Suffix(endpoint).
			Do(ctx)

		finished <- struct{}{}
	}()
	select {
	case <-finished:
		return result, nil
	case <-ctx.Done():
		return restclient.Result{}, nil
	case <-time.After(proxyTimeout):
		return restclient.Result{}, nil
	}
}

func getCheckpointContainerMetric(ctx context.Context, f *framework.Framework, pod *v1.Pod) (int, error) {
	framework.Logf("Getting 'checkpoint_container' metrics from %q", pod.Spec.NodeName)
	ms, err := e2emetrics.GetKubeletMetrics(
		ctx,
		f.ClientSet,
		pod.Spec.NodeName,
	)
	if err != nil {
		return 0, err
	}

	runtimeOperationsTotal, ok := ms["runtime_operations_total"]
	if !ok {
		// 找不到该指标，说明可能还没有写入过数据。
		return 0, nil
	}

	for _, item := range runtimeOperationsTotal {
		if item.Metric["__name__"] == "kubelet_runtime_operations_total" && item.Metric["operation_type"] == "checkpoint_container" {
			return int(item.Value), nil
		}
	}
	// 找不到指标通常意味着它还没被写入。
	return 0, nil
}

func getCheckpointContainerErrorMetric(ctx context.Context, f *framework.Framework, pod *v1.Pod) (int, error) {
	framework.Logf("Getting 'checkpoint_container' error metrics from %q", pod.Spec.NodeName)
	ms, err := e2emetrics.GetKubeletMetrics(
		ctx,
		f.ClientSet,
		pod.Spec.NodeName,
	)
	if err != nil {
		return 0, err
	}

	runtimeOperationsErrorsTotal, ok := ms["runtime_operations_errors_total"]
	if !ok {
		// 找不到该指标，说明可能还没有写入过数据。
		return 0, nil
	}

	for _, item := range runtimeOperationsErrorsTotal {
		if item.Metric["__name__"] == "kubelet_runtime_operations_errors_total" && item.Metric["operation_type"] == "checkpoint_container" {
			return int(item.Value), nil
		}
	}
	// 找不到指标通常意味着它还没被写入。
	return 0, nil
}

var _ = SIGDescribe("Checkpoint Container", feature.CheckpointContainer, func() {
	f := framework.NewDefaultFramework("checkpoint-container-test")
	f.NamespacePodSecurityLevel = admissionapi.LevelBaseline
	ginkgo.It("will checkpoint a container out of a pod", func(ctx context.Context) {
		ginkgo.By("creating a target pod")
		podClient := e2epod.NewPodClient(f)
		pod := podClient.CreateSync(ctx, &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "checkpoint-container-pod",
				Namespace: f.Namespace.Name,
			},
			Spec: v1.PodSpec{
				Containers: []v1.Container{
					{
						Name:    "test-container-1",
						Image:   imageutils.GetE2EImage(imageutils.BusyBox),
						Command: []string{"/bin/sleep"},
						Args:    []string{"10000"},
					},
				},
			},
		})

		p, err := podClient.Get(
			ctx,
			pod.Name,
			metav1.GetOptions{},
		)

		framework.ExpectNoError(err)
		isReady, err := testutils.PodRunningReady(p)
		framework.ExpectNoError(err)
		if !isReady {
			framework.Failf("pod %q should be ready", p.Name)
		}

		// 此时不应记录任何 checkpoint 操作。
		checkpointContainerMetric, err := getCheckpointContainerMetric(ctx, f, pod)
		framework.ExpectNoError(err)
		gomega.Expect(checkpointContainerMetric).To(gomega.Equal(0))
		// 此时不应记录任何错误。
		checkpointContainerErrorMetric, err := getCheckpointContainerErrorMetric(ctx, f, pod)
		framework.ExpectNoError(err)
		gomega.Expect(checkpointContainerErrorMetric).To(gomega.Equal(0))

		framework.Logf(
			"About to checkpoint container %q on %q",
			pod.Spec.Containers[0].Name,
			pod.Spec.NodeName,
		)
		result, err := proxyPostRequest(
			ctx,
			f.ClientSet,
			pod.Spec.NodeName,
			fmt.Sprintf(
				"checkpoint/%s/%s/%s",
				f.Namespace.Name,
				pod.Name,
				pod.Spec.Containers[0].Name,
			),
			framework.KubeletPort,
		)

		framework.ExpectNoError(err)

		err = result.Error()
		if err != nil {
			statusError, ok := err.(*apierrors.StatusError)
			if !ok {
				framework.Failf("got error %#v, expected StatusError", err)
			}
			// 若测试的 kubelet 将 ContainerCheckpoint 设为 false，应返回 404；
			// 因此 404 也属于可接受的结果。
			if (int(statusError.ErrStatus.Code)) == http.StatusNotFound {
				ginkgo.Skip("Feature 'ContainerCheckpoint' is not enabled and not available")
				return
			}

			// 若容器引擎未实现 Checkpoint CRI API，会返回 500 以及如下消息：
			// '(rpc error: code = Unimplemented desc = unknown method CheckpointContainer'
			// 或
			// '(rpc error: code = Unimplemented desc = method CheckpointContainer not implemented)'
			// ——当容器引擎明确返回其未启用该特性时。
			// 或
			// '(rpc error: code = Unknown desc = checkpoint/restore support not available)'
			// ——当容器引擎明确禁用了 checkpoint/restore 支持时。
			// 或
			// '(rpc error: code = Unknown desc = failed to checkpoint container "...": criu support is disabled by configuration)'
			// ——当 containerd 明确禁用了 CRIU 支持时。
			// 或
			// '(rpc error: code = Unknown desc = CRIU binary not found or too old (<31600). Failed to checkpoint container'
			// ——当 CRIU 二进制文件不存在或版本过旧时。
			// 或
			// '(rpc error: code = Unknown desc = failed to checkpoint container "...": criu binary not found in shim path or system PATH)'
			// '(rpc error: code = Unknown desc = failed to checkpoint container "...": checkpoint/restore requires at least CRIU 31600, current version is ...)'
			// ——新版 containerd 的 CRIU 预检会返回这些消息。
			if (int(statusError.ErrStatus.Code)) == http.StatusInternalServerError {
				if strings.Contains(
					statusError.ErrStatus.Message,
					"(rpc error: code = Unimplemented desc = unknown method CheckpointContainer",
				) {
					ginkgo.Skip("Container engine does not implement 'CheckpointContainer'")
					return
				}
				if strings.Contains(
					statusError.ErrStatus.Message,
					"(rpc error: code = Unimplemented desc = method CheckpointContainer not implemented)",
				) {
					ginkgo.Skip("Container engine does not implement 'CheckpointContainer'")
					return
				}
				if strings.Contains(
					statusError.ErrStatus.Message,
					"(rpc error: code = Unknown desc = checkpoint/restore support not available)",
				) {
					ginkgo.Skip("Container engine does not implement 'CheckpointContainer'")
					return
				}
				if strings.Contains(
					statusError.ErrStatus.Message,
					"criu support is disabled by configuration",
				) {
					ginkgo.Skip("Container engine reports CRIU support is disabled")
					return
				}
				if strings.Contains(
					statusError.ErrStatus.Message,
					"CRIU binary not found or too old (<31600). Failed to checkpoint container",
				) || strings.Contains(
					statusError.ErrStatus.Message,
					"criu binary not found in shim path or system PATH",
				) || strings.Contains(
					statusError.ErrStatus.Message,
					"checkpoint/restore requires at least CRIU",
				) {
					ginkgo.Skip("Container engine reports missing or too old CRIU binary")
					return
				}
			}
			framework.Failf(
				"Unexpected status code (%d) during 'CheckpointContainer': %q",
				statusError.ErrStatus.Code,
				statusError.ErrStatus.Message,
			)
		}

		framework.ExpectNoError(err)

		// checkpoint 已成功生成。接下来验证文件存在，且内容确实是一个 checkpoint。

		raw, err := result.Raw()
		framework.ExpectNoError(err)
		answer := checkpointResult{}
		err = json.Unmarshal(raw, &answer)
		framework.ExpectNoError(err)

		for _, item := range answer.Items {
			// 检查文件是否存在。
			_, err := os.Stat(item)
			framework.ExpectNoError(err)
			// 检查 tar 包内容，至少应包含以下文件：
			//  * spec.dump
			//  * config.dump
			//  * checkpoint/inventory.img
			// checkpoint 归档中若包含这些文件，通常就是一个完整的 checkpoint。
			checkForFiles := map[string]bool{
				"spec.dump":                false,
				"config.dump":              false,
				"checkpoint/inventory.img": false,
			}
			fileReader, err := os.Open(item)
			framework.ExpectNoError(err)
			tr := tar.NewReader(fileReader)
			for {
				hdr, err := tr.Next()
				if err == io.EOF {
					// 归档已读完。
					break
				}
				framework.ExpectNoError(err)
				if _, key := checkForFiles[hdr.Name]; key {
					checkForFiles[hdr.Name] = true
				}
			}
			for fileName := range checkForFiles {
				if !checkForFiles[fileName] {
					framework.Failf("File %q not found in checkpoint archive %q", fileName, item)
				}
			}
			// 清理 checkpoint 归档。
			os.RemoveAll(item)
		}
		// 应当恰好发生了一次 checkpoint 操作。
		checkpointContainerMetric, err = getCheckpointContainerMetric(ctx, f, pod)
		framework.ExpectNoError(err)
		gomega.Expect(checkpointContainerMetric).To(gomega.Equal(1))
		// 不应记录任何错误。
		checkpointContainerErrorMetric, err = getCheckpointContainerErrorMetric(ctx, f, pod)
		framework.ExpectNoError(err)
		gomega.Expect(checkpointContainerErrorMetric).To(gomega.Equal(0))
	})
})
