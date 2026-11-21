/*
Copyright 2015 The Kubernetes Authors.

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

package testing

import (
	"math/rand"
	"os"
	"testing"

	"github.com/google/go-cmp/cmp"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/apitesting/fuzzer"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	api "k8s.io/kubernetes/pkg/apis/core"
)

// BenchmarkPodConversion 测量 Pod 在 internal 与 v1 之间来回转换的开销。
func BenchmarkPodConversion(b *testing.B) {
	apiObjectFuzzer := fuzzer.FuzzerFor(FuzzerFuncs, rand.NewSource(benchmarkSeed), legacyscheme.Codecs)
	items := make([]api.Pod, 4)
	for i := range items {
		apiObjectFuzzer.Fill(&items[i])
		// InitContainers 在转换中会被编码为注解，这里置空以免干扰基准的公平性。
		items[i].Spec.InitContainers = nil
		items[i].Status.InitContainerStatuses = nil
	}

	// 加入一个固定的真实样例，使基准同时覆盖随机数据和稳定结构。
	items = append(items, benchmarkPod)
	width := len(items)

	scheme := legacyscheme.Scheme
	for i := 0; i < b.N; i++ {
		pod := &items[i%width]
		versionedObj, err := scheme.UnsafeConvertToVersion(pod, schema.GroupVersion{Group: "", Version: "v1"})
		if err != nil {
			b.Fatalf("Conversion error: %v", err)
		}
		if _, err = scheme.UnsafeConvertToVersion(versionedObj, schema.GroupVersion{Group: "", Version: runtime.APIVersionInternal}); err != nil {
			b.Fatalf("Conversion error: %v", err)
		}
	}
}

// BenchmarkNodeConversion 测量 Node 的转换开销，并在计时结束后用 DeepDerivative 校验结果。
// 从 JSON 文件解码真实对象，保证样例字段结构贴近实际使用场景。
func BenchmarkNodeConversion(b *testing.B) {
	data, err := os.ReadFile("node_example.json")
	if err != nil {
		b.Fatalf("Unexpected error while reading file: %v", err)
	}
	var node api.Node
	if err := runtime.DecodeInto(legacyscheme.Codecs.LegacyCodec(v1.SchemeGroupVersion), data, &node); err != nil {
		b.Fatalf("Unexpected error decoding node: %v", err)
	}

	scheme := legacyscheme.Scheme
	var result *api.Node
	// 解码和准备不计入耗时，只测量转换循环本身。
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		versionedObj, err := scheme.UnsafeConvertToVersion(&node, schema.GroupVersion{Group: "", Version: "v1"})
		if err != nil {
			b.Fatalf("Conversion error: %v", err)
		}
		obj, err := scheme.UnsafeConvertToVersion(versionedObj, schema.GroupVersion{Group: "", Version: runtime.APIVersionInternal})
		if err != nil {
			b.Fatalf("Conversion error: %v", err)
		}
		result = obj.(*api.Node)
	}
	b.StopTimer()
	// 用 DeepDerivative 而非严格相等：转换可能填充默认值，只要求原始字段被忠实保留。
	if !apiequality.Semantic.DeepDerivative(node, *result) {
		b.Fatalf("Incorrect conversion: %s", cmp.Diff(node, *result))
	}
}

// BenchmarkReplicationControllerConversion 测量 ReplicationController 的转换开销，并校验结果无信息丢失。
func BenchmarkReplicationControllerConversion(b *testing.B) {
	data, err := os.ReadFile("replication_controller_example.json")
	if err != nil {
		b.Fatalf("Unexpected error while reading file: %v", err)
	}
	var replicationController api.ReplicationController
	if err := runtime.DecodeInto(legacyscheme.Codecs.LegacyCodec(v1.SchemeGroupVersion), data, &replicationController); err != nil {
		b.Fatalf("Unexpected error decoding node: %v", err)
	}

	scheme := legacyscheme.Scheme
	var result *api.ReplicationController
	// 只测量转换循环，不计入文件读取与解码的准备时间。
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		versionedObj, err := scheme.UnsafeConvertToVersion(&replicationController, schema.GroupVersion{Group: "", Version: "v1"})
		if err != nil {
			b.Fatalf("Conversion error: %v", err)
		}
		obj, err := scheme.UnsafeConvertToVersion(versionedObj, schema.GroupVersion{Group: "", Version: runtime.APIVersionInternal})
		if err != nil {
			b.Fatalf("Conversion error: %v", err)
		}
		result = obj.(*api.ReplicationController)
	}
	b.StopTimer()
	// DeepDerivative 只要求原始字段被保留，允许转换过程补充默认值。
	if !apiequality.Semantic.DeepDerivative(replicationController, *result) {
		b.Fatalf("Incorrect conversion: expected %v, got %v", replicationController, *result)
	}
}
