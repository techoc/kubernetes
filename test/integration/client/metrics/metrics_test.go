/*
Copyright 2023 The Kubernetes Authors.

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

package metrics

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/component-base/metrics/legacyregistry"
	apiregistrationv1 "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"
	aggregatorclient "k8s.io/kube-aggregator/pkg/client/clientset_generated/clientset"
	kubeapiservertesting "k8s.io/kubernetes/cmd/kube-apiserver/app/testing"
	"k8s.io/kubernetes/test/integration/framework"

	// 指标是在 cmd/kube-apiserver/apiserver.go 中加载的，
	// 因此我们需要在此处加载它们以供测试使用
	_ "k8s.io/component-base/metrics/prometheus/restclient"
)

// 重要：指标是全局存储的，因此所有测试必须串行运行并重置指标。

// 针对 https://issues.k8s.io/117258 的回归测试
func TestAPIServerTransportMetrics(t *testing.T) {
	featuregatetesting.SetFeatureGatesDuringTest(t, feature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
		"AllAlpha": true,
		"AllBeta":  true,
	})

	// 重置默认 registry 中的指标
	legacyregistry.Reset()

	flags := framework.DefaultTestServerFlags()
	flags = append(flags, "--runtime-config=api/all=true,api/beta=true")
	result := kubeapiservertesting.StartTestServerOrDie(t, nil, flags, framework.SharedEtcd())
	defer result.TearDownFn()

	client := clientset.NewForConfigOrDie(result.ClientConfig)

	// 重要：如果测试发生变化，请反映当前值
	//     client_test.go:1407: metric rest_client_transport_cache_entries 3
	//     client_test.go:1407: metric rest_client_transport_create_calls_total{result="hit"} 20
	//     client_test.go:1407: metric rest_client_transport_create_calls_total{result="miss"} 3
	hits1, misses1, entries1 := checkTransportMetrics(t, client)
	// 启动时的命中率取决于多种因素
	if (hits1*100)/(hits1+misses1) < 85 {
		t.Fatalf("transport cache hit ratio %d lower than 90 percent", (hits1*100)/(hits1+misses1))
	}

	aggregatorClient := aggregatorclient.NewForConfigOrDie(result.ClientConfig)
	aggregatedAPI := &apiregistrationv1.APIService{
		ObjectMeta: metav1.ObjectMeta{Name: "v1alpha1.wardle.example.com"},
		Spec: apiregistrationv1.APIServiceSpec{
			Service: &apiregistrationv1.ServiceReference{
				Namespace: "kube-wardle",
				Name:      "api",
			},
			Group:                "wardle.example.com",
			Version:              "v1alpha1",
			GroupPriorityMinimum: 200,
			VersionPriority:      200,
		},
	}
	_, err := aggregatorClient.ApiregistrationV1().APIServices().Create(context.Background(), aggregatedAPI, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}

	requests := 30
	errors := 0
	for i := range requests {
		apiService, err := aggregatorClient.ApiregistrationV1().APIServices().Get(context.Background(), "v1alpha1.wardle.example.com", metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		// 修改对象
		apiService.Labels = map[string]string{"key": fmt.Sprintf("val%d", i)}
		_, err = aggregatorClient.ApiregistrationV1().APIServices().Update(context.Background(), apiService, metav1.UpdateOptions{})
		if err != nil && !apierrors.IsConflict(err) {
			t.Logf("unexpected error: %v", err)
			errors++
		}
	}

	if (errors*100)/requests > 20 {
		t.Fatalf("high number of errors during the test %d out of %d", errors, requests)
	}

	// 重要：如果测试发生变化，请反映当前值
	//     client_test.go:1407: metric rest_client_transport_cache_entries 4
	//     client_test.go:1407: metric rest_client_transport_create_calls_total{result="hit"} 120
	//     client_test.go:1407: metric rest_client_transport_create_calls_total{result="miss"} 4
	hits2, misses2, entries2 := checkTransportMetrics(t, client)
	if entries2-entries1 > 10 {
		t.Fatalf("possible transport leak, number of new cache entries increased by %d", entries2-entries1)
	}

	// 启动后的命中率应当上升，因为预期不会再创建新的 transport
	if (hits2*100)/(hits2+misses2) < 85 {
		t.Fatalf("transport cache hit ratio %d lower than 95 percent", (hits2*100)/(hits2+misses2))
	}
}

func checkTransportMetrics(t *testing.T, client *clientset.Clientset) (hits int, misses int, entries int) {
	t.Helper()
	body, err := client.RESTClient().Get().AbsPath("/metrics").DoRaw(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	// TODO: 如果有能够解析 prometheus 指标的库，这里可以做得更好
	// "k8s.io/component-base/metrics/testutil" 中现有的库使用全局变量，
	// 但我们希望解析 endpoint 返回的指标，以确保指标被正确暴露
	for _, line := range strings.Split(string(body), "\n") {
		if !strings.HasPrefix(line, "rest_client_transport") {
			continue
		}
		if strings.Contains(line, "uncacheable") {
			t.Fatalf("detected transport that is not cacheable, please check https://issues.k8s.io/112017")
		}

		output := strings.Split(line, " ")
		if len(output) != 2 {
			t.Fatalf("expected metrics to be in the format name value, got %v", output)
		}
		name := output[0]
		value, err := strconv.Atoi(output[1])
		if err != nil {
			t.Fatalf("metric value can not be converted to integer %v", err)
		}
		switch name {
		case "rest_client_transport_cache_entries":
			entries = value
		case `rest_client_transport_create_calls_total{result="hit"}`:
			hits = value
		case `rest_client_transport_create_calls_total{result="miss"}`:
			misses = value
		}
		t.Logf("metric %s", line)
	}

	if misses != entries || misses == 0 {
		t.Errorf("expected as many entries %d in the cache as misses, got %d", entries, misses)
	}

	if hits < misses {
		t.Errorf("expected more hits %d in the cache than misses %d", hits, misses)
	}
	return
}
