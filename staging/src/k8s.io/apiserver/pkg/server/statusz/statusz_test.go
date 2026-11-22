/*
Copyright 2024 The Kubernetes Authors.

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

package statusz

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"k8s.io/apimachinery/pkg/runtime"
	cbor "k8s.io/apimachinery/pkg/runtime/serializer/cbor/direct"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/version"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/apiserver/pkg/endpoints/metrics"
	"k8s.io/apiserver/pkg/features"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/component-base/metrics/testutil"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	v1alpha1 "k8s.io/apiserver/pkg/server/statusz/api/v1alpha1"
	v1beta1 "k8s.io/apiserver/pkg/server/statusz/api/v1beta1"
)

// wantTmpl 是 text/plain 响应的期望模板；statusz 明确声明不可机器解析，
// 因此这里按字符串包含关系断言，而不是结构化比较。
const wantTmpl = `
%s statusz
Warning: This endpoint is not meant to be machine parseable, has no formatting compatibility guarantees and is for debugging purposes only.

Started: %v
Up: %s
Go version: %s
Binary version: %v
Emulation version: %v
Paths: /livez /readyz
`

const wantTmplWithoutEmulation = `
%s statusz
Warning: This endpoint is not meant to be machine parseable, has no formatting compatibility guarantees and is for debugging purposes only.

Started: %v
Up: %s
Go version: %s
Binary version: %v

Paths: /livez /readyz
`

// TestHandleStatusz 覆盖内容协商的关键分支：text/plain 默认、v1beta1
// 结构化输出（JSON/YAML/CBOR）、v1alpha1 的弃用警告，以及各种不可接受的
// Accept 头。
func TestHandleStatusz(t *testing.T) {
	// Enable CBOR feature gate for CBOR test case
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.CBORServingAndStorage, true)

	delimiters = []string{":"}
	fakeStartTime := time.Now()
	fakeUptime := uptime(fakeStartTime)
	fakeGoVersion := "1.21"
	fakeBvStr := "1.31"
	fakeEvStr := "1.30"
	fakeBinaryVersion := parseVersion(t, fakeBvStr)
	fakeEmulationVersion := parseVersion(t, fakeEvStr)
	fakeListedPaths := []string{"/livez/poststarthook/peer-discovery-cache-sync", "/livez/post", "/readyz/informer-sync", "/readyz/log", "/readyz/ping"}
	tests := []struct {
		name               string
		acceptHeader       string
		componentName      string
		registry           fakeRegistry
		wantStatusCode     int
		wantBody           string
		wantStructuredBody interface{}
		wantWarning        bool
	}{
		{
			name:          "valid request for text/plain",
			acceptHeader:  "text/plain",
			componentName: "test-server",
			registry: fakeRegistry{
				startTime:    fakeStartTime,
				goVer:        fakeGoVersion,
				binaryVer:    fakeBinaryVersion,
				emulationVer: fakeEmulationVersion,
				listedPaths:  fakeListedPaths,
			},
			wantStatusCode: http.StatusOK,
			wantBody: fmt.Sprintf(
				wantTmpl,
				"test-server",
				fakeStartTime.Format(time.UnixDate),
				fakeUptime,
				fakeGoVersion,
				fakeBinaryVersion,
				fakeEmulationVersion,
			),
		},
		{
			// 显式请求 v1beta1 Statusz，必须返回带 TypeMeta/ObjectMeta 的结构化对象。
			name:          "valid request for application/json",
			acceptHeader:  "application/json;v=v1beta1;g=config.k8s.io;as=Statusz",
			componentName: "test-server",
			registry: fakeRegistry{
				startTime:    fakeStartTime,
				goVer:        fakeGoVersion,
				binaryVer:    fakeBinaryVersion,
				emulationVer: fakeEmulationVersion,
				listedPaths:  fakeListedPaths,
				deprecated:   map[string]bool{},
			},
			wantStatusCode: http.StatusOK,
			wantStructuredBody: &v1beta1.Statusz{
				TypeMeta: metav1.TypeMeta{
					Kind:       "Statusz",
					APIVersion: "config.k8s.io/v1beta1",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-server",
				},
				StartTime:        metav1.Time{Time: fakeStartTime},
				UptimeSeconds:    int64(time.Since(fakeStartTime).Seconds()),
				GoVersion:        fakeGoVersion,
				BinaryVersion:    fakeBvStr,
				EmulationVersion: fakeEvStr,
				Paths:            []string{"/livez", "/readyz"},
			},
		},
		{
			name:          "no accept header",
			acceptHeader:  "",
			componentName: "test-server",
			registry: fakeRegistry{
				startTime:    fakeStartTime,
				goVer:        fakeGoVersion,
				binaryVer:    fakeBinaryVersion,
				emulationVer: fakeEmulationVersion,
				listedPaths:  fakeListedPaths,
			},
			wantStatusCode: http.StatusOK,
			wantBody: fmt.Sprintf(
				wantTmpl,
				"test-server",
				fakeStartTime.Format(time.UnixDate),
				fakeUptime,
				fakeGoVersion,
				fakeBinaryVersion,
				fakeEmulationVersion,
			),
		},
		{
			// 不支持的媒体类型应返回 406，而不是静默降级为 text。
			name:           "invalid accept header",
			acceptHeader:   "application/xml",
			componentName:  "test-server",
			wantStatusCode: http.StatusNotAcceptable,
		},
		{
			// 模拟版本为空时文本输出应省略该行，而不是打印 "<nil>"。
			name:          "missing emulation version",
			acceptHeader:  "text/plain",
			componentName: "test-server",
			registry: fakeRegistry{
				startTime:    fakeStartTime,
				goVer:        fakeGoVersion,
				binaryVer:    fakeBinaryVersion,
				emulationVer: nil,
				listedPaths:  fakeListedPaths,
			},
			wantStatusCode: http.StatusOK,
			wantBody: fmt.Sprintf(
				wantTmplWithoutEmulation,
				"test-server",
				fakeStartTime.Format(time.UnixDate),
				fakeUptime,
				fakeGoVersion,
				fakeBinaryVersion,
			),
		},
		{
			// application/json 缺少 v/g/as 参数时无法确定版本，必须 406。
			name:           "application/json without params",
			acceptHeader:   "application/json",
			componentName:  "test-server",
			wantStatusCode: http.StatusNotAcceptable,
		},
		{
			name:           "application/json with missing as",
			acceptHeader:   "application/json;v=v1beta1;g=config.k8s.io",
			componentName:  "test-server",
			wantStatusCode: http.StatusNotAcceptable,
		},
		{
			// 通配 */* 走默认的 text/plain 路径。
			name:          "wildcard accept header",
			acceptHeader:  "*/*",
			componentName: "test-server",
			registry: fakeRegistry{
				startTime:    fakeStartTime,
				goVer:        fakeGoVersion,
				binaryVer:    fakeBinaryVersion,
				emulationVer: fakeEmulationVersion,
				listedPaths:  fakeListedPaths,
			},
			wantStatusCode: http.StatusOK,
			wantBody: fmt.Sprintf(
				wantTmpl,
				"test-server",
				fakeStartTime.Format(time.UnixDate),
				fakeUptime,
				fakeGoVersion,
				fakeBinaryVersion,
				fakeEmulationVersion,
			),
		},
		{
			// 非法的结构化媒体类型不应导致失败；回退到 */* 提供的 text 路径。
			name:          "bad json header fall back wildcard",
			acceptHeader:  "application/json;v=foo;g=config.k8s.io;as=Statusz,*/*",
			componentName: "test-server",
			registry: fakeRegistry{
				startTime:    fakeStartTime,
				goVer:        fakeGoVersion,
				binaryVer:    fakeBinaryVersion,
				emulationVer: fakeEmulationVersion,
				listedPaths:  fakeListedPaths,
			},
			wantStatusCode: http.StatusOK,
			wantBody: fmt.Sprintf(
				wantTmpl,
				"test-server",
				fakeStartTime.Format(time.UnixDate),
				fakeUptime,
				fakeGoVersion,
				fakeBinaryVersion,
				fakeEmulationVersion,
			),
		},
		{
			// v1alpha1 已被标记弃用，响应需带 Warning 头提示调用方迁移。
			name:          "deprecated v1alpha1 request",
			acceptHeader:  "application/json;v=v1alpha1;g=config.k8s.io;as=Statusz",
			componentName: "test-server",
			registry: fakeRegistry{
				startTime:    fakeStartTime,
				goVer:        fakeGoVersion,
				binaryVer:    fakeBinaryVersion,
				emulationVer: fakeEmulationVersion,
				listedPaths:  fakeListedPaths,
				deprecated:   map[string]bool{"v1alpha1": true},
			},
			wantStatusCode: http.StatusOK,
			wantStructuredBody: &v1alpha1.Statusz{
				TypeMeta: metav1.TypeMeta{
					Kind:       "Statusz",
					APIVersion: "config.k8s.io/v1alpha1",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-server",
				},
				StartTime:        metav1.Time{Time: fakeStartTime},
				UptimeSeconds:    int64(time.Since(fakeStartTime).Seconds()),
				GoVersion:        fakeGoVersion,
				BinaryVersion:    fakeBvStr,
				EmulationVersion: fakeEvStr,
				Paths:            []string{"/livez", "/readyz"},
			},
			wantWarning: true,
		},
		{
			name:          "valid request for application/yaml",
			acceptHeader:  "application/yaml;v=v1beta1;g=config.k8s.io;as=Statusz",
			componentName: "test-server",
			registry: fakeRegistry{
				startTime:    fakeStartTime,
				goVer:        fakeGoVersion,
				binaryVer:    fakeBinaryVersion,
				emulationVer: fakeEmulationVersion,
				listedPaths:  fakeListedPaths,
				deprecated:   map[string]bool{},
			},
			wantStatusCode: http.StatusOK,
			wantStructuredBody: &v1beta1.Statusz{
				TypeMeta: metav1.TypeMeta{
					Kind:       "Statusz",
					APIVersion: "config.k8s.io/v1beta1",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-server",
				},
				StartTime:        metav1.Time{Time: fakeStartTime},
				UptimeSeconds:    int64(time.Since(fakeStartTime).Seconds()),
				GoVersion:        fakeGoVersion,
				BinaryVersion:    fakeBvStr,
				EmulationVersion: fakeEvStr,
				Paths:            []string{"/livez", "/readyz"},
			},
		},
		{
			// CBOR 需要在测试中显式打开 CBORServingAndStorage 特性门控。
			name:          "valid request for application/cbor",
			acceptHeader:  "application/cbor;v=v1beta1;g=config.k8s.io;as=Statusz",
			componentName: "test-server",
			registry: fakeRegistry{
				startTime:    fakeStartTime,
				goVer:        fakeGoVersion,
				binaryVer:    fakeBinaryVersion,
				emulationVer: fakeEmulationVersion,
				listedPaths:  fakeListedPaths,
				deprecated:   map[string]bool{},
			},
			wantStatusCode: http.StatusOK,
			wantStructuredBody: &v1beta1.Statusz{
				TypeMeta: metav1.TypeMeta{
					Kind:       "Statusz",
					APIVersion: "config.k8s.io/v1beta1",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-server",
				},
				StartTime:        metav1.Time{Time: fakeStartTime},
				UptimeSeconds:    int64(time.Since(fakeStartTime).Seconds()),
				GoVersion:        fakeGoVersion,
				BinaryVersion:    fakeBvStr,
				EmulationVersion: fakeEvStr,
				Paths:            []string{"/livez", "/readyz"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mux := http.NewServeMux()
			Install(mux, tt.componentName, tt.registry)

			path := "/statusz"
			req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("http://example.com%s", path), nil)
			if err != nil {
				t.Fatalf("unexpected error while creating request: %v", err)
			}
			if tt.acceptHeader != "" {
				req.Header.Set("Accept", tt.acceptHeader)
			}

			w := httptest.NewRecorder()
			mux.ServeHTTP(w, req)

			if w.Code != tt.wantStatusCode {
				t.Fatalf("want status code: %v, got: %v", tt.wantStatusCode, w.Code)
			}

			if tt.wantStatusCode == http.StatusOK {
				if tt.wantStructuredBody != nil {
					var got interface{}
					switch tt.wantStructuredBody.(type) {
					case *v1alpha1.Statusz:
						got = &v1alpha1.Statusz{}
					case *v1beta1.Statusz:
						got = &v1beta1.Statusz{}
					default:
						t.Fatalf("unexpected type for wantStructuredBody: %T", tt.wantStructuredBody)
					}
					unmarshalResponse(t, w.Header().Get("Content-Type"), w.Body.Bytes(), got)
					if diff := cmp.Diff(tt.wantStructuredBody, got, timeEqual()); diff != "" {
						t.Errorf("Unexpected diff on response (-want,+got):\n%s", diff)
					}
					if tt.wantWarning {
						if !strings.Contains(w.Header().Get("Warning"), "deprecated") {
							t.Errorf("expected deprecation warning in header, but got: %s", w.Header().Get("Warning"))
						}
					}
				} else {
					if !strings.Contains(string(w.Body.String()), tt.wantBody) {
						t.Errorf("Unexpected response body:\n- want to contain: %s\n- got:  %s", tt.wantBody, string(w.Body.String()))
					}
				}
			}
		})
	}
}

// unmarshalResponse 依据响应 Content-Type 选择对应的解码器，
// 使同一组结构化断言可以覆盖 JSON/YAML/CBOR 三种编码。
func unmarshalResponse(t *testing.T, contentType string, body []byte, got interface{}) {
	t.Helper()
	switch {
	case strings.Contains(contentType, "application/json"):
		if err := json.Unmarshal(body, got); err != nil {
			t.Fatalf("unexpected error while unmarshalling JSON response: %v", err)
		}
	case strings.Contains(contentType, "application/cbor"):
		if err := cbor.Unmarshal(body, got); err != nil {
			t.Fatalf("unexpected error while unmarshalling CBOR response: %v", err)
		}
	case strings.Contains(contentType, "application/yaml"):
		if err := yaml.Unmarshal(body, got); err != nil {
			t.Fatalf("unexpected error while unmarshalling YAML response: %v", err)
		}
	default:
		t.Fatalf("unexpected content type: %s", contentType)
	}
}

// parseVersion 解析 "major.minor" 形式的版本，失败即终止测试。
func parseVersion(t *testing.T, v string) *version.Version {
	parsed, err := version.ParseMajorMinor(v)
	if err != nil {
		t.Fatalf("error parsing binary version: %s", v)
	}

	return parsed
}

// fakeRegistry 实现 statusz 所需的只读注册表接口，返回固定的测试数据。
// deprecated 字段模拟“哪些 API 版本已被标记弃用”。
type fakeRegistry struct {
	startTime    time.Time
	goVer        string
	binaryVer    *version.Version
	emulationVer *version.Version
	listedPaths  []string
	deprecated   map[string]bool
}

func (f fakeRegistry) deprecatedVersions() map[string]bool {
	return f.deprecated
}

func (f fakeRegistry) processStartTime() time.Time {
	return f.startTime
}

func (f fakeRegistry) goVersion() string {
	return f.goVer
}

func (f fakeRegistry) binaryVersion() *version.Version {
	return f.binaryVer
}

func (f fakeRegistry) emulationVersion() *version.Version {
	return f.emulationVer
}

func (f fakeRegistry) paths() []string {
	return f.listedPaths
}

// timeEqual 把时间比较截断到秒：statusz 只输出秒级精度，
// 但构造期望值时使用的是纳秒级的 time.Now()。
func timeEqual() cmp.Option {
	return cmp.Comparer(func(expectedTime, actualTime metav1.Time) bool {
		return expectedTime.Truncate(time.Second).Equal(actualTime.Truncate(time.Second))
	})
}

// TestNewStatuszCodecFactory ensures all media types in the codec factory
// are explicitly handled. If this test fails, a new media type was added
// to the codec factory and needs to be explicitly added to the supported
// or unsupported list in newStatuszCodecFactory.
func TestNewStatuszCodecFactory(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.CBORServingAndStorage, true)
	scheme := runtime.NewScheme()
	utilruntime.Must(v1alpha1.AddToScheme(scheme))
	utilruntime.Must(v1beta1.AddToScheme(scheme))

	_, err := newStatuszCodecFactory(scheme, "", nil)
	if err != nil {
		t.Fatalf("unknown media type(s) detected - update newStatuszCodecFactory to explicitly handle them: %v", err)
	}
}

// TestMetrics 验证 statusz 请求会计入 apiserver_request_total：
// text/plain 时的 group/version 为空，结构化请求则反映协商出的版本。
func TestMetrics(t *testing.T) {
	fakeStartTime := time.Now()
	fakeBinaryVersion := parseVersion(t, "1.31")
	fakeEmulationVersion := parseVersion(t, "1.30")
	fakeListedPaths := []string{"/livez/ping", "/readyz/ping"}

	reg := fakeRegistry{
		startTime:    fakeStartTime,
		goVer:        "1.21",
		binaryVer:    fakeBinaryVersion,
		emulationVer: fakeEmulationVersion,
		listedPaths:  fakeListedPaths,
	}

	mux := http.NewServeMux()
	Install(mux, "test-server", reg)
	metrics.Register()
	metrics.Reset()

	// text/plain request: group and version should be empty
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("http://example.com%s", DefaultStatuszPath), nil)
	if err != nil {
		t.Errorf("%v", err)
	}
	mux.ServeHTTP(httptest.NewRecorder(), req)

	// structured request: group and version should reflect the negotiated version
	req, err = http.NewRequest(http.MethodGet, fmt.Sprintf("http://example.com%s", DefaultStatuszPath), nil)
	if err != nil {
		t.Errorf("%v", err)
	}
	req.Header.Set("Accept", "application/json;v=v1alpha1;g=config.k8s.io;as=Statusz")
	mux.ServeHTTP(httptest.NewRecorder(), req)

	expected := strings.NewReader(`
        # HELP apiserver_request_total [STABLE] Counter of apiserver requests broken out for each verb, dry run value, group, version, resource, scope, component, and HTTP response code.
        # TYPE apiserver_request_total counter
        apiserver_request_total{code="200",component="test-server",dry_run="",group="",resource="statusz",scope="",subresource="",verb="GET",version=""} 1
        apiserver_request_total{code="200",component="test-server",dry_run="",group="config.k8s.io",resource="statusz",scope="",subresource="",verb="GET",version="v1alpha1"} 1
`)
	if err := testutil.GatherAndCompare(legacyregistry.DefaultGatherer, expected, "apiserver_request_total"); err != nil {
		t.Error(err)
	}
}
