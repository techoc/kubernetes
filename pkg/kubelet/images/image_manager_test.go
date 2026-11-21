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

package images

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/util/flowcontrol"
	"k8s.io/component-base/featuregate"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	crierrors "k8s.io/cri-api/pkg/errors"
	"k8s.io/kubernetes/pkg/controller/testutil"
	"k8s.io/kubernetes/pkg/credentialprovider"
	"k8s.io/kubernetes/pkg/features"
	kubeletconfiginternal "k8s.io/kubernetes/pkg/kubelet/apis/config"
	. "k8s.io/kubernetes/pkg/kubelet/container"
	ctest "k8s.io/kubernetes/pkg/kubelet/container/testing"
	"k8s.io/kubernetes/pkg/kubelet/images/pullmanager"
	"k8s.io/kubernetes/test/utils/ktesting"
	testingclock "k8s.io/utils/clock/testing"
	"k8s.io/utils/ptr"
)

// pullerExpects 描述一次 EnsureImageExists 调用期望产生的 runtime 调用、错误与事件。
type pullerExpects struct {
	calls                           []string
	err                             error
	shouldRecordStartedPullingTime  bool
	shouldRecordFinishedPullingTime bool
	events                          []v1.Event
	msg                             string
}

// pullerTestCase 是镜像管理器测试用例：输入（镜像、策略、凭据）与期望行为。
type pullerTestCase struct {
	testName                   string
	containerImage             string
	policy                     v1.PullPolicy
	pullSecrets                []v1.Secret
	allowedCredentials         *mockImagePullManagerConfig                       // controls what the image pull manager considers "allowed"
	serviceAccountName         string                                            // for testing service account coordinates
	registryCredentials        map[string][]credentialprovider.TrackedAuthConfig // image -> registry credentials (obtained from credential providers using SA tokens)
	inspectErr                 error
	pullerErr                  error
	qps                        float32
	burst                      int
	expected                   []pullerExpects
	expectedEnsureImageMetrics string
	enableFeatures             []featuregate.Feature
}

// mockImagePullManagerConfig 配置 mock 拉取管理器认为“允许”的凭据。
type mockImagePullManagerConfig struct {
	allowAll               bool
	allowedSecrets         map[string][]kubeletconfiginternal.ImagePullSecret         // image -> allowed secrets
	allowedServiceAccounts map[string][]kubeletconfiginternal.ImagePullServiceAccount // image -> allowed service accounts
}

// pullerTestCases 汇总默认用例与 KubeletEnsureSecretPulledImages 特性门控用例。
func pullerTestCases() []pullerTestCase {
	return append(
		noFGPullerTestCases(),
		ensureSecretImagesTestCases()...,
	)
}

// noFGPullerTestCases 返回无需开启任何特性门控即可验证默认行为的全部用例。
func noFGPullerTestCases() []pullerTestCase {
	return []pullerTestCase{
		{ // pull missing image
			testName:       "image missing, pull",
			containerImage: "missing_image",
			policy:         v1.PullIfNotPresent,
			inspectErr:     nil,
			pullerErr:      nil,
			qps:            0.0,
			burst:          0,
			expected: []pullerExpects{
				{[]string{"GetImageRef", "PullImage", "GetImageSize"}, nil, true, true,
					[]v1.Event{
						{Reason: "Pulling"},
						{Reason: "Pulled"},
					}, ""},
			},
			expectedEnsureImageMetrics: ensureExistsMetricForLabels("ifnotpresent", "false", "true"),
		},

		{ // image present, don't pull
			testName:       "image present, allow all, don't pull ",
			containerImage: "present_image",
			policy:         v1.PullIfNotPresent,
			inspectErr:     nil,
			pullerErr:      nil,
			qps:            0.0,
			burst:          0,
			expected: []pullerExpects{
				{[]string{"GetImageRef"}, nil, false, false,
					[]v1.Event{
						{Reason: "Pulled"},
					}, ""},
				{[]string{"GetImageRef"}, nil, false, false,
					[]v1.Event{
						{Reason: "Pulled"},
					}, ""},
				{[]string{"GetImageRef"}, nil, false, false,
					[]v1.Event{
						{Reason: "Pulled"},
					}, ""},
			},
			expectedEnsureImageMetrics: ensureExistsMetricForLabels("ifnotpresent", "true", "false"),
		},
		// 镜像已存在，仍发起拉取
		{containerImage: "present_image",
			testName:   "image present, pull",
			policy:     v1.PullAlways,
			inspectErr: nil,
			pullerErr:  nil,
			qps:        0.0,
			burst:      0,
			expected: []pullerExpects{
				{[]string{"PullImage", "GetImageSize"}, nil, true, true,
					[]v1.Event{
						{Reason: "Pulling"},
						{Reason: "Pulled"},
					}, ""},
				{[]string{"PullImage", "GetImageSize"}, nil, true, true,
					[]v1.Event{
						{Reason: "Pulling"},
						{Reason: "Pulled"},
					}, ""},
				{[]string{"PullImage", "GetImageSize"}, nil, true, true,
					[]v1.Event{
						{Reason: "Pulling"},
						{Reason: "Pulled"},
					}, ""},
			},
			expectedEnsureImageMetrics: ensureExistsMetricForLabels("always", "unknown", "true"),
		},
		// 镜像缺失，PullNever 报错
		{containerImage: "missing_image",
			testName:   "image missing, never pull",
			policy:     v1.PullNever,
			inspectErr: nil,
			pullerErr:  nil,
			qps:        0.0,
			burst:      0,
			expected: []pullerExpects{
				{[]string{"GetImageRef"}, ErrImageNeverPull, false, false,
					[]v1.Event{
						{Reason: "ErrImageNeverPull"},
					}, ""},
				{[]string{"GetImageRef"}, ErrImageNeverPull, false, false,
					[]v1.Event{
						{Reason: "ErrImageNeverPull"},
					}, ""},
				{[]string{"GetImageRef"}, ErrImageNeverPull, false, false,
					[]v1.Event{
						{Reason: "ErrImageNeverPull"},
					}, ""},
			},
			expectedEnsureImageMetrics: ensureExistsMetricForLabels("never", "false", "unknown"),
		},
		// 镜像缺失，无法 inspect
		{containerImage: "missing_image",
			testName:   "image missing, pull if not present, fail on image inspect",
			policy:     v1.PullIfNotPresent,
			inspectErr: errors.New("unknown inspectError"),
			pullerErr:  nil,
			qps:        0.0,
			burst:      0,
			expected: []pullerExpects{
				{[]string{"GetImageRef"}, ErrImageInspect, false, false,
					[]v1.Event{
						{Reason: "InspectFailed"},
					}, ""},
				{[]string{"GetImageRef"}, ErrImageInspect, false, false,
					[]v1.Event{
						{Reason: "InspectFailed"},
					}, ""},
				{[]string{"GetImageRef"}, ErrImageInspect, false, false,
					[]v1.Event{
						{Reason: "InspectFailed"},
					}, ""},
			},
			expectedEnsureImageMetrics: ensureExistsMetricForLabels("ifnotpresent", "unknown", "unknown"),
		},
		// 镜像缺失，无法获取
		{containerImage: "typo_image",
			testName:   "image missing, unable to fetch",
			policy:     v1.PullIfNotPresent,
			inspectErr: nil,
			pullerErr:  errors.New("404"),
			qps:        0.0,
			burst:      0,
			expected: []pullerExpects{
				{[]string{"GetImageRef", "PullImage"}, ErrImagePull, true, false,
					[]v1.Event{
						{Reason: "Pulling"},
						{Reason: "Failed"},
					}, ""},
				{[]string{"GetImageRef", "PullImage"}, ErrImagePull, true, false,
					[]v1.Event{
						{Reason: "Pulling"},
						{Reason: "Failed"},
					}, ""},
				{[]string{"GetImageRef"}, ErrImagePullBackOff, false, false,
					[]v1.Event{
						{Reason: "BackOff"},
					}, ""},
				{[]string{"GetImageRef", "PullImage"}, ErrImagePull, true, false,
					[]v1.Event{
						{Reason: "Pulling"},
						{Reason: "Failed"},
					}, ""},
				{[]string{"GetImageRef"}, ErrImagePullBackOff, false, false,
					[]v1.Event{
						{Reason: "BackOff"},
					}, ""},
				{[]string{"GetImageRef"}, ErrImagePullBackOff, false, false,
					[]v1.Event{
						{Reason: "BackOff"},
					}, ""},
			},
			expectedEnsureImageMetrics: ensureExistsMetricForLabels("ifnotpresent", "false", "true"),
		},
		// 镜像已存在，qps 非零，尝试拉取
		{containerImage: "present_image",
			testName:   "image present and qps>0, pull",
			policy:     v1.PullAlways,
			inspectErr: nil,
			pullerErr:  nil,
			qps:        400.0,
			burst:      600,
			expected: []pullerExpects{
				{[]string{"PullImage", "GetImageSize"}, nil, true, true,
					[]v1.Event{
						{Reason: "Pulling"},
						{Reason: "Pulled"},
					}, ""},
				{[]string{"PullImage", "GetImageSize"}, nil, true, true,
					[]v1.Event{
						{Reason: "Pulling"},
						{Reason: "Pulled"},
					}, ""},
				{[]string{"PullImage", "GetImageSize"}, nil, true, true,
					[]v1.Event{
						{Reason: "Pulling"},
						{Reason: "Pulled"},
					}, ""},
			},
			expectedEnsureImageMetrics: ensureExistsMetricForLabels("always", "unknown", "true"),
		},
		// 镜像已存在，qps 非零，qps 超限时尝试拉取
		{containerImage: "present_image",
			testName:   "image present and excessive qps rate, pull",
			policy:     v1.PullAlways,
			inspectErr: nil,
			pullerErr:  nil,
			qps:        2000.0,
			burst:      0,
			expected: []pullerExpects{
				{[]string(nil), ErrImagePull, true, false,
					[]v1.Event{
						{Reason: "Pulling"},
						{Reason: "Failed"},
					}, ""},
				{[]string(nil), ErrImagePull, true, false,
					[]v1.Event{
						{Reason: "Pulling"},
						{Reason: "Failed"},
					}, ""},
				{[]string(nil), ErrImagePullBackOff, false, false,
					[]v1.Event{
						{Reason: "BackOff"},
					}, ""},
			},
			expectedEnsureImageMetrics: ensureExistsMetricForLabels("always", "unknown", "true"),
		},
		// 错误场景：镜像名因引用格式非法而校验失败
		{containerImage: "FAILED_IMAGE",
			testName:   "invalid image name, no pull",
			policy:     v1.PullAlways,
			inspectErr: nil,
			pullerErr:  nil,
			qps:        0.0,
			burst:      0,
			expected: []pullerExpects{
				{[]string(nil), ErrInvalidImageName, false, false,
					[]v1.Event{
						{Reason: "InspectFailed"},
					}, ""},
			},
			expectedEnsureImageMetrics: ensureExistsMetricForLabels("always", "unknown", "unknown"),
		},
		// 错误场景：镜像名包含 http
		{containerImage: "http://url",
			testName:   "invalid image name with http, no pull",
			policy:     v1.PullAlways,
			inspectErr: nil,
			pullerErr:  nil,
			qps:        0.0,
			burst:      0,
			expected: []pullerExpects{
				{[]string(nil), ErrInvalidImageName, false, false,
					[]v1.Event{
						{Reason: "InspectFailed"},
					}, ""},
			},
			expectedEnsureImageMetrics: ensureExistsMetricForLabels("always", "unknown", "unknown"),
		},
		// 错误场景：镜像名包含 sha256
		{containerImage: "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
			testName:   "invalid image name with sha256, no pull",
			policy:     v1.PullAlways,
			inspectErr: nil,
			pullerErr:  nil,
			qps:        0.0,
			burst:      0,
			expected: []pullerExpects{
				{[]string(nil), ErrInvalidImageName, false, false,
					[]v1.Event{
						{Reason: "InspectFailed"},
					}, ""},
			},
			expectedEnsureImageMetrics: ensureExistsMetricForLabels("always", "unknown", "unknown"),
		},
		{containerImage: "typo_image",
			testName:   "image missing, SignatureValidationFailed",
			policy:     v1.PullIfNotPresent,
			inspectErr: nil,
			pullerErr:  crierrors.ErrSignatureValidationFailed,
			qps:        0.0,
			burst:      0,
			expected: []pullerExpects{
				{[]string{"GetImageRef", "PullImage"}, crierrors.ErrSignatureValidationFailed, true, false,
					[]v1.Event{
						{Reason: "Pulling"},
						{Reason: "Failed"},
					}, "image pull failed for typo_image because the signature validation failed"},
				{[]string{"GetImageRef", "PullImage"}, crierrors.ErrSignatureValidationFailed, true, false,
					[]v1.Event{
						{Reason: "Pulling"},
						{Reason: "Failed"},
					}, "image pull failed for typo_image because the signature validation failed"},
				{[]string{"GetImageRef"}, ErrImagePullBackOff, false, false,
					[]v1.Event{
						{Reason: "BackOff"},
					}, "Back-off pulling image \"typo_image\": SignatureValidationFailed: image pull failed for typo_image because the signature validation failed"},
				{[]string{"GetImageRef", "PullImage"}, crierrors.ErrSignatureValidationFailed, true, false,
					[]v1.Event{
						{Reason: "Pulling"},
						{Reason: "Failed"},
					}, "image pull failed for typo_image because the signature validation failed"},
				{[]string{"GetImageRef"}, ErrImagePullBackOff, false, false,
					[]v1.Event{
						{Reason: "BackOff"},
					}, "Back-off pulling image \"typo_image\": SignatureValidationFailed: image pull failed for typo_image because the signature validation failed"},
			},
			expectedEnsureImageMetrics: ensureExistsMetricForLabels("ifnotpresent", "false", "true"),
		},
	}
}

// ensureSecretImagesTestCases 返回 KubeletEnsureSecretPulledImages 特性门控专属用例，
// 同时包含所有非门控用例的副本，但在副本中也要求开启该门控。
func ensureSecretImagesTestCases() []pullerTestCase {
	testCases := []pullerTestCase{
		{
			testName:       "[KubeletEnsureSecretPulledImages] image present, unknown to image pull manager, pull",
			containerImage: "present_image",
			policy:         v1.PullIfNotPresent,
			allowedCredentials: &mockImagePullManagerConfig{
				allowAll: false,
				allowedSecrets: map[string][]kubeletconfiginternal.ImagePullSecret{
					"another_image": {{Namespace: "testns", Name: "testname", UID: "testuid"}},
				},
			},
			pullSecrets:    []v1.Secret{makeDockercfgSecretForRepo(metav1.ObjectMeta{Namespace: "testns", Name: "testname", UID: "testuid"}, "docker.io/library/present_image")},
			inspectErr:     nil,
			pullerErr:      nil,
			qps:            0.0,
			burst:          0,
			enableFeatures: []featuregate.Feature{features.KubeletEnsureSecretPulledImages},
			expected: []pullerExpects{
				{[]string{"GetImageRef", "PullImage", "GetImageSize"}, nil, true, true,
					[]v1.Event{
						{Reason: "Pulling"},
						{Reason: "Pulled"},
					}, ""},
				{[]string{"GetImageRef", "PullImage", "GetImageSize"}, nil, true, true,
					[]v1.Event{
						{Reason: "Pulling"},
						{Reason: "Pulled"},
					}, ""},
				{[]string{"GetImageRef", "PullImage", "GetImageSize"}, nil, true, true,
					[]v1.Event{
						{Reason: "Pulling"},
						{Reason: "Pulled"},
					}, ""},
			},
			expectedEnsureImageMetrics: ensureExistsMetricForLabels("ifnotpresent", "true", "true"),
		},
		{
			testName:       "[KubeletEnsureSecretPulledImages] image present, unknown secret to image pull manager, pull",
			containerImage: "present_image",
			policy:         v1.PullIfNotPresent,
			allowedCredentials: &mockImagePullManagerConfig{
				allowAll: false,
				allowedSecrets: map[string][]kubeletconfiginternal.ImagePullSecret{
					"present_image": {{Namespace: "testns", Name: "testname", UID: "testuid"}},
				},
			},
			pullSecrets:    []v1.Secret{makeDockercfgSecretForRepo(metav1.ObjectMeta{Namespace: "testns", Name: "testname", UID: "someothertestuid"}, "docker.io/library/present_image")},
			inspectErr:     nil,
			pullerErr:      nil,
			qps:            0.0,
			burst:          0,
			enableFeatures: []featuregate.Feature{features.KubeletEnsureSecretPulledImages},
			expected: []pullerExpects{
				{[]string{"GetImageRef", "PullImage", "GetImageSize"}, nil, true, true,
					[]v1.Event{
						{Reason: "Pulling"},
						{Reason: "Pulled"},
					}, ""},
				{[]string{"GetImageRef", "PullImage", "GetImageSize"}, nil, true, true,
					[]v1.Event{
						{Reason: "Pulling"},
						{Reason: "Pulled"},
					}, ""},
				{[]string{"GetImageRef", "PullImage", "GetImageSize"}, nil, true, true,
					[]v1.Event{
						{Reason: "Pulling"},
						{Reason: "Pulled"},
					}, ""},
			},
			expectedEnsureImageMetrics: ensureExistsMetricForLabels("ifnotpresent", "true", "true"),
		},
		{
			testName:       "[KubeletEnsureSecretPulledImages] image present, unknown secret to image pull manager, never pull policy -> fail",
			containerImage: "present_image",
			policy:         v1.PullNever,
			allowedCredentials: &mockImagePullManagerConfig{
				allowAll: false,
				allowedSecrets: map[string][]kubeletconfiginternal.ImagePullSecret{
					"present_image": {{Namespace: "testns", Name: "testname", UID: "testuid"}},
				},
			},
			pullSecrets:    []v1.Secret{makeDockercfgSecretForRepo(metav1.ObjectMeta{Namespace: "testns", Name: "testname", UID: "someothertestuid"}, "docker.io/library/present_image")},
			inspectErr:     nil,
			pullerErr:      nil,
			qps:            0.0,
			burst:          0,
			enableFeatures: []featuregate.Feature{features.KubeletEnsureSecretPulledImages},
			expected: []pullerExpects{
				{[]string{"GetImageRef"}, ErrImageNeverPull, false, false,
					[]v1.Event{
						{Reason: "ErrImageNeverPull"},
					}, ""},
				{[]string{"GetImageRef"}, ErrImageNeverPull, false, false,
					[]v1.Event{
						{Reason: "ErrImageNeverPull"},
					}, ""},
				{[]string{"GetImageRef"}, ErrImageNeverPull, false, false,
					[]v1.Event{
						{Reason: "ErrImageNeverPull"},
					}, ""},
			},
			expectedEnsureImageMetrics: ensureExistsMetricForLabels("never", "true", "true"),
		},
		{
			testName:       "[KubeletEnsureSecretPulledImages] image present, a secret matches one of known to the image pull manager, don't pull",
			containerImage: "present_image",
			policy:         v1.PullIfNotPresent,
			allowedCredentials: &mockImagePullManagerConfig{
				allowAll: false,
				allowedSecrets: map[string][]kubeletconfiginternal.ImagePullSecret{
					"present_image": {{Namespace: "testns", Name: "testname", UID: "testuid"}},
				},
			},
			pullSecrets: []v1.Secret{
				makeDockercfgSecretForRepo(metav1.ObjectMeta{Namespace: "testns", Name: "testname", UID: "someothertestuid"}, "docker.io/library/present_image"),
				makeDockercfgSecretForRepo(metav1.ObjectMeta{Namespace: "testns", Name: "testname", UID: "testuid"}, "docker.io/library/present_image"),
			},
			inspectErr:     nil,
			pullerErr:      nil,
			qps:            0.0,
			burst:          0,
			enableFeatures: []featuregate.Feature{features.KubeletEnsureSecretPulledImages},
			expected: []pullerExpects{
				{[]string{"GetImageRef"}, nil, false, false,
					[]v1.Event{
						{Reason: "Pulled"},
					}, ""},
				{[]string{"GetImageRef"}, nil, false, false,
					[]v1.Event{
						{Reason: "Pulled"},
					}, ""},
				{[]string{"GetImageRef"}, nil, false, false,
					[]v1.Event{
						{Reason: "Pulled"},
					}, ""},
			},
			expectedEnsureImageMetrics: ensureExistsMetricForLabels("ifnotpresent", "true", "false"),
		},
		{
			testName:           "[KubeletEnsureSecretPulledImages] image present, service account credentials available, don't pull",
			containerImage:     "present_image",
			policy:             v1.PullIfNotPresent,
			serviceAccountName: "test-service-account",
			registryCredentials: map[string][]credentialprovider.TrackedAuthConfig{
				"docker.io/library/present_image": {
					{
						AuthConfig: credentialprovider.AuthConfig{
							Username: "sa-user",
							Password: "sa-token",
						},
						Source: &credentialprovider.CredentialSource{
							ServiceAccount: &credentialprovider.ServiceAccountCoordinates{
								Namespace: "test-ns",
								Name:      "test-service-account",
								UID:       "sa-uid-123",
							},
						},
						AuthConfigHash: "sa-hash-123",
					},
				},
			},
			inspectErr:     nil,
			pullerErr:      nil,
			qps:            0.0,
			burst:          0,
			enableFeatures: []featuregate.Feature{features.KubeletEnsureSecretPulledImages},
			expected: []pullerExpects{
				{[]string{"GetImageRef"}, nil, false, false,
					[]v1.Event{
						{Reason: "Pulled"},
					}, ""},
			},
			expectedEnsureImageMetrics: ensureExistsMetricForLabels("ifnotpresent", "true", "false"),
		},
		{
			testName:           "[KubeletEnsureSecretPulledImages] image present, service account allowed by pull manager, don't pull",
			containerImage:     "present_image",
			policy:             v1.PullIfNotPresent,
			serviceAccountName: "test-service-account",
			allowedCredentials: &mockImagePullManagerConfig{
				allowAll: false,
				allowedServiceAccounts: map[string][]kubeletconfiginternal.ImagePullServiceAccount{
					"present_image": {{
						Namespace: "test-ns",
						Name:      "test-service-account",
						UID:       "sa-uid-123",
					}},
				},
			},
			registryCredentials: map[string][]credentialprovider.TrackedAuthConfig{
				"docker.io/library/present_image": {
					{
						AuthConfig: credentialprovider.AuthConfig{
							Username: "sa-user",
							Password: "sa-token",
						},
						Source: &credentialprovider.CredentialSource{
							ServiceAccount: &credentialprovider.ServiceAccountCoordinates{
								Namespace: "test-ns",
								Name:      "test-service-account",
								UID:       "sa-uid-123",
							},
						},
						AuthConfigHash: "sa-hash-123",
					},
				},
			},
			inspectErr:     nil,
			pullerErr:      nil,
			qps:            0.0,
			burst:          0,
			enableFeatures: []featuregate.Feature{features.KubeletEnsureSecretPulledImages},
			expected: []pullerExpects{
				{[]string{"GetImageRef"}, nil, false, false,
					[]v1.Event{
						{Reason: "Pulled"},
					}, ""},
			},
			expectedEnsureImageMetrics: ensureExistsMetricForLabels("ifnotpresent", "true", "false"),
		},
		{
			testName:           "[KubeletEnsureSecretPulledImages] image present, mixed credentials (secrets + service accounts), pull required",
			containerImage:     "present_image",
			policy:             v1.PullIfNotPresent,
			serviceAccountName: "test-service-account",
			pullSecrets:        []v1.Secret{makeDockercfgSecretForRepo(metav1.ObjectMeta{Namespace: "testns", Name: "testname", UID: "secret-uid"}, "docker.io/library/present_image")},
			registryCredentials: map[string][]credentialprovider.TrackedAuthConfig{
				"docker.io/library/present_image": {
					{
						AuthConfig: credentialprovider.AuthConfig{
							Username: "sa-user",
							Password: "sa-token",
						},
						Source: &credentialprovider.CredentialSource{
							ServiceAccount: &credentialprovider.ServiceAccountCoordinates{
								Namespace: "test-ns",
								Name:      "test-service-account",
								UID:       "sa-uid-456",
							},
						},
						AuthConfigHash: "sa-hash-456",
					},
				},
			},
			allowedCredentials: &mockImagePullManagerConfig{
				allowAll: false,
				allowedSecrets: map[string][]kubeletconfiginternal.ImagePullSecret{
					"present_image": {{Namespace: "testns", Name: "testname", UID: "different-secret-uid"}},
				},
			},
			inspectErr:     nil,
			pullerErr:      nil,
			qps:            0.0,
			burst:          0,
			enableFeatures: []featuregate.Feature{features.KubeletEnsureSecretPulledImages},
			expected: []pullerExpects{
				{[]string{"GetImageRef", "PullImage", "GetImageSize"}, nil, true, true,
					[]v1.Event{
						{Reason: "Pulling"},
						{Reason: "Pulled"},
					}, ""},
			},
			expectedEnsureImageMetrics: ensureExistsMetricForLabels("ifnotpresent", "true", "true"),
		},
		{
			testName:       "[KubeletEnsureSecretPulledImages] image present, only node credentials (no source), proceed without tracking",
			containerImage: "present_image",
			policy:         v1.PullIfNotPresent,
			registryCredentials: map[string][]credentialprovider.TrackedAuthConfig{
				"docker.io/library/present_image": {
					{
						AuthConfig: credentialprovider.AuthConfig{
							Username: "node-user",
							Password: "node-pass",
						},
						Source:         nil, // No source means node-accessible
						AuthConfigHash: "node-hash-123",
					},
				},
			},
			inspectErr:     nil,
			pullerErr:      nil,
			qps:            0.0,
			burst:          0,
			enableFeatures: []featuregate.Feature{features.KubeletEnsureSecretPulledImages},
			expected: []pullerExpects{
				{[]string{"GetImageRef"}, nil, false, false,
					[]v1.Event{
						{Reason: "Pulled"},
					}, ""},
			},
			expectedEnsureImageMetrics: ensureExistsMetricForLabels("ifnotpresent", "true", "false"),
		},
	}

	for _, tc := range noFGPullerTestCases() {
		tc.testName = "[KubeletEnsureSecretPulledImages] " + tc.testName
		tc.enableFeatures = append(tc.enableFeatures, features.KubeletEnsureSecretPulledImages)
		testCases = append(testCases, tc)
	}

	return testCases
}

// mockPodPullingTimeRecorder 记录每个 pod 拉取开始/结束的打点状态，用于测试。
type mockPodPullingTimeRecorder struct {
	sync.Mutex
	startedPullingRecorded  map[types.UID]bool
	finishedPullingRecorded map[types.UID]bool
}

// RecordImageStartedPulling 标记指定 pod 已开始拉取镜像（幂等）。
func (m *mockPodPullingTimeRecorder) RecordImageStartedPulling(podUID types.UID) {
	m.Lock()
	defer m.Unlock()

	if !m.startedPullingRecorded[podUID] {
		m.startedPullingRecorded[podUID] = true
	}
}

// RecordImageFinishedPulling 仅在已记录开始拉取后才标记结束，模拟真实语义。
func (m *mockPodPullingTimeRecorder) RecordImageFinishedPulling(podUID types.UID) {
	m.Lock()
	defer m.Unlock()

	if m.startedPullingRecorded[podUID] {
		m.finishedPullingRecorded[podUID] = true
	}
}

// reset 清空所有记录，便于在同一测试中复用。
func (m *mockPodPullingTimeRecorder) reset() {
	m.Lock()
	defer m.Unlock()
	clear(m.startedPullingRecorded)
	clear(m.finishedPullingRecorded)
}

// mockImagePullManager 是可配置的拉取管理器 mock，按允许列表决定是否拉取。
type mockImagePullManager struct {
	pullmanager.NoopImagePullManager

	config *mockImagePullManagerConfig
}

// MustAttemptImagePull 依据配置的允许 secret/service account 列表，判断是否仍需拉取镜像。
func (m *mockImagePullManager) MustAttemptImagePull(ctx context.Context, image, _ string, getPodCredentials pullmanager.GetPodCredentials) (bool, error) {
	if m.config == nil || m.config.allowAll {
		return false, nil
	}

	podSecrets, podServiceAccount, err := getPodCredentials()
	if err != nil {
		return true, err
	}

	// 检查 secret
	if allowedSecrets, ok := m.config.allowedSecrets[image]; ok {
		for _, s := range podSecrets {
			for _, allowed := range allowedSecrets {
				if s.Namespace == allowed.Namespace && s.Name == allowed.Name && s.UID == allowed.UID {
					return false, nil
				}
			}
		}
	}

	// 检查 service account
	if podServiceAccount != nil {
		if allowedServiceAccounts, ok := m.config.allowedServiceAccounts[image]; ok {
			if slices.Contains(allowedServiceAccounts, *podServiceAccount) {
				return false, nil
			}
		}
	}

	return true, nil
}

// mockImagePullManagerWithTracking 跟踪 MustAttemptImagePull 的调用与入参，
// 用于验证 service account 坐标是否正确传递。
type mockImagePullManagerWithTracking struct {
	pullmanager.NoopImagePullManager

	allowAll            bool
	mustAttemptReturn   bool
	mustAttemptCalled   bool
	lastImage           string
	lastImageRef        string
	lastSecrets         []kubeletconfiginternal.ImagePullSecret
	lastServiceAccounts []kubeletconfiginternal.ImagePullServiceAccount
	recordedCredentials *kubeletconfiginternal.ImagePullCredentials
}

// MustAttemptImagePull 记录入参并调用凭据回调，按配置返回是否允许拉取。
func (m *mockImagePullManagerWithTracking) MustAttemptImagePull(ctx context.Context, image, imageRef string, getPodCredentials pullmanager.GetPodCredentials) (bool, error) {
	m.mustAttemptCalled = true
	m.lastImage = image
	m.lastImageRef = imageRef

	podSecrets, podServiceAccount, err := getPodCredentials()
	if err != nil {
		return true, err
	}

	m.lastSecrets = podSecrets
	if podServiceAccount != nil {
		m.lastServiceAccounts = []kubeletconfiginternal.ImagePullServiceAccount{*podServiceAccount}
	}

	if m.allowAll {
		return false, nil
	}
	return m.mustAttemptReturn, nil
}

// RecordImagePulled 记录最近一次上报的凭据，便于断言。
func (m *mockImagePullManagerWithTracking) RecordImagePulled(ctx context.Context, image, imageRef string, credentials *kubeletconfiginternal.ImagePullCredentials) {
	m.recordedCredentials = credentials
}

// mockDockerKeyringWithTrackedCreds 为测试提供带来源跟踪的凭据。
type mockDockerKeyringWithTrackedCreds struct {
	credentialprovider.BasicDockerKeyring
	trackedCreds map[string][]credentialprovider.TrackedAuthConfig
}

// Lookup 优先返回预置的跟踪凭据，否则回退到基础 keyring 查询。
func (m *mockDockerKeyringWithTrackedCreds) Lookup(image string) ([]credentialprovider.TrackedAuthConfig, bool) {
	if creds, ok := m.trackedCreds[image]; ok {
		return creds, true
	}
	// 回退到基础 keyring 查询 —— 它本身已返回 TrackedAuthConfig
	return m.BasicDockerKeyring.Lookup(image)
}

// pullerTestEnv 按用例构造 ImageManager 及其依赖的 fake runtime、时钟与事件记录器。
func pullerTestEnv(
	t ktesting.TB,
	c pullerTestCase,
	serialized bool,
	maxParallelImagePulls *int32,
) (
	puller ImageManager,
	fakeClock *testingclock.FakeClock,
	fakeRuntime *ctest.FakeRuntime,
	container *v1.Container,
	fakePodPullingTimeRecorder *mockPodPullingTimeRecorder,
	fakeRecorder *testutil.FakeRecorder,
) {
	container = &v1.Container{
		Name:            "container_name",
		Image:           c.containerImage,
		ImagePullPolicy: c.policy,
	}

	backOff := flowcontrol.NewBackOff(time.Second, time.Minute)
	fakeClock = testingclock.NewFakeClock(time.Now())
	backOff.Clock = fakeClock

	fakeRuntime = &ctest.FakeRuntime{T: t}
	fakeRecorder = testutil.NewFakeRecorder()

	fakeRuntime.ImageList = []Image{{ID: "present_image:latest"}}
	fakeRuntime.Err = c.pullerErr
	fakeRuntime.InspectErr = c.inspectErr

	fakePodPullingTimeRecorder = &mockPodPullingTimeRecorder{
		startedPullingRecorded:  make(map[types.UID]bool),
		finishedPullingRecorded: make(map[types.UID]bool),
	}

	pullManager := &mockImagePullManager{
		config: c.allowedCredentials,
	}
	if pullManager.config == nil {
		pullManager.config = &mockImagePullManagerConfig{allowAll: true}
	}

	for _, fg := range c.enableFeatures {
		featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, fg, true)
	}

	// 根据是否提供 registry 凭据选择相应的 keyring
	var keyring credentialprovider.DockerKeyring
	if c.registryCredentials != nil {
		// service account 测试使用带跟踪凭据的 mock keyring
		keyring = &mockDockerKeyringWithTrackedCreds{
			trackedCreds: c.registryCredentials,
		}
	} else {
		// 非 service account 测试使用基础 keyring
		keyring = &credentialprovider.BasicDockerKeyring{}
	}

	puller = NewImageManager(fakeRecorder, keyring, fakeRuntime, pullManager, backOff, serialized, maxParallelImagePulls, c.qps, c.burst, fakePodPullingTimeRecorder)
	return
}

// TestParallelPuller 在非串行模式下逐用例校验镜像拉取行为、事件与错误。
func TestParallelPuller(t *testing.T) {
	cases := pullerTestCases()

	useSerializedEnv := false
	for _, c := range cases {
		t.Run(c.testName, func(t *testing.T) {
			ctx := ktesting.Init(t)
			puller, fakeClock, fakeRuntime, container, fakePodPullingTimeRecorder, _ := pullerTestEnv(t, c, useSerializedEnv, nil)

			pod := &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:            "test_pod",
					Namespace:       "test-ns",
					UID:             "bar",
					ResourceVersion: "42",
				},
				Spec: v1.PodSpec{},
			}
			if c.serviceAccountName != "" {
				pod.Spec.ServiceAccountName = c.serviceAccountName
			}

			podSandboxConfig := &runtimeapi.PodSandboxConfig{
				Metadata: &runtimeapi.PodSandboxMetadata{
					Name:      pod.Name,
					Namespace: pod.Namespace,
					Uid:       string(pod.UID),
				},
			}

			for _, expected := range c.expected {
				fakeRuntime.CalledFunctions = nil
				fakeClock.Step(time.Second)

				_, msg, err := puller.EnsureImageExists(ctx, nil, pod, container.Image, c.pullSecrets, podSandboxConfig, "", container.ImagePullPolicy)
				fakeRuntime.AssertCalls(expected.calls)
				assert.Equal(t, expected.err, err)
				assert.Equal(t, expected.shouldRecordStartedPullingTime, fakePodPullingTimeRecorder.startedPullingRecorded[pod.UID])
				assert.Equal(t, expected.shouldRecordFinishedPullingTime, fakePodPullingTimeRecorder.finishedPullingRecorded[pod.UID])
				assert.Contains(t, msg, expected.msg)
				fakePodPullingTimeRecorder.reset()
			}
		})
	}
}

// TestSerializedPuller 在串行模式下校验镜像拉取行为，确保同一时刻只处理一个拉取。
func TestSerializedPuller(t *testing.T) {
	cases := pullerTestCases()

	useSerializedEnv := true
	for _, c := range cases {
		t.Run(c.testName, func(t *testing.T) {
			ctx := ktesting.Init(t)
			puller, fakeClock, fakeRuntime, container, fakePodPullingTimeRecorder, _ := pullerTestEnv(t, c, useSerializedEnv, nil)

			pod := &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:            "test_pod",
					Namespace:       "test-ns",
					UID:             "bar",
					ResourceVersion: "42",
				},
				Spec: v1.PodSpec{},
			}
			if c.serviceAccountName != "" {
				pod.Spec.ServiceAccountName = c.serviceAccountName
			}

			podSandboxConfig := &runtimeapi.PodSandboxConfig{
				Metadata: &runtimeapi.PodSandboxMetadata{
					Name:      pod.Name,
					Namespace: pod.Namespace,
					Uid:       string(pod.UID),
				},
			}

			for _, expected := range c.expected {
				fakeRuntime.CalledFunctions = nil
				fakeClock.Step(time.Second)

				_, msg, err := puller.EnsureImageExists(ctx, nil, pod, container.Image, c.pullSecrets, podSandboxConfig, "", container.ImagePullPolicy)
				fakeRuntime.AssertCalls(expected.calls)
				assert.Equal(t, expected.err, err)
				assert.Equal(t, expected.shouldRecordStartedPullingTime, fakePodPullingTimeRecorder.startedPullingRecorded[pod.UID])
				assert.Equal(t, expected.shouldRecordFinishedPullingTime, fakePodPullingTimeRecorder.finishedPullingRecorded[pod.UID])
				assert.Contains(t, msg, expected.msg)
				fakePodPullingTimeRecorder.reset()
			}
		})
	}
}

// TestApplyDefaultImageTag 校验镜像引用在缺少 tag 时补全默认 tag 的行为。
func TestApplyDefaultImageTag(t *testing.T) {
	for _, testCase := range []struct {
		testName string
		Input    string
		Output   string
	}{
		{testName: "root", Input: "root", Output: "root:latest"},
		{testName: "root:tag", Input: "root:tag", Output: "root:tag"},
		{testName: "root@sha", Input: "root@sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", Output: "root@sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"},
		{testName: "root:latest@sha", Input: "root:latest@sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", Output: "root:latest@sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"},
		{testName: "root:latest", Input: "root:latest", Output: "root:latest"},
	} {
		t.Run(testCase.testName, func(t *testing.T) {
			image, err := applyDefaultImageTag(testCase.Input)
			if err != nil {
				t.Errorf("applyDefaultImageTag(%s) failed: %v", testCase.Input, err)
			} else if image != testCase.Output {
				t.Errorf("Expected image reference: %q, got %q", testCase.Output, image)
			}
		})
	}
}

// TestPullAndListImageWithPodAnnotations 校验镜像列表结果会带上 pod 注解派生的信息。
func TestPullAndListImageWithPodAnnotations(t *testing.T) {
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "test_pod",
			Namespace:       "test-ns",
			UID:             "bar",
			ResourceVersion: "42",
			Annotations: map[string]string{
				"kubernetes.io/runtimehandler": "handler_name",
			},
		}}

	podSandboxConfig := &runtimeapi.PodSandboxConfig{
		Metadata: &runtimeapi.PodSandboxMetadata{
			Name:      pod.Name,
			Namespace: pod.Namespace,
			Uid:       string(pod.UID),
		},
	}

	c := pullerTestCase{ // pull missing image
		testName:       "test pull and list image with pod annotations",
		containerImage: "missing_image",
		policy:         v1.PullIfNotPresent,
		inspectErr:     nil,
		pullerErr:      nil,
		expected: []pullerExpects{
			{[]string{"GetImageRef", "PullImage", "GetImageSize"}, nil, true, true, nil, ""},
		}}

	useSerializedEnv := true
	t.Run(c.testName, func(t *testing.T) {
		ctx := ktesting.Init(t)
		puller, fakeClock, fakeRuntime, container, fakePodPullingTimeRecorder, _ := pullerTestEnv(t, c, useSerializedEnv, nil)
		fakeRuntime.CalledFunctions = nil
		fakeRuntime.ImageList = []Image{}
		fakeClock.Step(time.Second)

		_, _, err := puller.EnsureImageExists(ctx, nil, pod, container.Image, c.pullSecrets, podSandboxConfig, "", container.ImagePullPolicy)
		fakeRuntime.AssertCalls(c.expected[0].calls)
		assert.Equal(t, c.expected[0].err, err, "tick=%d", 0)
		assert.Equal(t, c.expected[0].shouldRecordStartedPullingTime, fakePodPullingTimeRecorder.startedPullingRecorded[pod.UID])
		assert.Equal(t, c.expected[0].shouldRecordFinishedPullingTime, fakePodPullingTimeRecorder.finishedPullingRecorded[pod.UID])

		images, _ := fakeRuntime.ListImages(ctx)
		assert.Len(t, images, 1, "ListImages() count")

		image := images[0]
		assert.Equal(t, "missing_image:latest", image.ID, "Image ID")
		assert.Equal(t, "", image.Spec.RuntimeHandler, "image.Spec.RuntimeHandler not empty", "ImageID", image.ID)

		expectedAnnotations := []Annotation{
			{
				Name:  "kubernetes.io/runtimehandler",
				Value: "handler_name",
			}}
		assert.Equal(t, expectedAnnotations, image.Spec.Annotations, "image spec annotations")
	})
}

// TestPullAndListImageWithRuntimeHandlerInImageCriAPIFeatureGate 校验
// RuntimeClassInImageCriAPI 门控开启时，镜像列表会携带 runtime handler 信息。
func TestPullAndListImageWithRuntimeHandlerInImageCriAPIFeatureGate(t *testing.T) {
	runtimeHandler := "handler_name"
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "test_pod",
			Namespace:       "test-ns",
			UID:             "bar",
			ResourceVersion: "42",
			Annotations: map[string]string{
				"kubernetes.io/runtimehandler": runtimeHandler,
			},
		},
		Spec: v1.PodSpec{
			RuntimeClassName: &runtimeHandler,
		},
	}
	podSandboxConfig := &runtimeapi.PodSandboxConfig{
		Metadata: &runtimeapi.PodSandboxMetadata{
			Name:      pod.Name,
			Namespace: pod.Namespace,
			Uid:       string(pod.UID),
		},
	}
	c := pullerTestCase{ // pull missing image
		testName:       "test pull and list image with pod annotations",
		containerImage: "missing_image",
		policy:         v1.PullIfNotPresent,
		inspectErr:     nil,
		pullerErr:      nil,
		expected: []pullerExpects{
			{[]string{"GetImageRef", "PullImage", "GetImageSize"}, nil, true, true, nil, ""},
		}}

	useSerializedEnv := true
	t.Run(c.testName, func(t *testing.T) {
		featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.RuntimeClassInImageCriAPI, true)
		ctx := ktesting.Init(t)
		puller, fakeClock, fakeRuntime, container, fakePodPullingTimeRecorder, _ := pullerTestEnv(t, c, useSerializedEnv, nil)
		fakeRuntime.CalledFunctions = nil
		fakeRuntime.ImageList = []Image{}
		fakeClock.Step(time.Second)

		_, _, err := puller.EnsureImageExists(ctx, nil, pod, container.Image, c.pullSecrets, podSandboxConfig, runtimeHandler, container.ImagePullPolicy)
		fakeRuntime.AssertCalls(c.expected[0].calls)
		assert.Equal(t, c.expected[0].err, err, "tick=%d", 0)
		assert.Equal(t, c.expected[0].shouldRecordStartedPullingTime, fakePodPullingTimeRecorder.startedPullingRecorded[pod.UID])
		assert.Equal(t, c.expected[0].shouldRecordFinishedPullingTime, fakePodPullingTimeRecorder.finishedPullingRecorded[pod.UID])

		images, _ := fakeRuntime.ListImages(ctx)
		assert.Len(t, images, 1, "ListImages() count")

		image := images[0]
		assert.Equal(t, "missing_image:latest", image.ID, "Image ID")

		// RuntimeClassInImageCriAPI 门控开启时，检查 ListImages() 响应中每个镜像的
		// runtime handler 信息
		assert.Equal(t, runtimeHandler, image.Spec.RuntimeHandler, "runtime handler returned not as expected", "Image ID", image)

		expectedAnnotations := []Annotation{
			{
				Name:  "kubernetes.io/runtimehandler",
				Value: "handler_name",
			}}
		assert.Equal(t, expectedAnnotations, image.Spec.Annotations, "image spec annotations")
	})
}

// TestMaxParallelImagePullsLimit 校验 maxParallelImagePulls 限制并发拉取数量的行为。
func TestMaxParallelImagePullsLimit(t *testing.T) {
	ktesting.Init(t).SyncTest("", func(tCtx ktesting.TContext) {
		testMaxParallelImagePullsLimit(tCtx)
	})
}

// testMaxParallelImagePullsLimit 是 TestMaxParallelImagePullsLimit 的实现主体。
func testMaxParallelImagePullsLimit(tCtx ktesting.TContext) {
	t := tCtx.TB()
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "test_pod",
			Namespace:       "test-ns",
			UID:             "bar",
			ResourceVersion: "42",
		}}
	podSandboxConfig := &runtimeapi.PodSandboxConfig{
		Metadata: &runtimeapi.PodSandboxMetadata{
			Name:      pod.Name,
			Namespace: pod.Namespace,
			Uid:       string(pod.UID),
		},
	}

	testCase := &pullerTestCase{
		containerImage: "present_image",
		testName:       "image present, pull ",
		policy:         v1.PullAlways,
		inspectErr:     nil,
		pullerErr:      nil,
		qps:            0.0,
		burst:          0,
	}

	useSerializedEnv := false
	maxParallelImagePulls := 5
	var wg sync.WaitGroup

	puller, fakeClock, fakeRuntime, container, _, _ := pullerTestEnv(t, *testCase, useSerializedEnv, ptr.To(int32(maxParallelImagePulls)))
	fakeRuntime.BlockImagePulls = true
	fakeRuntime.CalledFunctions = nil
	fakeRuntime.T = t
	fakeClock.Step(time.Second)

	// 前 5 个 EnsureImageExists 应触发 runtime 调用
	for i := 0; i < maxParallelImagePulls; i++ {
		wg.Add(1)
		go func() {
			_, _, err := puller.EnsureImageExists(tCtx, nil, pod, container.Image, testCase.pullSecrets, podSandboxConfig, "", container.ImagePullPolicy)
			assert.NoError(t, err)
			wg.Done()
		}()
	}
	time.Sleep(1 * time.Second)
	fakeRuntime.AssertCallCounts("PullImage", 5)

	// 接下来两个 EnsureImageExists 应被阻塞，因为已达到 maxParallelImagePulls
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			_, _, err := puller.EnsureImageExists(tCtx, nil, pod, container.Image, testCase.pullSecrets, podSandboxConfig, "", container.ImagePullPolicy)
			assert.NoError(t, err)
			wg.Done()
		}()
	}
	time.Sleep(1 * time.Second)
	fakeRuntime.AssertCallCounts("PullImage", 5)

	// 从 runtime 侧放行两个拉取，使两个 EnsureImageExists 得以继续
	fakeRuntime.UnblockImagePulls(2)
	time.Sleep(1 * time.Second)
	fakeRuntime.AssertCallCounts("PullImage", 7)

	// 放行剩余 5 个拉取，所有 EnsureImageExists 都能继续
	fakeRuntime.UnblockImagePulls(5)

	wg.Wait()
	fakeRuntime.AssertCallCounts("PullImage", 7)
}

// TestParallelPodPullingTimeRecorderWithErr 校验拉取失败时打点记录不会误标结束。
func TestParallelPodPullingTimeRecorderWithErr(t *testing.T) {
	ktesting.Init(t).SyncTest("", func(tCtx ktesting.TContext) {
		testParallelPodPullingTimeRecorderWithErr(tCtx)
	})
}

// testParallelPodPullingTimeRecorderWithErr 是 TestParallelPodPullingTimeRecorderWithErr 的实现主体。
func testParallelPodPullingTimeRecorderWithErr(tCtx ktesting.TContext) {
	t := tCtx.TB()
	pod1 := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "test_pod1",
			Namespace:       "test-ns",
			UID:             "bar1",
			ResourceVersion: "42",
		}}
	pod1SandboxConfig := &runtimeapi.PodSandboxConfig{
		Metadata: &runtimeapi.PodSandboxMetadata{
			Name:      pod1.Name,
			Namespace: pod1.Namespace,
			Uid:       string(pod1.UID),
		},
	}

	pod2 := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "test_pod2",
			Namespace:       "test-ns",
			UID:             "bar2",
			ResourceVersion: "42",
		}}
	pod2SandboxConfig := &runtimeapi.PodSandboxConfig{
		Metadata: &runtimeapi.PodSandboxMetadata{
			Name:      pod2.Name,
			Namespace: pod2.Namespace,
			Uid:       string(pod2.UID),
		},
	}

	pods := [2]*v1.Pod{pod1, pod2}
	podSandboxes := [2]*runtimeapi.PodSandboxConfig{pod1SandboxConfig, pod2SandboxConfig}

	testCase := &pullerTestCase{
		containerImage: "missing_image",
		testName:       "missing image, pull if not present",
		policy:         v1.PullIfNotPresent,
		inspectErr:     nil,
		pullerErr:      nil,
		qps:            0.0,
		burst:          0,
	}

	useSerializedEnv := false
	maxParallelImagePulls := 2
	var wg sync.WaitGroup

	puller, fakeClock, fakeRuntime, container, fakePodPullingTimeRecorder, _ := pullerTestEnv(t, *testCase, useSerializedEnv, ptr.To(int32(maxParallelImagePulls)))
	fakeRuntime.BlockImagePulls = true
	fakeRuntime.CalledFunctions = nil
	fakeRuntime.T = t
	fakeClock.Step(time.Second)

	// 首先，每个 pod 的 puller 调用 EnsureImageExists
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			_, _, _ = puller.EnsureImageExists(tCtx, nil, pods[i], container.Image, testCase.pullSecrets, podSandboxes[i], "", container.ImagePullPolicy)
			wg.Done()
		}(i)
	}
	time.Sleep(1 * time.Second)

	// 断言 PullImage 调用次数为 2
	fakeRuntime.AssertCallCounts("PullImage", 2)

	// 两个 pod 都应记录为已开始但未结束
	assert.True(t, fakePodPullingTimeRecorder.startedPullingRecorded[pods[0].UID])
	assert.True(t, fakePodPullingTimeRecorder.startedPullingRecorded[pods[1].UID])
	assert.False(t, fakePodPullingTimeRecorder.finishedPullingRecorded[pods[0].UID])
	assert.False(t, fakePodPullingTimeRecorder.finishedPullingRecorded[pods[1].UID])

	// 放行其中一个 pod 拉取镜像
	fakeRuntime.UnblockImagePulls(1)
	time.Sleep(1 * time.Second)

	// 为第二个 pod 注入拉取错误并放行
	fakeRuntime.SendImagePullError(errors.New("pull image error"))

	wg.Wait()

	// 这次 EnsureImageExists 会直接返回而不拉取
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			_, _, err := puller.EnsureImageExists(tCtx, nil, pods[i], container.Image, testCase.pullSecrets, podSandboxes[i], "", container.ImagePullPolicy)
			assert.NoError(t, err)
			wg.Done()
		}(i)
	}
	wg.Wait()

	// 断言 PullImage 调用次数仍为 2
	fakeRuntime.AssertCallCounts("PullImage", 2)

	// 两个打点记录都应为已结束
	assert.True(t, fakePodPullingTimeRecorder.finishedPullingRecorded[pods[0].UID])
	assert.True(t, fakePodPullingTimeRecorder.finishedPullingRecorded[pods[1].UID])
}

// TestEvalCRIPullErr 校验 evalCRIPullErr 对各类 CRI 拉取错误的分类与消息转换。
func TestEvalCRIPullErr(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		input  error
		assert func(string, error)
	}{
		{
			name:  "fallback error",
			input: errors.New("test"),
			assert: func(msg string, err error) {
				assert.ErrorIs(t, err, ErrImagePull)
				assert.Contains(t, msg, "test")
			},
		},
		{
			name:  "registry is unavailable",
			input: crierrors.ErrRegistryUnavailable,
			assert: func(msg string, err error) {
				assert.ErrorIs(t, err, crierrors.ErrRegistryUnavailable)
				assert.Equal(t, "image pull failed for test because the registry is unavailable", msg)
			},
		},
		{
			name:  "registry is unavailable with additional error message",
			input: fmt.Errorf("%v: foo", crierrors.ErrRegistryUnavailable),
			assert: func(msg string, err error) {
				assert.ErrorIs(t, err, crierrors.ErrRegistryUnavailable)
				assert.Equal(t, "image pull failed for test because the registry is unavailable: foo", msg)
			},
		},
		{
			name:  "signature is invalid",
			input: crierrors.ErrSignatureValidationFailed,
			assert: func(msg string, err error) {
				assert.ErrorIs(t, err, crierrors.ErrSignatureValidationFailed)
				assert.Equal(t, "image pull failed for test because the signature validation failed", msg)
			},
		},
		{
			name:  "signature is invalid with additional error message (wrapped)",
			input: fmt.Errorf("%w: bar", crierrors.ErrSignatureValidationFailed),
			assert: func(msg string, err error) {
				assert.ErrorIs(t, err, crierrors.ErrSignatureValidationFailed)
				assert.Equal(t, "image pull failed for test because the signature validation failed: bar", msg)
			},
		},
	} {
		testInput := tc.input
		testAssert := tc.assert

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			msg, err := evalCRIPullErr("test", testInput)
			testAssert(msg, err)
		})
	}
}

// TestImagePullPrecheck 在串行模式下校验 EnsureImageExists 的预检查逻辑与事件输出。
func TestImagePullPrecheck(t *testing.T) {
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "test_pod",
			Namespace:       "test-ns",
			UID:             "bar",
			ResourceVersion: "42",
		}}
	podSandboxConfig := &runtimeapi.PodSandboxConfig{
		Metadata: &runtimeapi.PodSandboxMetadata{
			Name:      pod.Name,
			Namespace: pod.Namespace,
			Uid:       string(pod.UID),
		},
	}

	cases := pullerTestCases()

	useSerializedEnv := true
	for _, c := range cases {
		t.Run(c.testName, func(t *testing.T) {
			ctx := ktesting.Init(t)
			puller, fakeClock, fakeRuntime, container, _, fakeRecorder := pullerTestEnv(t, c, useSerializedEnv, nil)

			for _, expected := range c.expected {
				fakeRuntime.CalledFunctions = nil
				fakeRecorder.Events = []*v1.Event{}
				fakeClock.Step(time.Second)

				_, _, err := puller.EnsureImageExists(ctx, &v1.ObjectReference{}, pod, container.Image, c.pullSecrets, podSandboxConfig, "", container.ImagePullPolicy)
				fakeRuntime.AssertCalls(expected.calls)
				var recorderEvents []v1.Event
				for _, event := range fakeRecorder.Events {
					recorderEvents = append(recorderEvents, v1.Event{Reason: event.Reason})
				}
				if diff := cmp.Diff(recorderEvents, expected.events); diff != "" {
					t.Errorf("unexpected events diff (-want +got):\n%s", diff)
				}
				if diff := cmp.Diff(expected.err, err, cmpopts.EquateErrors()); diff != "" {
					ctx.Errorf("did not get expected error: %v\ndiff (-want, +got):\n%s", err, diff)
				}
			}
		})
	}
}

// makeDockercfgSecretForRepo 构造一个包含指定仓库凭据的 docker config 类型 Secret。
func makeDockercfgSecretForRepo(sMeta metav1.ObjectMeta, repo string) v1.Secret {
	return v1.Secret{
		ObjectMeta: sMeta,
		Type:       v1.SecretTypeDockerConfigJson,
		Data: map[string][]byte{
			v1.DockerConfigJsonKey: []byte(`{"auths": {"` + repo + `": {"auth": "dXNlcjpwYXNzd29yZA=="}}}`),
		},
	}
}

// TestEnsureImageExistsWithServiceAccountCoordinates 校验 service account 坐标
// 被正确传递给拉取管理器，并影响凭据记录。
func TestEnsureImageExistsWithServiceAccountCoordinates(t *testing.T) {
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "test_pod",
			Namespace:       "test-ns",
			UID:             "pod-uid-123",
			ResourceVersion: "42",
		},
		Spec: v1.PodSpec{
			ServiceAccountName: "test-service-account",
		},
	}

	podSandboxConfig := &runtimeapi.PodSandboxConfig{
		Metadata: &runtimeapi.PodSandboxMetadata{
			Name:      pod.Name,
			Namespace: pod.Namespace,
			Uid:       string(pod.UID),
		},
	}

	cases := []struct {
		name                      string
		containerImage            string
		policy                    v1.PullPolicy
		enableEnsureSecretImages  bool
		expectedServiceAccounts   []kubeletconfiginternal.ImagePullServiceAccount
		shouldCallMustAttemptPull bool
		mustAttemptPullReturn     bool
		expectedImagePull         bool
	}{
		{
			name:                     "service account credentials passed to image pull manager",
			containerImage:           "present_image",
			policy:                   v1.PullIfNotPresent,
			enableEnsureSecretImages: true,
			expectedServiceAccounts: []kubeletconfiginternal.ImagePullServiceAccount{
				{
					Namespace: "test-ns",
					Name:      "test-service-account",
					UID:       "sa-uid-123",
				},
			},
			shouldCallMustAttemptPull: true,
			mustAttemptPullReturn:     false, // Image doesn't need to be pulled
			expectedImagePull:         false,
		},
		{
			name:                     "service account credentials require image pull",
			containerImage:           "present_image",
			policy:                   v1.PullIfNotPresent,
			enableEnsureSecretImages: true,
			expectedServiceAccounts: []kubeletconfiginternal.ImagePullServiceAccount{
				{
					Namespace: "test-ns",
					Name:      "test-service-account",
					UID:       "sa-uid-456",
				},
			},
			shouldCallMustAttemptPull: true,
			mustAttemptPullReturn:     true, // Image needs to be pulled
			expectedImagePull:         true,
		},
		{
			name:                      "feature gate disabled - no service account check",
			containerImage:            "present_image",
			policy:                    v1.PullIfNotPresent,
			enableEnsureSecretImages:  false,
			shouldCallMustAttemptPull: false,
			expectedImagePull:         false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tCtx := ktesting.Init(t)
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.KubeletEnsureSecretPulledImages, tc.enableEnsureSecretImages)
			fakeClock := testingclock.NewFakeClock(time.Now())
			fakeRuntime := &ctest.FakeRuntime{T: t}
			fakeRecorder := testutil.NewFakeRecorder()
			fakePodPullingTimeRecorder := &mockPodPullingTimeRecorder{
				startedPullingRecorded:  make(map[types.UID]bool),
				finishedPullingRecorded: make(map[types.UID]bool),
			}

			fakeRuntime.ImageList = []Image{{ID: "present_image:latest"}}

			mockPullManager := &mockImagePullManagerWithTracking{
				allowAll:          !tc.enableEnsureSecretImages,
				mustAttemptReturn: tc.mustAttemptPullReturn,
			}

			keyring := &mockDockerKeyringWithTrackedCreds{
				trackedCreds: map[string][]credentialprovider.TrackedAuthConfig{},
			}

			if tc.enableEnsureSecretImages && len(tc.expectedServiceAccounts) > 0 {
				repoToPull := "docker.io/library/present_image"
				saCoords := tc.expectedServiceAccounts[0]
				keyring.trackedCreds[repoToPull] = []credentialprovider.TrackedAuthConfig{
					{
						AuthConfig: credentialprovider.AuthConfig{
							Username: "user",
							Password: "token",
						},
						Source: &credentialprovider.CredentialSource{
							ServiceAccount: &credentialprovider.ServiceAccountCoordinates{
								Namespace: saCoords.Namespace,
								Name:      saCoords.Name,
								UID:       saCoords.UID,
							},
						},
						AuthConfigHash: "hash123",
					},
				}
			}

			backOff := flowcontrol.NewBackOff(time.Second, time.Minute)
			backOff.Clock = fakeClock

			puller := NewImageManager(fakeRecorder, keyring, fakeRuntime, mockPullManager, backOff, true, nil, 0.0, 0, fakePodPullingTimeRecorder)

			container := &v1.Container{
				Name:            "container_name",
				Image:           tc.containerImage,
				ImagePullPolicy: tc.policy,
			}

			_, _, err := puller.EnsureImageExists(tCtx, nil, pod, container.Image, []v1.Secret{}, podSandboxConfig, "", container.ImagePullPolicy)
			require.NoError(t, err)

			if tc.shouldCallMustAttemptPull {
				assert.True(t, mockPullManager.mustAttemptCalled, "MustAttemptImagePull should have been called")
				assert.Equal(t, tc.expectedServiceAccounts, mockPullManager.lastServiceAccounts, "Service accounts should match")
			} else {
				assert.False(t, mockPullManager.mustAttemptCalled, "MustAttemptImagePull should not have been called")
			}

			if tc.expectedImagePull {
				fakeRuntime.AssertCalls([]string{"GetImageRef", "PullImage", "GetImageSize"})
			} else {
				fakeRuntime.AssertCalls([]string{"GetImageRef"})
			}
		})
	}
}

// TestEnsureImageExistsWithNodeCredentialsOnly 校验仅使用节点凭据时，
// 不会向拉取管理器传递 secret 或 service account。
func TestEnsureImageExistsWithNodeCredentialsOnly(t *testing.T) {
	tCtx := ktesting.Init(t)
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.KubeletEnsureSecretPulledImages, true)

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "test_pod",
			Namespace:       "test-ns",
			UID:             "pod-uid-123",
			ResourceVersion: "42",
		},
	}

	podSandboxConfig := &runtimeapi.PodSandboxConfig{
		Metadata: &runtimeapi.PodSandboxMetadata{
			Name:      pod.Name,
			Namespace: pod.Namespace,
			Uid:       string(pod.UID),
		},
	}

	fakeClock := testingclock.NewFakeClock(time.Now())
	fakeRuntime := &ctest.FakeRuntime{T: t}
	fakeRecorder := testutil.NewFakeRecorder()
	fakePodPullingTimeRecorder := &mockPodPullingTimeRecorder{
		startedPullingRecorded:  make(map[types.UID]bool),
		finishedPullingRecorded: make(map[types.UID]bool),
	}

	fakeRuntime.ImageList = []Image{{ID: "present_image:latest"}}

	mockPullManager := &mockImagePullManagerWithTracking{
		allowAll:          false,
		mustAttemptReturn: false, // Don't force pull
	}

	repoToPull := "docker.io/library/present_image"
	keyring := &mockDockerKeyringWithTrackedCreds{
		trackedCreds: map[string][]credentialprovider.TrackedAuthConfig{
			repoToPull: {
				{
					AuthConfig: credentialprovider.AuthConfig{
						Username: "nodeuser",
						Password: "nodepass",
					},
					Source:         nil, // No source means node-accessible
					AuthConfigHash: "node-hash-123",
				},
			},
		},
	}

	backOff := flowcontrol.NewBackOff(time.Second, time.Minute)
	backOff.Clock = fakeClock

	puller := NewImageManager(fakeRecorder, keyring, fakeRuntime, mockPullManager, backOff, true, nil, 0.0, 0, fakePodPullingTimeRecorder)

	container := &v1.Container{
		Name:            "container_name",
		Image:           "present_image",
		ImagePullPolicy: v1.PullIfNotPresent,
	}

	_, _, err := puller.EnsureImageExists(tCtx, nil, pod, container.Image, []v1.Secret{}, podSandboxConfig, "", container.ImagePullPolicy)
	require.NoError(t, err)

	// 验证调用 MustAttemptImagePull 时 secret 与 service account 为空，
	// 因为节点凭据无需跟踪
	assert.True(t, mockPullManager.mustAttemptCalled, "MustAttemptImagePull should have been called")
	assert.Empty(t, mockPullManager.lastSecrets, "No secrets should be passed for node credentials")
	assert.Empty(t, mockPullManager.lastServiceAccounts, "No service accounts should be passed for node credentials")

	// 镜像已存在且可访问，因此不应发起拉取
	fakeRuntime.AssertCalls([]string{"GetImageRef"})
}

// ensureExistsMetricForLabels 生成 kubelet_image_manager_ensure_image_requests_total
// 指标在给定标签组合下的期望文本。
func ensureExistsMetricForLabels(pullPolicy, imagePresentLocally, pullRequired string) string {
	const desc = `
# HELP kubelet_image_manager_ensure_image_requests_total [ALPHA] Number of ensure-image requests processed by the kubelet.
# TYPE kubelet_image_manager_ensure_image_requests_total counter
`
	return desc + fmt.Sprintf(
		"kubelet_image_manager_ensure_image_requests_total{present_locally=\"%s\", pull_policy=\"%s\", pull_required=\"%s\"} 1\n",
		imagePresentLocally,
		pullPolicy,
		pullRequired,
	)
}
