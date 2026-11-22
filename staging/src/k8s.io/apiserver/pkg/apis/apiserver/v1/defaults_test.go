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

package v1

import (
	"testing"
	"time"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/google/go-cmp/cmp"
)

// TestKMSProviderTimeoutDefaults 验证 KMSConfiguration 在未显式设置 timeout 时
// 会补上默认值，已设置时保持不变。
func TestKMSProviderTimeoutDefaults(t *testing.T) {
	testCases := []struct {
		desc string
		in   *KMSConfiguration
		want *KMSConfiguration
	}{
		{
			desc: "timeout not supplied",
			in:   &KMSConfiguration{},
			want: &KMSConfiguration{Timeout: defaultTimeout, CacheSize: &defaultCacheSize, APIVersion: defaultAPIVersion},
		},
		{
			desc: "timeout supplied",
			in:   &KMSConfiguration{Timeout: &v1.Duration{Duration: 1 * time.Minute}},
			want: &KMSConfiguration{Timeout: &v1.Duration{Duration: 1 * time.Minute}, CacheSize: &defaultCacheSize, APIVersion: defaultAPIVersion},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.desc, func(t *testing.T) {
			SetDefaults_KMSConfiguration(tt.in)
			if d := cmp.Diff(tt.want, tt.in); d != "" {
				t.Fatalf("KMS Provider mismatch (-want +got):\n%s", d)
			}
		})
	}
}

// TestKMSProviderCacheDefaults 验证 cacheSize 的默认化规则：
// nil 时设置默认值；显式提供（含 0 与负数）时原样保留；
// v2 协议不使用缓存，因此 v2 下不补 cacheSize。
func TestKMSProviderCacheDefaults(t *testing.T) {
	var (
		zero     int32 = 0
		ten      int32 = 10
		negative int32 = -1
	)

	testCases := []struct {
		desc string
		in   *KMSConfiguration
		want *KMSConfiguration
	}{
		{
			desc: "cache size not supplied",
			in:   &KMSConfiguration{},
			want: &KMSConfiguration{Timeout: defaultTimeout, CacheSize: &defaultCacheSize, APIVersion: defaultAPIVersion},
		},
		{
			desc: "cache of zero size supplied",
			in:   &KMSConfiguration{CacheSize: &zero},
			want: &KMSConfiguration{Timeout: defaultTimeout, CacheSize: &zero, APIVersion: defaultAPIVersion},
		},
		{
			desc: "positive cache size supplied",
			in:   &KMSConfiguration{CacheSize: &ten},
			want: &KMSConfiguration{Timeout: defaultTimeout, CacheSize: &ten, APIVersion: defaultAPIVersion},
		},
		{
			desc: "negative cache size supplied",
			in:   &KMSConfiguration{CacheSize: &negative},
			want: &KMSConfiguration{Timeout: defaultTimeout, CacheSize: &negative, APIVersion: defaultAPIVersion},
		},
		{
			desc: "cache size not supplied but API version is v2",
			in:   &KMSConfiguration{APIVersion: "v2"},
			want: &KMSConfiguration{Timeout: defaultTimeout, APIVersion: "v2"},
		},
		{
			desc: "cache size not supplied with API version v1",
			in:   &KMSConfiguration{APIVersion: "v1"},
			want: &KMSConfiguration{Timeout: defaultTimeout, CacheSize: &defaultCacheSize, APIVersion: defaultAPIVersion},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.desc, func(t *testing.T) {
			SetDefaults_KMSConfiguration(tt.in)
			if d := cmp.Diff(tt.want, tt.in); d != "" {
				t.Fatalf("KMS Provider mismatch (-want +got):\n%s", d)
			}
		})
	}
}

// TestKMSProviderAPIVersionDefaults 验证 apiVersion 的默认化：
// 缺省时置为默认版本，显式 v2 时不再补 cacheSize。
func TestKMSProviderAPIVersionDefaults(t *testing.T) {
	testCases := []struct {
		desc string
		in   *KMSConfiguration
		want *KMSConfiguration
	}{
		{
			desc: "apiVersion not supplied",
			in:   &KMSConfiguration{},
			want: &KMSConfiguration{Timeout: defaultTimeout, CacheSize: &defaultCacheSize, APIVersion: defaultAPIVersion},
		},
		{
			desc: "apiVersion supplied",
			in:   &KMSConfiguration{Timeout: &v1.Duration{Duration: 1 * time.Minute}, APIVersion: "v1"},
			want: &KMSConfiguration{Timeout: &v1.Duration{Duration: 1 * time.Minute}, CacheSize: &defaultCacheSize, APIVersion: "v1"},
		},
		{
			desc: "apiVersion v2 supplied, cache size not defaulted",
			in:   &KMSConfiguration{Timeout: &v1.Duration{Duration: 1 * time.Minute}, APIVersion: "v2"},
			want: &KMSConfiguration{Timeout: &v1.Duration{Duration: 1 * time.Minute}, APIVersion: "v2"},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.desc, func(t *testing.T) {
			SetDefaults_KMSConfiguration(tt.in)
			if d := cmp.Diff(tt.want, tt.in); d != "" {
				t.Fatalf("KMS Provider mismatch (-want +got):\n%s", d)
			}
		})
	}
}
