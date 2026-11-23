/*
Copyright 2018 The Kubernetes Authors.

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

package mount

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
)

// bind 挂载拆两步：先只做 bind，再补 -o 选项；这里验证选项被分到正确的阶段。
func TestMakeBindOpts(t *testing.T) {
	tests := []struct {
		mountOption         []string
		isBind              bool
		expectedBindOpts    []string
		expectedRemountOpts []string
	}{
		{
			[]string{"vers=2", "ro", "_netdev"},
			false,
			[]string{},
			[]string{},
		},
		{
			[]string{"bind", "vers=2", "ro", "_netdev"},
			true,
			[]string{"bind", "_netdev"},
			[]string{"bind", "remount", "vers=2", "ro", "_netdev"},
		},
	}
	for _, test := range tests {
		bind, bindOpts, bindRemountOpts := MakeBindOpts(test.mountOption)
		if bind != test.isBind {
			t.Errorf("Expected bind to be %v but got %v", test.isBind, bind)
		}
		if test.isBind {
			if !reflect.DeepEqual(test.expectedBindOpts, bindOpts) {
				t.Errorf("Expected bind mount options to be %+v got %+v", test.expectedBindOpts, bindOpts)
			}
			if !reflect.DeepEqual(test.expectedRemountOpts, bindRemountOpts) {
				t.Errorf("Expected remount options to be %+v got %+v", test.expectedRemountOpts, bindRemountOpts)
			}
		}

	}
}

// 敏感选项里也可能混进 bind/remount，判定阶段时两类输入都得看，否则会漏判。
// 敏感值只能留在敏感重挂载那组，普通重挂载选项里不该出现密码。
func TestMakeBindOptsSensitive(t *testing.T) {
	// 每行是一组输入与期望的四元结果，重点看 bind/remount 被剔除后的去向。
	tests := []struct {
		mountOptions                 []string
		sensitiveMountOptions        []string
		isBind                       bool
		expectedBindOpts             []string
		expectedRemountOpts          []string
		expectedSensitiveRemountOpts []string
	}{
		{
			mountOptions:                 []string{"vers=2", "ro", "_netdev"},
			sensitiveMountOptions:        []string{"user=foo", "pass=bar"},
			isBind:                       false,
			expectedBindOpts:             []string{},
			expectedRemountOpts:          []string{},
			expectedSensitiveRemountOpts: []string{"user=foo", "pass=bar"},
		},
		// bind 藏在敏感选项里，仍应识别为 bind mount。
		{
			mountOptions:                 []string{"vers=2", "ro", "_netdev"},
			sensitiveMountOptions:        []string{"user=foo", "pass=bar", "bind"},
			isBind:                       true,
			expectedBindOpts:             []string{"bind", "_netdev"},
			expectedRemountOpts:          []string{"bind", "remount", "vers=2", "ro", "_netdev"},
			expectedSensitiveRemountOpts: []string{"user=foo", "pass=bar"},
		},
		{
			mountOptions:                 []string{"vers=2", "remount", "ro", "_netdev"},
			sensitiveMountOptions:        []string{"user=foo", "pass=bar"},
			isBind:                       false,
			expectedBindOpts:             []string{},
			expectedRemountOpts:          []string{},
			expectedSensitiveRemountOpts: []string{"user=foo", "pass=bar"},
		},
		{
			mountOptions:                 []string{"vers=2", "ro", "_netdev"},
			sensitiveMountOptions:        []string{"user=foo", "pass=bar", "remount"},
			isBind:                       false,
			expectedBindOpts:             []string{},
			expectedRemountOpts:          []string{},
			expectedSensitiveRemountOpts: []string{"user=foo", "pass=bar"},
		},
		{
			mountOptions:                 []string{"vers=2", "bind", "ro", "_netdev"},
			sensitiveMountOptions:        []string{"user=foo", "remount", "pass=bar"},
			isBind:                       true,
			expectedBindOpts:             []string{"bind", "_netdev"},
			expectedRemountOpts:          []string{"bind", "remount", "vers=2", "ro", "_netdev"},
			expectedSensitiveRemountOpts: []string{"user=foo", "pass=bar"},
		},
		{
			mountOptions:                 []string{"vers=2", "bind", "ro", "_netdev"},
			sensitiveMountOptions:        []string{"user=foo", "remount", "pass=bar"},
			isBind:                       true,
			expectedBindOpts:             []string{"bind", "_netdev"},
			expectedRemountOpts:          []string{"bind", "remount", "vers=2", "ro", "_netdev"},
			expectedSensitiveRemountOpts: []string{"user=foo", "pass=bar"},
		},
	}
	for _, test := range tests {
		bind, bindOpts, bindRemountOpts, bindRemountSensitiveOpts := MakeBindOptsSensitive(test.mountOptions, test.sensitiveMountOptions)
		if bind != test.isBind {
			t.Errorf("Expected bind to be %v but got %v", test.isBind, bind)
		}
		if test.isBind {
			if !reflect.DeepEqual(test.expectedBindOpts, bindOpts) {
				t.Errorf("Expected bind mount options to be %+v got %+v", test.expectedBindOpts, bindOpts)
			}
			if !reflect.DeepEqual(test.expectedRemountOpts, bindRemountOpts) {
				t.Errorf("Expected remount options to be %+v got %+v", test.expectedRemountOpts, bindRemountOpts)
			}
			if !reflect.DeepEqual(test.expectedSensitiveRemountOpts, bindRemountSensitiveOpts) {
				t.Errorf("Expected sensitive remount options to be %+v got %+v", test.expectedSensitiveRemountOpts, bindRemountSensitiveOpts)
			}
		}

	}
}

// 不只要确认密码没出现，还要确认占位符数量与敏感项数一致：只掩码一部分同样危险。
func TestOptionsForLogging(t *testing.T) {
	testcases := []struct {
		options          []string
		sensitiveOptions []string
	}{
		{
			options:          []string{"o1", "o2"},
			sensitiveOptions: []string{"s1"},
		},
		{
			options:          []string{"o1", "o2"},
			sensitiveOptions: []string{"s1", "s2"},
		},
		{
			sensitiveOptions: []string{"s1", "s2"},
		},
		{
			options: []string{"o1", "o2"},
		},
		{},
	}

	// 覆盖只有敏感项、只有普通项、两者都空等组合，防止实现只处理某一种输入。
	for _, v := range testcases {
		maskedStr := sanitizedOptionsForLogging(v.options, v.sensitiveOptions)

		for _, sensitiveOption := range v.sensitiveOptions {
			if strings.Contains(maskedStr, sensitiveOption) {
				t.Errorf("Found sensitive log option %q in %q", sensitiveOption, maskedStr)
			}
		}

		actualCount := strings.Count(maskedStr, sensitiveOptionsRemoved)
		expectedCount := len(v.sensitiveOptions)
		if actualCount != expectedCount {
			t.Errorf("Found %v instances of %q in %q. Expected %v", actualCount, sensitiveOptionsRemoved, maskedStr, expectedCount)
		}
	}
}

// 0 与负数表示不启用限流；只有正数才建信号量，免得每次格式化都背着同步开销。
func TestWithMaxConcurrentFormat(t *testing.T) {
	const timeout = 1 * time.Minute

	// 同时校验容量与超时值，避免"有限流但超时为零"这种半成品配置。
	tests := []struct {
		max     int
		wantSem bool
	}{
		{
			max: 0,
		},
		{
			max: -1,
		},
		{
			max:     1,
			wantSem: true,
		},
		{
			max:     3,
			wantSem: true,
		},
	}

	for _, tc := range tests {
		t.Run(fmt.Sprintf("max=%d,timeout=%s", tc.max, timeout.String()), func(t *testing.T) {
			mounter := NewSafeFormatAndMount(nil, nil, WithMaxConcurrentFormat(tc.max, timeout))

			if gotSem := mounter.formatSem != nil; gotSem != tc.wantSem {
				t.Errorf("NewSafeFormatAndMount() got formatSem: %t, want: %t", gotSem, tc.wantSem)
			}
			if tc.wantSem {
				if gotCap := cap(mounter.formatSem); gotCap != tc.max {
					t.Errorf("NewSafeFormatAndMount() got cap(formatSem): %d, want: %d", gotCap, tc.max)
				}
				if mounter.formatTimeout != timeout {
					t.Errorf("NewSafeFormatAndMount() got formatTimeout: %s, want: %s", mounter.formatTimeout.String(), timeout.String())
				}
			}
		})
	}
}
