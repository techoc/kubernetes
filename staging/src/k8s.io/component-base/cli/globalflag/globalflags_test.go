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

package globalflag

import (
	"flag"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/spf13/pflag"

	cliflag "k8s.io/component-base/cli/flag"
	"k8s.io/component-base/logs"
)

// TestAddGlobalFlags 验证 AddGlobalFlags 注册的 flag 集合与全局 flag 一致：
// 它会从标准库 flag 和 logs 包注册的 flag 推导期望集合，再做正向与负向匹配。
func TestAddGlobalFlags(t *testing.T) {
	namedFlagSets := &cliflag.NamedFlagSets{}
	nfs := namedFlagSets.FlagSet("global")
	nfs.SetNormalizeFunc(cliflag.WordSepNormalizeFunc)
	AddGlobalFlags(nfs, "test-cmd")

	actualFlag := []string{}
	nfs.VisitAll(func(flag *pflag.Flag) {
		actualFlag = append(actualFlag, flag.Name)
	})

	// 从 flag.CommandLine 收集期望 flag，但排除测试框架注入的 test.*。
	wantedFlag := []string{"help"}
	pflag.CommandLine.AddGoFlagSet(flag.CommandLine)
	logs.AddFlags(pflag.CommandLine)
	normalizeFunc := nfs.GetNormalizeFunc()
	pflag.VisitAll(func(flag *pflag.Flag) {
		if !strings.Contains(flag.Name, "test.") {
			wantedFlag = append(wantedFlag, string(normalizeFunc(nfs, flag.Name)))
		}
	})
	sort.Strings(wantedFlag)

	if !reflect.DeepEqual(wantedFlag, actualFlag) {
		t.Errorf("[Default]: expected %+v, got %+v", wantedFlag, actualFlag)
	}

	// 后续用例通过期望是否相等来检查实际集合的匹配/不匹配：
	// matchExpected=true 表示期望与实际“不应”相等。
	tests := []struct {
		expectedFlag  []string
		matchExpected bool
	}{
		{
			// 正常情况
			expectedFlag:  []string{"help", "log-flush-frequency", "v", "vmodule"},
			matchExpected: false,
		},
		{
			// 缺少标志
			expectedFlag:  []string{"logtostderr", "log-dir"},
			matchExpected: true,
		},
		{
			// 空标志
			expectedFlag:  []string{},
			matchExpected: true,
		},
		{
			// 无效标志
			expectedFlag:  []string{"foo"},
			matchExpected: true,
		},
	}

	for i, test := range tests {
		if reflect.DeepEqual(test.expectedFlag, actualFlag) == test.matchExpected {
			t.Errorf("[%d]: expected %+v, got %+v", i, test.expectedFlag, actualFlag)
		}
	}
}
