/*
Copyright 2020 The Kubernetes Authors.

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

package validation

import (
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

// TestValidateListOptions 覆盖 ListOptions 的合法性组合，重点是 WatchList
// 相关的 ResourceVersionMatch/SendInitialEvents 约束以及 feature gate 开关的影响。
// 多个用例同名 "watch-sendInitialEvents-no-watchlist-fg-disabled" 是历史遗留，
// 后一个实际测试的是 continue 与 resourceVersionMatch 的互斥。
func TestValidateListOptions(t *testing.T) {
	cases := []struct {
		name                    string
		opts                    internalversion.ListOptions
		watchListFeatureEnabled bool
		expectErrors            []string
	}{{
		name: "valid-default",
		opts: internalversion.ListOptions{},
	}, {
		// exact 搭配具体 RV 是标准用法。
		name: "valid-resourceversionmatch-exact",
		opts: internalversion.ListOptions{
			ResourceVersion:      "1",
			ResourceVersionMatch: metav1.ResourceVersionMatchExact,
		},
	}, {
		// RV=0 是"任意/最新"的哨兵值，exact 语义下没有意义，必须拒绝。
		name: "invalid-resourceversionmatch-exact",
		opts: internalversion.ListOptions{
			ResourceVersion:      "0",
			ResourceVersionMatch: metav1.ResourceVersionMatchExact,
		},
		expectErrors: []string{"resourceVersionMatch: Forbidden: resourceVersionMatch \"exact\" is forbidden for resourceVersion \"0\""},
	}, {
		name: "valid-resourceversionmatch-notolderthan",
		opts: internalversion.ListOptions{
			ResourceVersion:      "0",
			ResourceVersionMatch: metav1.ResourceVersionMatchNotOlderThan,
		},
	}, {
		name: "invalid-resourceversionmatch",
		opts: internalversion.ListOptions{
			ResourceVersion:      "0",
			ResourceVersionMatch: "foo",
		},
		expectErrors: []string{"resourceVersionMatch: Unsupported value: \"foo\": supported values: \"Exact\", \"NotOlderThan\", \"\""},
	}, {
		// list 不能用 sendInitialEvents（那是 watch 的语义）。
		name: "list-sendInitialEvents-forbidden",
		opts: internalversion.ListOptions{
			SendInitialEvents: ptr.To(true),
		},
		expectErrors: []string{"sendInitialEvents: Forbidden: sendInitialEvents is forbidden for list"},
	}, {
		name: "valid-watch-default",
		opts: internalversion.ListOptions{
			Watch: true,
		},
	}, {
		name: "valid-watch-sendInitialEvents-on",
		opts: internalversion.ListOptions{
			Watch:                true,
			SendInitialEvents:    ptr.To(true),
			ResourceVersionMatch: metav1.ResourceVersionMatchNotOlderThan,
			AllowWatchBookmarks:  true,
		},
		watchListFeatureEnabled: true,
	}, {
		name: "valid-watch-sendInitialEvents-off",
		opts: internalversion.ListOptions{
			Watch:                true,
			SendInitialEvents:    ptr.To(false),
			ResourceVersionMatch: metav1.ResourceVersionMatchNotOlderThan,
			AllowWatchBookmarks:  true,
		},
		watchListFeatureEnabled: true,
	}, {
		// WatchList 语义要求 NotOlderThan，单独给 resourceVersionMatch 不合法。
		name: "watch-resourceversionmatch-without-sendInitialEvents-forbidden",
		opts: internalversion.ListOptions{
			Watch:                true,
			ResourceVersionMatch: metav1.ResourceVersionMatchNotOlderThan,
		},
		expectErrors: []string{"resourceVersionMatch: Forbidden: resourceVersionMatch is forbidden for watch unless sendInitialEvents is provided"},
	}, {
		// gate 关闭时启用 sendInitialEvents 被拒；同时缺 NotOlderThan 也报错。
		name: "watch-sendInitialEvents-without-resourceversionmatch-forbidden",
		opts: internalversion.ListOptions{
			Watch:             true,
			SendInitialEvents: ptr.To(true),
		},
		expectErrors: []string{"resourceVersionMatch: Forbidden: sendInitialEvents requires setting resourceVersionMatch to NotOlderThan", "sendInitialEvents: Forbidden: sendInitialEvents is forbidden for watch unless the WatchList feature gate is enabled"},
	}, {
		name: "watch-sendInitialEvents-with-exact-resourceversionmatch-forbidden",
		opts: internalversion.ListOptions{
			Watch:                true,
			SendInitialEvents:    ptr.To(true),
			ResourceVersionMatch: metav1.ResourceVersionMatchExact,
			AllowWatchBookmarks:  true,
		},
		watchListFeatureEnabled: true,
		expectErrors:            []string{"resourceVersionMatch: Forbidden: sendInitialEvents requires setting resourceVersionMatch to NotOlderThan", "resourceVersionMatch: Unsupported value: \"Exact\": supported values: \"NotOlderThan\""},
	}, {
		name: "watch-sendInitialEvents-on-with-empty-resourceversionmatch-forbidden",
		opts: internalversion.ListOptions{
			Watch:                true,
			SendInitialEvents:    ptr.To(true),
			ResourceVersionMatch: "",
		},
		expectErrors: []string{"resourceVersionMatch: Forbidden: sendInitialEvents requires setting resourceVersionMatch to NotOlderThan", "sendInitialEvents: Forbidden: sendInitialEvents is forbidden for watch unless the WatchList feature gate is enabled"},
	}, {
		name: "watch-sendInitialEvents-off-with-empty-resourceversionmatch-forbidden",
		opts: internalversion.ListOptions{
			Watch:                true,
			SendInitialEvents:    ptr.To(false),
			ResourceVersionMatch: "",
		},
		expectErrors: []string{"resourceVersionMatch: Forbidden: sendInitialEvents requires setting resourceVersionMatch to NotOlderThan", "sendInitialEvents: Forbidden: sendInitialEvents is forbidden for watch unless the WatchList feature gate is enabled"},
	}, {
		name: "watch-sendInitialEvents-with-incorrect-resourceversionmatch-forbidden",
		opts: internalversion.ListOptions{
			Watch:                true,
			SendInitialEvents:    ptr.To(true),
			ResourceVersionMatch: "incorrect",
			AllowWatchBookmarks:  true,
		},
		watchListFeatureEnabled: true,
		expectErrors:            []string{"resourceVersionMatch: Forbidden: sendInitialEvents requires setting resourceVersionMatch to NotOlderThan", "resourceVersionMatch: Unsupported value: \"incorrect\": supported values: \"NotOlderThan\""},
	}, {
		// note that validating allowWatchBookmarks would break backward compatibility
		// because it was possible to request initial events via resourceVersion=0 before this change
		name: "watch-sendInitialEvents-no-allowWatchBookmark",
		opts: internalversion.ListOptions{
			Watch:                true,
			SendInitialEvents:    ptr.To(true),
			ResourceVersionMatch: metav1.ResourceVersionMatchNotOlderThan,
		},
		watchListFeatureEnabled: true,
	}, {
		// gate 关闭时即使参数齐全，sendInitialEvents 仍被拒绝。
		name: "watch-sendInitialEvents-no-watchlist-fg-disabled",
		opts: internalversion.ListOptions{
			Watch:                true,
			SendInitialEvents:    ptr.To(true),
			ResourceVersionMatch: metav1.ResourceVersionMatchNotOlderThan,
			AllowWatchBookmarks:  true,
		},
		expectErrors: []string{"sendInitialEvents: Forbidden: sendInitialEvents is forbidden for watch unless the WatchList feature gate is enabled"},
	}, {
		// watch 同时带 continue 时 resourceVersionMatch 无意义，拒绝该组合。
		name: "watch-sendInitialEvents-no-watchlist-fg-disabled",
		opts: internalversion.ListOptions{
			Watch:                true,
			SendInitialEvents:    ptr.To(true),
			ResourceVersionMatch: metav1.ResourceVersionMatchNotOlderThan,
			AllowWatchBookmarks:  true,
			Continue:             "123",
		},
		watchListFeatureEnabled: true,
		expectErrors:            []string{"resourceVersionMatch: Forbidden: resourceVersionMatch is forbidden when continue is provided"},
	}}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			errs := ValidateListOptions(&tc.opts, tc.watchListFeatureEnabled)
			// 错误顺序是校验实现的一部分，这里逐条按序比较，避免掩盖顺序回归。
			if len(tc.expectErrors) > 0 {
				if len(errs) != len(tc.expectErrors) {
					t.Errorf("expected %d errors but got %d errors", len(tc.expectErrors), len(errs))
					return
				}
				for i, expectedErr := range tc.expectErrors {
					if expectedErr != errs[i].Error() {
						t.Errorf("expected error '%s' but got '%s'", expectedErr, errs[i].Error())
					}
				}
				return
			}
			if len(errs) != 0 {
				t.Errorf("expected no errors, but got: %v", errs)
			}
		})
	}
}
