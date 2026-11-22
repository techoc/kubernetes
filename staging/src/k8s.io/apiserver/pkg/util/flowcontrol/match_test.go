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

package flowcontrol

import (
	"fmt"
	"math/rand"
	"testing"

	flowcontrol "k8s.io/api/flowcontrol/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/endpoints/request"
	fcfmt "k8s.io/apiserver/pkg/util/flowcontrol/format"
)

// TestMatching 固定种子（42）生成 300 个随机 FlowSchema 及其请求摘要，
// 逐一验证匹配结果。固定种子使失败可复现；先验证两个强制规则
// （Exempt 与 CatchAll），确保匹配器的最基本路径始终成立。
func TestMatching(t *testing.T) {
	checkFTR(t, mandFTRExempt)
	checkFTR(t, mandFTRCatchAll)
	rngOuter := rand.New(rand.NewSource(42))
	goodPLNames := sets.NewString("pl1", "pl2", "pl3", "pl4", "pl5")
	badPLNames := sets.NewString("ql1", "ql2", "ql3", "ql4", "ql5")
	for i := 0; i < 300; i++ {
		rng := rand.New(rand.NewSource(int64(rngOuter.Uint64())))
		t.Run(fmt.Sprintf("trial%d:", i), func(t *testing.T) {
			ftr := genFS(t, rng, fmt.Sprintf("fs%d", i), rng.Float32() < 0.2, goodPLNames, badPLNames)
			checkFTR(t, ftr)
		})
	}
}

// checkFTR 按 digests 中记录的正/负样本逐条比对 matchesFlowSchema。
// digests 的键即期望的匹配结果，从而无需为每个样本单独写断言。
func checkFTR(t *testing.T, ftr *fsTestingRecord) {
	for expectMatch, digests1 := range ftr.digests {
		if testDebugLogs {
			t.Logf("%s.digests[%v] = %#+v", ftr.fs.Name, expectMatch, digests1)
		}
		for _, digests2 := range digests1 {
			for _, digest := range digests2 {
				actualMatch := matchesFlowSchema(digest, ftr.fs)
				if expectMatch != actualMatch {
					t.Errorf("Fail for %s vs %#+v: expectedMatch=%v, actualMatch=%v", fcfmt.Fmt(ftr.fs), digest, expectMatch, actualMatch)
				}
			}
		}
	}
}

// TestPolicyRules 用随机生成的 PolicyRule 以及明确应匹配/不匹配的请求
// 摘要验证 matchesPolicyRule。应匹配样本覆盖 subject 与规则均命中的
// 情况；应跳过样本覆盖 subject 或任一规则字段不命中的情况。
func TestPolicyRules(t *testing.T) {
	rngOuter := rand.New(rand.NewSource(42))
	for i := 0; i < 300; i++ {
		rng := rand.New(rand.NewSource(int64(rngOuter.Uint64())))
		t.Run(fmt.Sprintf("trial%d:", i), func(t *testing.T) {
			r := rng.Float32()
			n := rng.Float32()
			policyRule, matchingRDigests, matchingNDigests, skippingRDigests, skippingNDigests := genPolicyRuleWithSubjects(t, rng, fmt.Sprintf("t%d", i), rng.Float32() < 0.2, r < 0.10, n < 0.10, r < 0.05, n < 0.05)
			if testDebugLogs {
				t.Logf("policyRule=%s, mrd=%#+v, mnd=%#+v, srd=%#+v, snd=%#+v", fcfmt.Fmt(policyRule), matchingRDigests, matchingNDigests, skippingRDigests, skippingNDigests)
			}
			for _, digest := range append(matchingRDigests, matchingNDigests...) {
				if !matchesPolicyRule(digest, &policyRule) {
					t.Errorf("Fail: expected %s to match %#+v but it did not", fcfmt.Fmt(policyRule), digest)
				}
			}
			for _, digest := range append(skippingRDigests, skippingNDigests...) {
				if matchesPolicyRule(digest, &policyRule) {
					t.Errorf("Fail: expected %s to not match %#+v but it did", fcfmt.Fmt(policyRule), digest)
				}
			}
		})
	}
}

// TestLiterals 用写死的请求摘要逐项验证字面量匹配规则：user/group
// 主题（含 "*" 通配）、verb/apiGroup/resource/namespace 的字面量与 "*"
// 通配、命名空间与集群范围的区别，以及非资源请求的 URL 匹配。
// checkRules 的 expectMatch 为期望结果，正反两组规则成对出现。
func TestLiterals(t *testing.T) {
	// reqRN：带命名空间的资源请求，用于验证 namespace 维度。
	ui := &user.DefaultInfo{Name: "goodu", UID: "1",
		Groups: []string{"goodg1", "goodg2"}}
	reqRN := RequestDigest{
		RequestInfo: &request.RequestInfo{
			IsResourceRequest: true,
			Path:              "/apis/goodapig/v1/namespaces/goodns/goodrscs",
			Verb:              "goodverb",
			APIPrefix:         "apis",
			APIGroup:          "goodapig",
			APIVersion:        "v1",
			Namespace:         "goodns",
			Resource:          "goodrscs",
			Name:              "eman",
			Parts:             []string{"goodrscs", "eman"},
		},
		User: ui,
	}
	// reqRU：集群范围（无命名空间）的资源请求，用于验证 ClusterScope。
	reqRU := RequestDigest{
		RequestInfo: &request.RequestInfo{
			IsResourceRequest: true,
			Path:              "/apis/goodapig/v1/goodrscs",
			Verb:              "goodverb",
			APIPrefix:         "apis",
			APIGroup:          "goodapig",
			APIVersion:        "v1",
			Namespace:         "",
			Resource:          "goodrscs",
			Name:              "eman",
			Parts:             []string{"goodrscs", "eman"},
		},
		User: ui,
	}
	// reqN：非资源请求，只有 verb 与 URL 可用于匹配。
	reqN := RequestDigest{
		RequestInfo: &request.RequestInfo{
			IsResourceRequest: false,
			Path:              "/openapi/v2",
			Verb:              "goodverb",
		},
		User: ui,
	}
	// 命名空间资源请求的正向规则：每个维度都命中，含两次通配。
	checkRules(t, true, reqRN, []flowcontrol.PolicyRulesWithSubjects{{
		Subjects: []flowcontrol.Subject{{Kind: flowcontrol.SubjectKindUser,
			User: &flowcontrol.UserSubject{Name: "goodu"}}},
		ResourceRules: []flowcontrol.ResourcePolicyRule{{
			Verbs:      []string{"goodverb"},
			APIGroups:  []string{"goodapig"},
			Resources:  []string{"goodrscs"},
			Namespaces: []string{"goodns"}}}}, {
		Subjects: []flowcontrol.Subject{{Kind: flowcontrol.SubjectKindGroup,
			Group: &flowcontrol.GroupSubject{Name: "goodg1"}}},
		ResourceRules: []flowcontrol.ResourcePolicyRule{{
			Verbs:      []string{"goodverb"},
			APIGroups:  []string{"goodapig"},
			Resources:  []string{"goodrscs"},
			Namespaces: []string{"goodns"}}}}, {
		Subjects: []flowcontrol.Subject{{Kind: flowcontrol.SubjectKindUser,
			User: &flowcontrol.UserSubject{Name: "*"}}},
		ResourceRules: []flowcontrol.ResourcePolicyRule{{
			Verbs:      []string{"goodverb"},
			APIGroups:  []string{"goodapig"},
			Resources:  []string{"goodrscs"},
			Namespaces: []string{"goodns"}}}}, {
		Subjects: []flowcontrol.Subject{{Kind: flowcontrol.SubjectKindGroup,
			Group: &flowcontrol.GroupSubject{Name: "*"}}},
		ResourceRules: []flowcontrol.ResourcePolicyRule{{
			Verbs:      []string{"goodverb"},
			APIGroups:  []string{"goodapig"},
			Resources:  []string{"goodrscs"},
			Namespaces: []string{"goodns"}}}}, {
		Subjects: []flowcontrol.Subject{{Kind: flowcontrol.SubjectKindUser,
			User: &flowcontrol.UserSubject{Name: "goodu"}}},
		ResourceRules: []flowcontrol.ResourcePolicyRule{{
			Verbs:      []string{"*"},
			APIGroups:  []string{"goodapig"},
			Resources:  []string{"goodrscs"},
			Namespaces: []string{"goodns"}}}}, {
		Subjects: []flowcontrol.Subject{{Kind: flowcontrol.SubjectKindUser,
			User: &flowcontrol.UserSubject{Name: "goodu"}}},
		ResourceRules: []flowcontrol.ResourcePolicyRule{{
			Verbs:      []string{"goodverb"},
			APIGroups:  []string{"*"},
			Resources:  []string{"goodrscs"},
			Namespaces: []string{"goodns"}}}}, {
		Subjects: []flowcontrol.Subject{{Kind: flowcontrol.SubjectKindUser,
			User: &flowcontrol.UserSubject{Name: "goodu"}}},
		ResourceRules: []flowcontrol.ResourcePolicyRule{{
			Verbs:      []string{"goodverb"},
			APIGroups:  []string{"goodapig"},
			Resources:  []string{"*"},
			Namespaces: []string{"goodns"}}}}, {
		Subjects: []flowcontrol.Subject{{Kind: flowcontrol.SubjectKindUser,
			User: &flowcontrol.UserSubject{Name: "goodu"}}},
		ResourceRules: []flowcontrol.ResourcePolicyRule{{
			Verbs:      []string{"goodverb"},
			APIGroups:  []string{"goodapig"},
			Resources:  []string{"goodrscs"},
			Namespaces: []string{"*"}}}},
	})
	// 命名空间资源请求的反向规则：每个规则都有一处不命中。
	checkRules(t, false, reqRN, []flowcontrol.PolicyRulesWithSubjects{{
		Subjects: []flowcontrol.Subject{{Kind: flowcontrol.SubjectKindUser,
			User: &flowcontrol.UserSubject{Name: "badu"}}},
		ResourceRules: []flowcontrol.ResourcePolicyRule{{
			Verbs:      []string{"goodverb"},
			APIGroups:  []string{"goodapig"},
			Resources:  []string{"goodrscs"},
			Namespaces: []string{"goodns"}}}}, {
		Subjects: []flowcontrol.Subject{{Kind: flowcontrol.SubjectKindGroup,
			Group: &flowcontrol.GroupSubject{Name: "badg"}}},
		ResourceRules: []flowcontrol.ResourcePolicyRule{{
			Verbs:      []string{"goodverb"},
			APIGroups:  []string{"goodapig"},
			Resources:  []string{"goodrscs"},
			Namespaces: []string{"goodns"}}}}, {
		Subjects: []flowcontrol.Subject{{Kind: flowcontrol.SubjectKindUser,
			User: &flowcontrol.UserSubject{Name: "goodu"}}},
		ResourceRules: []flowcontrol.ResourcePolicyRule{{
			Verbs:      []string{"badverb"},
			APIGroups:  []string{"goodapig"},
			Resources:  []string{"goodrscs"},
			Namespaces: []string{"goodns"}}}}, {
		Subjects: []flowcontrol.Subject{{Kind: flowcontrol.SubjectKindUser,
			User: &flowcontrol.UserSubject{Name: "goodu"}}},
		ResourceRules: []flowcontrol.ResourcePolicyRule{{
			Verbs:      []string{"goodverb"},
			APIGroups:  []string{"badapig"},
			Resources:  []string{"goodrscs"},
			Namespaces: []string{"goodns"}}}}, {
		Subjects: []flowcontrol.Subject{{Kind: flowcontrol.SubjectKindUser,
			User: &flowcontrol.UserSubject{Name: "goodu"}}},
		ResourceRules: []flowcontrol.ResourcePolicyRule{{
			Verbs:      []string{"goodverb"},
			APIGroups:  []string{"goodapig"},
			Resources:  []string{"badrscs"},
			Namespaces: []string{"goodns"}}}}, {
		Subjects: []flowcontrol.Subject{{Kind: flowcontrol.SubjectKindUser,
			User: &flowcontrol.UserSubject{Name: "goodu"}}},
		ResourceRules: []flowcontrol.ResourcePolicyRule{{
			Verbs:      []string{"goodverb"},
			APIGroups:  []string{"goodapig"},
			Resources:  []string{"goodrscs"},
			Namespaces: []string{"badns"}}}},
	})
	// 集群范围请求的正向规则：ClusterScope 为 true 且无 namespace 限制。
	checkRules(t, true, reqRU, []flowcontrol.PolicyRulesWithSubjects{{
		Subjects: []flowcontrol.Subject{{Kind: flowcontrol.SubjectKindUser,
			User: &flowcontrol.UserSubject{Name: "goodu"}}},
		ResourceRules: []flowcontrol.ResourcePolicyRule{{
			Verbs:        []string{"goodverb"},
			APIGroups:    []string{"goodapig"},
			Resources:    []string{"goodrscs"},
			ClusterScope: true}}}, {
		Subjects: []flowcontrol.Subject{{Kind: flowcontrol.SubjectKindUser,
			User: &flowcontrol.UserSubject{Name: "goodu"}}},
		ResourceRules: []flowcontrol.ResourcePolicyRule{{
			Verbs:        []string{"*"},
			APIGroups:    []string{"goodapig"},
			Resources:    []string{"goodrscs"},
			ClusterScope: true}}}, {
		Subjects: []flowcontrol.Subject{{Kind: flowcontrol.SubjectKindUser,
			User: &flowcontrol.UserSubject{Name: "goodu"}}},
		ResourceRules: []flowcontrol.ResourcePolicyRule{{
			Verbs:        []string{"goodverb"},
			APIGroups:    []string{"*"},
			Resources:    []string{"goodrscs"},
			ClusterScope: true}}}, {
		Subjects: []flowcontrol.Subject{{Kind: flowcontrol.SubjectKindUser,
			User: &flowcontrol.UserSubject{Name: "goodu"}}},
		ResourceRules: []flowcontrol.ResourcePolicyRule{{
			Verbs:        []string{"goodverb"},
			APIGroups:    []string{"goodapig"},
			Resources:    []string{"*"},
			ClusterScope: true}}}})
	// 集群范围请求的反向规则：最后一例 ClusterScope=false 表示规则
	// 只匹配命名空间内资源，不能命中集群范围请求。
	checkRules(t, false, reqRU, []flowcontrol.PolicyRulesWithSubjects{{
		Subjects: []flowcontrol.Subject{{Kind: flowcontrol.SubjectKindUser,
			User: &flowcontrol.UserSubject{Name: "goodu"}}},
		ResourceRules: []flowcontrol.ResourcePolicyRule{{
			Verbs:        []string{"badverb"},
			APIGroups:    []string{"goodapig"},
			Resources:    []string{"goodrscs"},
			ClusterScope: true}}}, {
		Subjects: []flowcontrol.Subject{{Kind: flowcontrol.SubjectKindUser,
			User: &flowcontrol.UserSubject{Name: "goodu"}}},
		ResourceRules: []flowcontrol.ResourcePolicyRule{{
			Verbs:        []string{"goodverb"},
			APIGroups:    []string{"badapig"},
			Resources:    []string{"goodrscs"},
			ClusterScope: true}}}, {
		Subjects: []flowcontrol.Subject{{Kind: flowcontrol.SubjectKindUser,
			User: &flowcontrol.UserSubject{Name: "goodu"}}},
		ResourceRules: []flowcontrol.ResourcePolicyRule{{
			Verbs:        []string{"goodverb"},
			APIGroups:    []string{"goodapig"},
			Resources:    []string{"badrscs"},
			ClusterScope: true}}}, {
		Subjects: []flowcontrol.Subject{{Kind: flowcontrol.SubjectKindUser,
			User: &flowcontrol.UserSubject{Name: "goodu"}}},
		ResourceRules: []flowcontrol.ResourcePolicyRule{{
			Verbs:        []string{"goodverb"},
			APIGroups:    []string{"goodapig"},
			Resources:    []string{"goodrscs"},
			ClusterScope: false}}},
	})
	// 非资源请求的正向规则：匹配 verb 与非资源 URL，URL 支持 "*"。
	checkRules(t, true, reqN, []flowcontrol.PolicyRulesWithSubjects{{
		Subjects: []flowcontrol.Subject{{Kind: flowcontrol.SubjectKindUser,
			User: &flowcontrol.UserSubject{Name: "goodu"}}},
		NonResourceRules: []flowcontrol.NonResourcePolicyRule{{
			Verbs:           []string{"goodverb"},
			NonResourceURLs: []string{"/openapi/v2"}}}}, {
		Subjects: []flowcontrol.Subject{{Kind: flowcontrol.SubjectKindUser,
			User: &flowcontrol.UserSubject{Name: "goodu"}}},
		NonResourceRules: []flowcontrol.NonResourcePolicyRule{{
			Verbs:           []string{"*"},
			NonResourceURLs: []string{"/openapi/v2"}}}}, {
		Subjects: []flowcontrol.Subject{{Kind: flowcontrol.SubjectKindUser,
			User: &flowcontrol.UserSubject{Name: "goodu"}}},
		NonResourceRules: []flowcontrol.NonResourcePolicyRule{{
			Verbs:           []string{"goodverb"},
			NonResourceURLs: []string{"*"}}}},
	})
	// 非资源请求的反向规则：verb 或 URL 任一不命中。
	checkRules(t, false, reqN, []flowcontrol.PolicyRulesWithSubjects{{
		Subjects: []flowcontrol.Subject{{Kind: flowcontrol.SubjectKindUser,
			User: &flowcontrol.UserSubject{Name: "goodu"}}},
		NonResourceRules: []flowcontrol.NonResourcePolicyRule{{
			Verbs:           []string{"badverb"},
			NonResourceURLs: []string{"/openapi/v2"}}}}, {
		Subjects: []flowcontrol.Subject{{Kind: flowcontrol.SubjectKindUser,
			User: &flowcontrol.UserSubject{Name: "goodu"}}},
		NonResourceRules: []flowcontrol.NonResourcePolicyRule{{
			Verbs:           []string{"goodverb"},
			NonResourceURLs: []string{"/closedapi/v2"}}}},
	})
}

// checkRules 把每条规则包装成独立 FlowSchema 后验证匹配结果。
// 每条规则单独成 schema 是为了让失败信息直接指向出错规则。
func checkRules(t *testing.T, expectMatch bool, digest RequestDigest, rules []flowcontrol.PolicyRulesWithSubjects) {
	for idx, rule := range rules {
		fs := &flowcontrol.FlowSchema{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("rule%d", idx)},
			Spec: flowcontrol.FlowSchemaSpec{
				Rules: []flowcontrol.PolicyRulesWithSubjects{rule}}}
		actualMatch := matchesFlowSchema(digest, fs)
		if expectMatch != actualMatch {
			t.Errorf("expectMatch=%v, actualMatch=%v, digest=%#+v, fs=%s", expectMatch, actualMatch, digest, fcfmt.Fmt(fs))
		}
	}
}
