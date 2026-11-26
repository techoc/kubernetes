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
	"sync/atomic"
	"testing"

	"k8s.io/utils/clock"
	"k8s.io/utils/ptr"

	flowcontrol "k8s.io/api/flowcontrol/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	fcboot "k8s.io/apiserver/pkg/apis/flowcontrol/bootstrap"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/endpoints/request"
	fq "k8s.io/apiserver/pkg/util/flowcontrol/fairqueuing"
	fqtesting "k8s.io/apiserver/pkg/util/flowcontrol/fairqueuing/testing"
	fcfmt "k8s.io/apiserver/pkg/util/flowcontrol/format"
	"k8s.io/apiserver/pkg/util/flowcontrol/metrics"
)

// noRestraintQSF 是不施加并发限制的队列工厂，用于在不引入额外调度行为的前提下
// 校验生成的 PriorityLevelConfiguration 结构是否合法。
var noRestraintQSF = fqtesting.NewNoRestraintFactory()

// genPL 用给定名字与随机 spec 构造合法的 PriorityLevelConfiguration，
// 名字不能与内置优先级同名。
// 生成的配置 95% 使用排队响应，同时随机化队列数/HandSize，
// 以覆盖 APF 队列算法的不同参数组合。
func genPL(rng *rand.Rand, name string) *flowcontrol.PriorityLevelConfiguration {
	plc := &flowcontrol.PriorityLevelConfiguration{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: flowcontrol.PriorityLevelConfigurationSpec{
			Type: flowcontrol.PriorityLevelEnablementLimited,
			Limited: &flowcontrol.LimitedPriorityLevelConfiguration{
				NominalConcurrencyShares: ptr.To(int32(rng.Int31n(100) + 1)),
				LimitResponse: flowcontrol.LimitResponse{
					Type: flowcontrol.LimitResponseTypeReject}}}}
	if rng.Float32() < 0.95 {
		plc.Spec.Limited.LimitResponse.Type = flowcontrol.LimitResponseTypeQueue
		hs := rng.Int31n(5) + 1
		plc.Spec.Limited.LimitResponse.Queuing = &flowcontrol.QueuingConfiguration{
			Queues:           hs + rng.Int31n(20),
			HandSize:         hs,
			QueueLengthLimit: 5}
	}
	labelVals := []string{"test"}
	// 构造队列集会同步校验配置；生成非法配置时直接 panic 以便暴露生成器 bug。
	_, err := queueSetCompleterForPL(noRestraintQSF, nil, plc, metrics.RatioedGaugeVecPhasedElementPair(metrics.PriorityLevelConcurrencyGaugeVec, 1, 1, labelVals), metrics.PriorityLevelExecutionSeatsGaugeVec.NewForLabelValuesSafe(0, 1, labelVals), fq.NewNamedIntegrator(clock.RealClock{}, name))
	if err != nil {
		panic(err)
	}
	return plc
}

// FlowSchema 及其与测试相关的特征。
// fsTestingRecord 把一个 FlowSchema 与其“应匹配/不匹配”的请求摘要关联起来。
// digests 的第一层是“是否匹配”，第二层是“是否为资源请求”，
// 便于测试按维度统计匹配行为。
type fsTestingRecord struct {
	fs *flowcontrol.FlowSchema
	// 是否引用了存在的优先级。
	wellFormed                    bool
	matchesAllResourceRequests    bool
	matchesAllNonResourceRequests bool
	// 按「是否匹配」「是否为资源请求」两级索引请求摘要。
	digests map[bool]map[bool][]RequestDigest
}

// addDigest 按匹配结果与请求类别归档一个摘要。
func (ftr *fsTestingRecord) addDigest(digest RequestDigest, matches bool) {
	ftr.digests[matches][digest.RequestInfo.IsResourceRequest] = append(ftr.digests[matches][digest.RequestInfo.IsResourceRequest], digest)
}

// addDigests 批量归档摘要。
func (ftr *fsTestingRecord) addDigests(digests []RequestDigest, matches bool) {
	for _, digest := range digests {
		ftr.addDigest(digest, matches)
	}
}

// flowDistinguisherMethodTypes 是合法的区分方法集合。
var flowDistinguisherMethodTypes = sets.NewString(
	string(flowcontrol.FlowDistinguisherMethodByUserType),
	string(flowcontrol.FlowDistinguisherMethodByNamespaceType),
)

// mandFTRExempt 对应内置的 exempt FlowSchema：
// 只有 system:privileged 组能匹配，普通已认证用户不匹配。
var mandFTRExempt = &fsTestingRecord{
	fs:         fcboot.MandatoryFlowSchemaExempt,
	wellFormed: true,
	digests: map[bool]map[bool][]RequestDigest{
		false: {
			false: {{
				RequestInfo: &request.RequestInfo{
					IsResourceRequest: false,
					Path:              "/foo/bar",
					Verb:              "frobulate"},
				User: &user.DefaultInfo{
					Name:   "nobody",
					Groups: []string{user.AllAuthenticated, "nogroup"},
				},
			}},
			true: {{
				RequestInfo: &request.RequestInfo{
					IsResourceRequest: true,
					Verb:              "mandate",
					APIGroup:          "nogroup",
					Namespace:         "nospace",
					Resource:          "nons",
				},
				User: &user.DefaultInfo{
					Name:   "nobody",
					Groups: []string{user.AllAuthenticated, "nogroup"},
				},
			}},
		},
		true: {
			false: {{
				RequestInfo: &request.RequestInfo{
					IsResourceRequest: false,
					Path:              "/foo/bar",
					Verb:              "frobulate"},
				User: &user.DefaultInfo{
					Name:   "nobody",
					Groups: []string{user.AllAuthenticated, user.SystemPrivilegedGroup},
				},
			}},
			true: {{
				RequestInfo: &request.RequestInfo{
					IsResourceRequest: true,
					Verb:              "mandate",
					APIGroup:          "nogroup",
					Namespace:         "nospace",
					Resource:          "nons",
				},
				User: &user.DefaultInfo{
					Name:   "nobody",
					Groups: []string{user.AllAuthenticated, user.SystemPrivilegedGroup},
				},
			}},
		},
	},
}

// mandFTRCatchAll 对应内置的 catch-all FlowSchema：匹配所有已认证请求。
var mandFTRCatchAll = &fsTestingRecord{
	fs:         fcboot.MandatoryFlowSchemaCatchAll,
	wellFormed: true,
	digests: map[bool]map[bool][]RequestDigest{
		false: {},
		true: {
			false: {{
				RequestInfo: &request.RequestInfo{
					IsResourceRequest: false,
					Path:              "/foo/bar",
					Verb:              "frobulate"},
				User: &user.DefaultInfo{
					Name:   "nobody",
					Groups: []string{user.AllAuthenticated, "nogroup"},
				},
			}},
			true: {{
				RequestInfo: &request.RequestInfo{
					IsResourceRequest: true,
					Verb:              "mandate",
					APIGroup:          "nogroup",
					Namespace:         "nospace",
					Resource:          "nons",
				},
				User: &user.DefaultInfo{
					Name:   "nobody",
					Groups: []string{user.AllAuthenticated, "nogroup"},
				},
			}},
		},
	},
}

// genFS 用给定名字与随机 spec 构造合法 FlowSchema 及其测试特征。
// 当一组 FlowSchema 使用不同名字生成时：(a) 匹配摘要只会命中生成它的那个 schema；
// (b) 不匹配摘要不会命中组内的任何 schema。生成的 spec 大概率合法但也可能非法，
// 非法时会引用 badPLNames 中的优先级。goodPLNames 可为空，badPLNames 不能为空。
//
// 关键不变量：所有名字都以传入的 name 为前缀，因此不同 FlowSchema 的
// 匹配摘要互不重叠，测试可以只凭名字判断某请求应匹配哪个 schema。
func genFS(t *testing.T, rng *rand.Rand, name string, mayMatchClusterScope bool, goodPLNames, badPLNames sets.String) *fsTestingRecord {
	fs := &flowcontrol.FlowSchema{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       flowcontrol.FlowSchemaSpec{}}
	// 5% 概率生成 0 条规则，否则从 1--6 中偏小地取值。
	nRules := (1 + rng.Intn(3)) * (1 + rng.Intn(2)) * ((19 + rng.Intn(20)) / 20)
	ftr := &fsTestingRecord{fs: fs,
		wellFormed:                    true,
		matchesAllResourceRequests:    nRules > 0 && rng.Float32() < 0.1,
		matchesAllNonResourceRequests: nRules > 0 && rng.Float32() < 0.1,
		digests: map[bool]map[bool][]RequestDigest{
			false: {false: {}, true: {}},
			true:  {false: {}, true: {}}},
	}
	// 90% 概率引用合法优先级；否则生成“悬空引用”，用于覆盖非法配置路径。
	dangleStatus := flowcontrol.ConditionFalse
	if rng.Float32() < 0.9 && len(goodPLNames) > 0 {
		fs.Spec.PriorityLevelConfiguration = flowcontrol.PriorityLevelConfigurationReference{Name: pickSetString(rng, goodPLNames)}
	} else {
		fs.Spec.PriorityLevelConfiguration = flowcontrol.PriorityLevelConfigurationReference{Name: pickSetString(rng, badPLNames)}
		ftr.wellFormed = false
		dangleStatus = flowcontrol.ConditionTrue
	}
	fs.Status.Conditions = []flowcontrol.FlowSchemaCondition{{
		Type:   flowcontrol.FlowSchemaConditionDangling,
		Status: dangleStatus}}
	fs.Spec.MatchingPrecedence = rng.Int31n(9997) + 2
	if rng.Float32() < 0.8 {
		fdmt := flowcontrol.FlowDistinguisherMethodType(pickSetString(rng, flowDistinguisherMethodTypes))
		fs.Spec.DistinguisherMethod = &flowcontrol.FlowDistinguisherMethod{Type: fdmt}
	}
	fs.Spec.Rules = []flowcontrol.PolicyRulesWithSubjects{}
	everyResourceMatcher := -1
	if ftr.matchesAllResourceRequests {
		if mayMatchClusterScope {
			everyResourceMatcher = 0
		} else {
			everyResourceMatcher = rng.Intn(nRules)
		}
	}
	everyNonResourceMatcher := -1
	if ftr.matchesAllNonResourceRequests {
		if mayMatchClusterScope {
			everyNonResourceMatcher = 0
		} else {
			everyNonResourceMatcher = rng.Intn(nRules)
		}
	}
	// 可能匹配集群级资源时只允许一条规则，否则会破坏规则间的互斥性。
	for i := 0; i < nRules && (i == 0 || !mayMatchClusterScope); i++ {
		rule, ruleMatchingRDigests, ruleMatchingNDigests, ruleSkippingRDigests, ruleSkippingNDigests := genPolicyRuleWithSubjects(t, rng, fmt.Sprintf("%s-%d", name, i+1), mayMatchClusterScope, ftr.matchesAllResourceRequests, ftr.matchesAllNonResourceRequests, i == everyResourceMatcher, i == everyNonResourceMatcher)
		fs.Spec.Rules = append(fs.Spec.Rules, rule)
		ftr.addDigests(ruleMatchingRDigests, true)
		ftr.addDigests(ruleMatchingNDigests, true)
		ftr.addDigests(ruleSkippingRDigests, false)
		ftr.addDigests(ruleSkippingNDigests, false)
	}
	// 无规则时仍需构造“不匹配任何规则”的摘要，以验证空规则集的行为。
	if nRules == 0 {
		var skippingRDigests, skippingNDigests []RequestDigest
		_, _, _, skippingRDigests, skippingNDigests = genPolicyRuleWithSubjects(t, rng, name+"-1", false, false, false, false, false)
		ftr.addDigests(skippingRDigests, false)
		ftr.addDigests(skippingNDigests, false)
	}
	if testDebugLogs {
		t.Logf("Returning name=%s, plRef=%q, wellFormed=%v, matchesAllResourceRequests=%v, matchesAllNonResourceRequests=%v for mayMatchClusterScope=%v", fs.Name, fs.Spec.PriorityLevelConfiguration.Name, ftr.wellFormed, ftr.matchesAllResourceRequests, ftr.matchesAllNonResourceRequests, mayMatchClusterScope)
	}
	return ftr
}

// noextra 表示不带 Extra 字段的用户，用于避免测试数据意外携带额外属性。
var noextra = make(map[string][]string)

// 生成一条合法的 PolicyRulesWithSubjects，并返回匹配/不匹配该规则的资源型与非资源型摘要。
// 一组规则使用唯一前缀生成时，每条规则的“不匹配摘要”不会命中组内任何规则。
// someMatchesAll* 表示组内是否有规则匹配全部对应类型请求，为真时相应的反例切片为空；
// matchAll* 表示当前生成的规则是否要匹配全部对应类型。
// 规则间互斥依赖以给定前缀开头的名字，但集群级请求没有 namespace 可用，
// 因此通常排除集群级规则；mayMatchClusterScope 为真时不再保证互斥。
func genPolicyRuleWithSubjects(t *testing.T, rng *rand.Rand, pfx string, mayMatchClusterScope, someMatchesAllResourceRequests, someMatchesAllNonResourceRequests, matchAllResourceRequests, matchAllNonResourceRequests bool) (flowcontrol.PolicyRulesWithSubjects, []RequestDigest, []RequestDigest, []RequestDigest, []RequestDigest) {
	subjects := []flowcontrol.Subject{}
	matchingUIs := []user.Info{}
	skippingUIs := []user.Info{}
	resourceRules := []flowcontrol.ResourcePolicyRule{}
	nonResourceRules := []flowcontrol.NonResourcePolicyRule{}
	matchingRRIs := []*request.RequestInfo{}
	skippingRRIs := []*request.RequestInfo{}
	matchingNRIs := []*request.RequestInfo{}
	skippingNRIs := []*request.RequestInfo{}
	nSubj := rng.Intn(4)
	for i := 0; i < nSubj; i++ {
		subject, smus, ssus := genSubject(rng, fmt.Sprintf("%s-%d", pfx, i+1))
		subjects = append(subjects, subject)
		matchingUIs = append(matchingUIs, smus...)
		skippingUIs = append(skippingUIs, ssus...)
	}
	// 需要匹配“全部请求”时，追加一个通配主体（* 用户/组或认证+未认证组）。
	if matchAllResourceRequests || matchAllNonResourceRequests {
		switch rng.Intn(3) {
		case 0:
			subjects = append(subjects, mkUserSubject("*"))
		case 1:
			subjects = append(subjects, mkGroupSubject("*"))
		default:
			subjects = append(subjects, mkGroupSubject("system:authenticated"), mkGroupSubject("system:unauthenticated"))
		}
		matchingUIs = append(matchingUIs, skippingUIs...)
	}
	// 若集合中已有规则匹配全部，则不存在“跳过”主体；否则至少生成一个反例。
	if someMatchesAllResourceRequests || someMatchesAllNonResourceRequests {
		skippingUIs = []user.Info{}
	} else if nSubj == 0 {
		_, _, skippingUIs = genSubject(rng, pfx+"-o")
	}
	var nRR, nNRR int
	for nRR+nNRR == 0 || matchAllResourceRequests && nRR == 0 || matchAllNonResourceRequests && nNRR == 0 {
		nRR = rng.Intn(4)
		nNRR = rng.Intn(4)
	}
	allResourceMatcher := -1
	if matchAllResourceRequests {
		allResourceMatcher = rng.Intn(nRR)
	}
	// 可能匹配集群级资源时只允许一条资源规则，否则会破坏规则间互斥。
	for i := 0; i < nRR && (i == 0 || !mayMatchClusterScope); i++ {
		rr, rmrs, rsrs := genResourceRule(rng, fmt.Sprintf("%s-%d", pfx, i+1), mayMatchClusterScope, i == allResourceMatcher, someMatchesAllResourceRequests)
		resourceRules = append(resourceRules, rr)
		matchingRRIs = append(matchingRRIs, rmrs...)
		skippingRRIs = append(skippingRRIs, rsrs...)
	}
	if nRR == 0 {
		_, _, skippingRRIs = genResourceRule(rng, pfx+"-o", mayMatchClusterScope, false, someMatchesAllResourceRequests)
	}
	allNonResourceMatcher := -1
	if matchAllNonResourceRequests {
		allNonResourceMatcher = rng.Intn(nNRR)
	}
	for i := 0; i < nNRR; i++ {
		nrr, nmrs, nsrs := genNonResourceRule(rng, fmt.Sprintf("%s-%d", pfx, i+1), i == allNonResourceMatcher, someMatchesAllNonResourceRequests)
		nonResourceRules = append(nonResourceRules, nrr)
		matchingNRIs = append(matchingNRIs, nmrs...)
		skippingNRIs = append(skippingNRIs, nsrs...)
	}
	if nRR == 0 {
		_, _, skippingNRIs = genNonResourceRule(rng, pfx+"-o", false, someMatchesAllNonResourceRequests)
	}
	rule := flowcontrol.PolicyRulesWithSubjects{Subjects: subjects, ResourceRules: resourceRules, NonResourceRules: nonResourceRules}
	if testDebugLogs {
		t.Logf("For pfx=%s, mayMatchClusterScope=%v, someMatchesAllResourceRequests=%v, someMatchesAllNonResourceRequests=%v, marr=%v, manrr=%v: generated prws=%s, mu=%s, su=%s, mrr=%s, mnr=%s, srr=%s, snr=%s", pfx, mayMatchClusterScope, someMatchesAllResourceRequests, someMatchesAllNonResourceRequests, matchAllResourceRequests, matchAllNonResourceRequests, fcfmt.Fmt(rule), fcfmt.Fmt(matchingUIs), fcfmt.Fmt(skippingUIs), fcfmt.Fmt(matchingRRIs), fcfmt.Fmt(matchingNRIs), fcfmt.Fmt(skippingRRIs), fcfmt.Fmt(skippingNRIs))
	}
	// 用主体×请求的笛卡尔积得到候选摘要；skip 摘要覆盖“主体不匹配”“请求不匹配”
	// 以及“两者都不匹配”三种反例。
	matchingRDigests := cross(matchingUIs, matchingRRIs)
	skippingRDigests := append(append(cross(matchingUIs, skippingRRIs),
		cross(skippingUIs, matchingRRIs)...),
		cross(skippingUIs, skippingRRIs)...)
	matchingNDigests := cross(matchingUIs, matchingNRIs)
	skippingNDigests := append(append(cross(matchingUIs, skippingNRIs),
		cross(skippingUIs, matchingNRIs)...),
		cross(skippingUIs, skippingNRIs)...)
	// 随机抽取一个子集，控制用例规模并增加覆盖多样性。
	matchingRDigests = shuffleAndTakeDigests(t, rng, &rule, true, matchingRDigests, (1+rng.Intn(2))*(1+rng.Intn(2)))
	skippingRDigests = shuffleAndTakeDigests(t, rng, &rule, false, skippingRDigests, (1+rng.Intn(2))*(1+rng.Intn(2)))
	matchingNDigests = shuffleAndTakeDigests(t, rng, &rule, true, matchingNDigests, (1+rng.Intn(2))*(1+rng.Intn(2)))
	skippingNDigests = shuffleAndTakeDigests(t, rng, &rule, false, skippingNDigests, (1+rng.Intn(2))*(1+rng.Intn(2)))
	return rule, matchingRDigests, matchingNDigests, skippingRDigests, skippingNDigests
}

// cross 生成用户与请求信息的全部组合。
func cross(uis []user.Info, ris []*request.RequestInfo) []RequestDigest {
	ans := make([]RequestDigest, 0, len(uis)*len(ris))
	for _, ui := range uis {
		for _, ri := range ris {
			ans = append(ans, RequestDigest{RequestInfo: ri, User: ui})
		}
	}
	return ans
}

// shuffleAndTakeDigests 随机抽取 n 个摘要，并在规则非 nil 时即时验证
// 每个摘要的匹配结果与 toMatch 一致——这是生成器正确性的自检。
func shuffleAndTakeDigests(t *testing.T, rng *rand.Rand, rule *flowcontrol.PolicyRulesWithSubjects, toMatch bool, digests []RequestDigest, n int) []RequestDigest {
	ans := make([]RequestDigest, 0, n)
	for len(ans) < n && len(digests) > 0 {
		i := rng.Intn(len(digests))
		digest := digests[i]
		ans = append(ans, digest)
		digests[i] = digests[len(digests)-1]
		digests = digests[:len(digests)-1]
		if rule != nil {
			thisMatches := matchesPolicyRule(digest, rule)
			if toMatch {
				if testDebugLogs {
					t.Logf("Added matching digest %#+v", digest)
				}
				if !thisMatches {
					t.Errorf("Fail in check: rule %s does not match digest %#+v", fcfmt.Fmt(rule), digest)
				}
			} else {
				if testDebugLogs {
					t.Logf("Added skipping digest %#+v", digest)
				}
				if thisMatches {
					t.Errorf("Fail in check: rule %s matches digest %#+v", fcfmt.Fmt(rule), digest)
				}
			}
		}
	}
	return ans
}

// uCounter 为 uniqify 提供进程内唯一的标识来源。
var uCounter uint32 = 1

// uniqify 给用户附加递增的 Extra 值，使原本相同的摘要在去重/哈希时被视为不同条目。
func uniqify(in RequestDigest) RequestDigest {
	u1 := in.User.(*user.DefaultInfo)
	u2 := *u1
	u2.Extra = map[string][]string{"u": {fmt.Sprintf("z%d", atomic.AddUint32(&uCounter, 1))}}
	return RequestDigest{User: &u2, RequestInfo: in.RequestInfo}
}

// genSubject 生成一个匹配给定前缀名称的合法 Subject，
// 并返回匹配与不匹配的成员列表（两者都使用以该前缀开头的名字）。
func genSubject(rng *rand.Rand, pfx string) (flowcontrol.Subject, []user.Info, []user.Info) {
	subject := flowcontrol.Subject{}
	var matchingUIs, skippingUIs []user.Info
	// 三种主体类型大致等概率出现。
	x := rng.Float32()
	switch {
	case x < 0.33:
		subject.Kind = flowcontrol.SubjectKindUser
		subject.User, matchingUIs, skippingUIs = genUser(rng, pfx)
	case x < 0.67:
		subject.Kind = flowcontrol.SubjectKindGroup
		subject.Group, matchingUIs, skippingUIs = genGroup(rng, pfx)
	default:
		subject.Kind = flowcontrol.SubjectKindServiceAccount
		subject.ServiceAccount, matchingUIs, skippingUIs = genServiceAccount(rng, pfx)
	}
	return subject, matchingUIs, skippingUIs
}

// genUser 生成 User 主体及匹配/不匹配的用户；不匹配者仅名字多一个后缀。
func genUser(rng *rand.Rand, pfx string) (*flowcontrol.UserSubject, []user.Info, []user.Info) {
	mui := &user.DefaultInfo{
		Name:   pfx + "-u",
		UID:    "good-id",
		Groups: []string{pfx + "-g1", mg(rng), pfx + "-g2"},
		Extra:  noextra}
	skips := []user.Info{&user.DefaultInfo{
		Name:   mui.Name + "x",
		UID:    mui.UID,
		Groups: mui.Groups,
		Extra:  mui.Extra}}
	return &flowcontrol.UserSubject{Name: mui.Name}, []user.Info{mui}, skips
}

// groupCover 是所有已认证/未认证用户都会属于的两个基础组，
// 用于生成不会干扰前缀匹配的“噪声”组。
var groupCover = []string{"system:authenticated", "system:unauthenticated"}

// mg 随机选择一个基础组。
func mg(rng *rand.Rand) string {
	return groupCover[rng.Intn(len(groupCover))]
}

// mkUserSubject 构造匹配任意名字的 User 主体（名字可为 "*"）。
func mkUserSubject(username string) flowcontrol.Subject {
	return flowcontrol.Subject{
		Kind: flowcontrol.SubjectKindUser,
		User: &flowcontrol.UserSubject{Name: username},
	}
}

// mkGroupSubject 构造匹配指定组的主体（组名可为 "*"）。
func mkGroupSubject(group string) flowcontrol.Subject {
	return flowcontrol.Subject{
		Kind:  flowcontrol.SubjectKindGroup,
		Group: &flowcontrol.GroupSubject{Name: group},
	}
}

// genGroup 生成 Group 主体及匹配/不匹配的用户，
// 随机在匹配组的相对位置插入噪声组，以检验组匹配与顺序无关。
func genGroup(rng *rand.Rand, pfx string) (*flowcontrol.GroupSubject, []user.Info, []user.Info) {
	name := pfx + "-g"
	ui := &user.DefaultInfo{
		Name:   pfx + "-u",
		UID:    "good-id",
		Groups: []string{name},
		Extra:  noextra}
	if rng.Intn(2) == 0 {
		ui.Groups = append([]string{mg(rng)}, ui.Groups...)
	} else {
		ui.Groups = append(ui.Groups, mg(rng))
	}
	if rng.Intn(3) == 0 {
		ui.Groups = append([]string{pfx + "-h"}, ui.Groups...)
	}
	if rng.Intn(3) == 0 {
		ui.Groups = append(ui.Groups, pfx+"-i")
	}
	skipper := &user.DefaultInfo{
		Name:   pfx + "-u",
		UID:    "bad-id",
		Groups: []string{pfx + "-j", mg(rng)},
		Extra:  noextra}
	if rng.Intn(2) == 0 {
		skipper.Groups = append(skipper.Groups, pfx+"-k")
	}
	return &flowcontrol.GroupSubject{Name: name}, []user.Info{ui}, []user.Info{skipper}
}

// genServiceAccount 生成 ServiceAccount 主体及匹配/不匹配用户；
// 5% 概率使用 "*" 名字以覆盖通配语义。
func genServiceAccount(rng *rand.Rand, pfx string) (*flowcontrol.ServiceAccountSubject, []user.Info, []user.Info) {
	ns := pfx + "-ns"
	name := pfx + "-n"
	mname := name
	if rng.Float32() < 0.05 {
		mname = "*"
	}
	mui := &user.DefaultInfo{
		Name:   fmt.Sprintf("system:serviceaccount:%s:%s", ns, name),
		UID:    "good-id",
		Groups: []string{pfx + "-g1", mg(rng), pfx + "-g2"},
		Extra:  noextra}
	var skips []user.Info
	// 不匹配者要么 namespace 少/多一个字符，要么用户名多一个后缀。
	if mname == "*" || rng.Intn(2) == 0 {
		skips = []user.Info{&user.DefaultInfo{
			Name:   fmt.Sprintf("system:serviceaccount:%sx:%s", ns, name),
			UID:    "bad-id",
			Groups: mui.Groups,
			Extra:  mui.Extra}}
	} else {
		skips = []user.Info{&user.DefaultInfo{
			Name:   fmt.Sprintf("system:serviceaccount:%s:%sx", ns, name),
			UID:    "bad-id",
			Groups: mui.Groups,
			Extra:  mui.Extra}}
	}
	return &flowcontrol.ServiceAccountSubject{Namespace: ns, Name: mname}, []user.Info{mui}, skips
}

// genResourceRule 随机生成合法的 ResourcePolicyRule，以及匹配/不匹配的 RequestInfo 列表。
func genResourceRule(rng *rand.Rand, pfx string, mayMatchClusterScope, matchAllResources, someMatchesAllResources bool) (flowcontrol.ResourcePolicyRule, []*request.RequestInfo, []*request.RequestInfo) {
	namespaces := []string{pfx + "-n1", pfx + "-n2", pfx + "-n3"}
	rnamespaces := namespaces
	// 10% 概率把第一个命名空间置空并标记 ClusterScope，覆盖集群级规则。
	if mayMatchClusterScope && rng.Float32() < 0.1 {
		namespaces[0] = ""
		rnamespaces = namespaces[1:]
	}
	rr := flowcontrol.ResourcePolicyRule{
		Verbs:        []string{pfx + "-v1", pfx + "-v2", pfx + "-v3"},
		APIGroups:    []string{pfx + ".g1", pfx + ".g2", pfx + ".g3"},
		Resources:    []string{pfx + "-r1s", pfx + "-r2s", pfx + "-r3s"},
		ClusterScope: namespaces[0] == "",
		Namespaces:   rnamespaces}
	matchingRIs := genRRIs(rng, 3, rr.Verbs, rr.APIGroups, rr.Resources, namespaces)
	var skippingRIs []*request.RequestInfo
	if !someMatchesAllResources {
		skipNSs := []string{pfx + "-n4", pfx + "-n5", pfx + "-n6"}
		if mayMatchClusterScope && rr.Namespaces[0] != "" && rng.Float32() < 0.1 {
			skipNSs[0] = ""
		}
		skippingRIs = genRRIs(rng, 3,
			[]string{pfx + "-v4", pfx + "-v5", pfx + "-v6"},
			[]string{pfx + ".g4", pfx + ".g5", pfx + ".g6"},
			[]string{pfx + "-r4s", pfx + "-r5s", pfx + "-r6s"},
			skipNSs)
	}
	// 选取部分字段通配；仅在非“匹配全部”时才有意义。
	starMask := rng.Intn(15)
	if matchAllResources || starMask&1 == 1 && rng.Float32() < 0.1 {
		rr.Verbs = []string{flowcontrol.VerbAll}
	}
	if matchAllResources || starMask&2 == 2 && rng.Float32() < 0.1 {
		rr.APIGroups = []string{flowcontrol.APIGroupAll}
	}
	if matchAllResources || starMask&4 == 4 && rng.Float32() < 0.1 {
		rr.Resources = []string{flowcontrol.ResourceAll}
	}
	if matchAllResources || starMask&8 == 8 && rng.Float32() < 0.1 {
		rr.ClusterScope = true
		rr.Namespaces = []string{flowcontrol.NamespaceEvery}
	}
	return rr, matchingRIs, skippingRIs
}

// genRRIs 在 verbs/groups/resources/namespaces 的笛卡尔积中随机取 m 个不同的请求。
func genRRIs(rng *rand.Rand, m int, verbs, apiGroups, resources, namespaces []string) []*request.RequestInfo {
	nv := len(verbs)
	ng := len(apiGroups)
	nr := len(resources)
	nn := len(namespaces)
	coords := chooseInts(rng, nv*ng*nr*nn, m)
	ans := make([]*request.RequestInfo, 0, m)
	for _, coord := range coords {
		ans = append(ans, &request.RequestInfo{
			IsResourceRequest: true,
			Verb:              verbs[coord%nv],
			APIGroup:          apiGroups[coord/nv%ng],
			Resource:          resources[coord/nv/ng%nr],
			Namespace:         namespaces[coord/nv/ng/nr]})
	}
	return ans
}

// genNRRIs 在 verbs×urls 中随机取 m 个非资源请求；
// 随机追加 "/more" 以覆盖前缀匹配（而非精确匹配）的语义。
func genNRRIs(rng *rand.Rand, m int, verbs, urls []string) []*request.RequestInfo {
	nv := len(verbs)
	nu := len(urls)
	coords := chooseInts(rng, nv*nu, m)
	ans := make([]*request.RequestInfo, 0, m)
	for _, coord := range coords {
		ri := &request.RequestInfo{
			IsResourceRequest: false,
			Verb:              verbs[coord%nv],
			Path:              urls[coord/nv]}
		if rng.Intn(2) == 1 {
			ri.Path = ri.Path + "/more"
		}
		ans = append(ans, ri)
	}
	return ans
}

// chooseInts 无放回地随机抽取 m 个 [0,n) 的整数。
func chooseInts(rng *rand.Rand, n, m int) []int {
	ans := sets.NewInt()
	for len(ans) < m {
		i := rng.Intn(n)
		if ans.Has(i) {
			continue
		}
		ans.Insert(i)
	}
	return ans.List()
}

// genNonResourceRule 随机生成合法的 NonResourcePolicyRule，以及匹配/不匹配的 RequestInfo 列表。
func genNonResourceRule(rng *rand.Rand, pfx string, matchAllNonResources, someMatchesAllNonResources bool) (flowcontrol.NonResourcePolicyRule, []*request.RequestInfo, []*request.RequestInfo) {
	nrr := flowcontrol.NonResourcePolicyRule{
		Verbs:           []string{pfx + "-v1", pfx + "-v2", pfx + "-v3"},
		NonResourceURLs: []string{"/" + pfx + "/g/p1", "/" + pfx + "/g/p2", "/" + pfx + "/g/p3"},
	}
	matchingRIs := genNRRIs(rng, 3, nrr.Verbs, nrr.NonResourceURLs)
	var skippingRIs []*request.RequestInfo
	if !someMatchesAllNonResources {
		skippingRIs = genNRRIs(rng, 3,
			[]string{pfx + "-v4", pfx + "-v5", pfx + "-v6"},
			[]string{"/" + pfx + "/b/p1", "/" + pfx + "/b/p2", "/" + pfx + "/b/p3"})
	}
	// 选取部分字段考虑通配；仅在非“匹配全部”时才有意义。
	// 注意：非通配分支会同时加入 "/*" 前缀与不带尾斜杠的路径，
	// 覆盖“前缀匹配”与“精确匹配”两种 URL 语义。
	starMask := rng.Intn(3)
	if matchAllNonResources || starMask&1 == 1 && rng.Float32() < 0.1 {
		nrr.Verbs = []string{flowcontrol.VerbAll}
	}
	if matchAllNonResources || starMask&2 == 2 && rng.Float32() < 0.1 {
		nrr.NonResourceURLs = []string{"*"}
	} else {
		nrr.NonResourceURLs[rng.Intn(3)] = "/" + pfx + "/g/*"
		nrr.NonResourceURLs[rng.Intn(3)] = "/" + pfx + "/g"
	}
	return nrr, matchingRIs, skippingRIs
}

// pickSetString 从集合中随机取一个元素；集合为空时 panic，
// 因为调用方都保证 badPLNames 非空。
func pickSetString(rng *rand.Rand, set sets.String) string {
	i, n := 0, rng.Intn(len(set))
	for s := range set {
		if i == n {
			return s
		}
		i++
	}
	panic("empty set")
}
