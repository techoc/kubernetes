/*
Copyright 2021 The Kubernetes Authors.

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

package cel

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/checker"
	"github.com/google/cel-go/common/types"

	apiextensions "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/schema"
	"k8s.io/apimachinery/pkg/util/version"
	celconfig "k8s.io/apiserver/pkg/apis/cel"
	apiservercel "k8s.io/apiserver/pkg/cel"
	"k8s.io/apiserver/pkg/cel/environment"
	"k8s.io/apiserver/pkg/cel/library"
	"k8s.io/apiserver/pkg/cel/metrics"
)

const (
	// ScopedVarName 是 CEL 校验表达式中局部作用域数据元素绑定的变量名。
	ScopedVarName = "self"

	// OldScopedVarName 是 CEL 校验表达式中局部作用域数据元素的旧值绑定的变量名。
	OldScopedVarName = "oldSelf"
)

// CompilationResult 表示单条规则的 CEL 编译结果。
type CompilationResult struct {
	Program cel.Program
	Error   *apiservercel.Error
	// 为 true 表示编译后的表达式引用了标识符 "oldSelf"。
	UsesOldSelf bool
	// 编译后表达式的 CEL 成本单位最坏情况，以 cel.EstimateCost 的口径度量。
	MaxCost uint64
	// MaxCardinality 表示若该规则位于 OpenAPIv3 schema 中无界的 map 或 list 下，
	// 最坏情况下可能被调用的次数。
	MaxCardinality uint64
	// MessageExpression 是规则校验失败时用于生成错误消息的已编译 CEL 程序。
	// 未提供 MessageExpression 或该表达式编译失败时为 nil。
	MessageExpression cel.Program
	// MessageExpressionError 是编译 MessageExpression 时遇到的错误，没有错误时为 nil。
	MessageExpressionError *apiservercel.Error
	// MessageExpressionMaxCost 是已编译 MessageExpression 的最坏情况成本，
	// 以 cel.EstimateCost 的口径度量。
	MessageExpressionMaxCost uint64
	// NormalizedRuleFieldPath 是用户指定并归一化后的相对字段路径。
	NormalizedRuleFieldPath string
}

// EnvLoader 决定每个表达式使用哪个 CEL 环境。
// 调用方应根据 environment.NewExpressions 和 environment.StoredExpressions
// 的指引返回合适的 CEL 环境。
type EnvLoader interface {
	// RuleEnv 从 EnvSet 中为给定 CEL 规则返回合适的环境。
	RuleEnv(envSet *environment.EnvSet, expression string) *cel.Env
	// MessageExpressionEnv 从 EnvSet 中为给定 CEL messageExpression 返回合适的环境。
	MessageExpressionEnv(envSet *environment.EnvSet, expression string) *cel.Env
}

// NewExpressionsEnvLoader 创建一个始终使用 NewExpressions 环境类型的 EnvLoader。
func NewExpressionsEnvLoader() EnvLoader {
	return alwaysNewEnvLoader{loadFn: func(envSet *environment.EnvSet) *cel.Env {
		return envSet.NewExpressionsEnv()
	}}
}

// StoredExpressionsEnvLoader 创建一个始终使用 StoredExpressions 环境类型的 EnvLoader。
func StoredExpressionsEnvLoader() EnvLoader {
	return alwaysNewEnvLoader{loadFn: func(envSet *environment.EnvSet) *cel.Env {
		return envSet.StoredExpressionsEnv()
	}}
}

// alwaysNewEnvLoader 忽略表达式内容，始终从 EnvSet 取同一类环境。
type alwaysNewEnvLoader struct {
	loadFn func(envSet *environment.EnvSet) *cel.Env
}

// RuleEnv 返回固定环境。
func (pe alwaysNewEnvLoader) RuleEnv(envSet *environment.EnvSet, _ string) *cel.Env {
	return pe.loadFn(envSet)
}

// MessageExpressionEnv 返回固定环境。
func (pe alwaysNewEnvLoader) MessageExpressionEnv(envSet *environment.EnvSet, _ string) *cel.Env {
	return pe.loadFn(envSet)
}

// Compile 编译全部 XValidations 规则（不递归进 schema），为每条 ValidationRule
// 返回一个 CompilationResult；出错则返回 error。declType 应是对应结构 schema 的
// CEL DeclType。
// 每个 CompilationResult 可能是：
//   - Program 非 nil、Error 为 nil：编译成功
//   - Program 为 nil、Error 非 nil：编译失败
//   - Program 为 nil、Error 为 nil：规则为空，未尝试编译
//
// perCallLimit 仅供测试使用。调用方应始终传入 k8s.io/apiserver/pkg/apis/cel/config.go 中的常量 PerCallLimit。
// baseEnvSet 用作基础 CEL 环境，见 common.BaseEnvironment。
func Compile(s *schema.Structural, declType *apiservercel.DeclType, perCallLimit uint64, baseEnvSet *environment.EnvSet, envLoader EnvLoader) ([]CompilationResult, error) {
	t := time.Now()
	defer func() {
		metrics.Metrics.ObserveCompilation(time.Since(t))
	}()

	if len(s.XValidations) == 0 {
		return nil, nil
	}
	if declType == nil {
		return nil, errors.New("failed to convert to declType for CEL validation rules")
	}
	celRules := s.XValidations

	oldSelfEnvSet, optionalOldSelfEnvSet, err := prepareEnvSet(baseEnvSet, declType)
	if err != nil {
		return nil, err
	}
	estimator := newCostEstimator(declType)
	// compResults 保存编译结果，顺序与 x-kubernetes-validations 规则一致。
	compResults := make([]CompilationResult, len(celRules))
	maxCardinality := maxCardinality(declType.MinSerializedSize)
	for i, rule := range celRules {
		// optionalOldSelf 规则需要 oldSelf 允许为 optional，未启用时用普通 oldSelf 环境。
		ruleEnvSet := oldSelfEnvSet
		if rule.OptionalOldSelf != nil && *rule.OptionalOldSelf {
			ruleEnvSet = optionalOldSelfEnvSet
		}
		compResults[i] = compileRule(s, rule, ruleEnvSet, envLoader, estimator, maxCardinality, perCallLimit)
	}

	return compResults, nil
}

// prepareEnvSet 构造两套环境：oldSelf 恒定的，以及 oldSelf 为 optional 的。
func prepareEnvSet(baseEnvSet *environment.EnvSet, declType *apiservercel.DeclType) (oldSelfEnvSet *environment.EnvSet, optionalOldSelfEnvSet *environment.EnvSet, err error) {
	scopedType := declType.MaybeAssignTypeName(generateUniqueSelfTypeName())

	oldSelfEnvSet, err = baseEnvSet.Extend(
		environment.VersionedOptions{
			// 特性实际在 1.23 引入，但这里刻意设为 1.0，因为这些选项应始终存在。
			IntroducedVersion: version.MajorMinor(1, 0),
			EnvOptions: []cel.EnvOption{
				cel.Variable(ScopedVarName, scopedType.CelType()),
			},
			DeclTypes: []*apiservercel.DeclType{
				scopedType,
			},
		},
		environment.VersionedOptions{
			IntroducedVersion: version.MajorMinor(1, 24),
			EnvOptions: []cel.EnvOption{
				cel.Variable(OldScopedVarName, scopedType.CelType()),
			},
		},
	)
	if err != nil {
		return nil, nil, err
	}

	optionalOldSelfEnvSet, err = baseEnvSet.Extend(
		environment.VersionedOptions{
			// 特性实际在 1.23 引入，但这里刻意设为 1.0，因为这些选项应始终存在。
			IntroducedVersion: version.MajorMinor(1, 0),
			EnvOptions: []cel.EnvOption{
				cel.Variable(ScopedVarName, scopedType.CelType()),
			},
			DeclTypes: []*apiservercel.DeclType{
				scopedType,
			},
		},
		environment.VersionedOptions{
			IntroducedVersion: version.MajorMinor(1, 24),
			EnvOptions: []cel.EnvOption{
				cel.Variable(OldScopedVarName, types.NewOptionalType(scopedType.CelType())),
			},
		},
	)
	if err != nil {
		return nil, nil, err
	}

	return oldSelfEnvSet, optionalOldSelfEnvSet, nil
}

// compileRule 编译单条校验规则及其可选的 messageExpression。
func compileRule(s *schema.Structural, rule apiextensions.ValidationRule, envSet *environment.EnvSet, envLoader EnvLoader, estimator *library.CostEstimator, maxCardinality uint64, perCallLimit uint64) (compilationResult CompilationResult) {
	if len(strings.TrimSpace(rule.Rule)) == 0 {
		// 仍返回一个编译结果，但按本函数的返回值语义，program 和 error 都留 nil
		return
	}
	ruleEnv := envLoader.RuleEnv(envSet, rule.Rule)
	ast, issues := ruleEnv.Compile(rule.Rule)
	if issues != nil {
		compilationResult.Error = &apiservercel.Error{Type: apiservercel.ErrorTypeInvalid, Detail: "compilation failed: " + issues.String()}
		return
	}
	if ast.OutputType() != cel.BoolType {
		compilationResult.Error = &apiservercel.Error{Type: apiservercel.ErrorTypeInvalid, Detail: "cel expression must evaluate to a bool"}
		return
	}

	checkedExpr, err := cel.AstToCheckedExpr(ast)
	if err != nil {
		// 理论上不可能：env.Compile 未报错就不会走到这里
		compilationResult.Error = &apiservercel.Error{Type: apiservercel.ErrorTypeInternal, Detail: "unexpected compilation error: " + err.Error()}
		return
	}
	// 检查引用表即可判断是否用到 oldSelf，无需展开 AST。
	for _, ref := range checkedExpr.ReferenceMap {
		if ref.Name == OldScopedVarName {
			compilationResult.UsesOldSelf = true
			break
		}
	}

	// TODO: 理想情况下可以在校验时把每条表达式的限额设为剩余总预算，
	// 但这需要能在求值时传入限额，或把程序创建推迟到校验阶段
	prog, err := ruleEnv.Program(ast,
		cel.CostLimit(perCallLimit),
		cel.CostTracking(estimator),
		cel.InterruptCheckFrequency(celconfig.CheckFrequency),
	)
	if err != nil {
		compilationResult.Error = &apiservercel.Error{Type: apiservercel.ErrorTypeInvalid, Detail: "program instantiation failed: " + err.Error()}
		return
	}
	costEst, err := ruleEnv.EstimateCost(ast, estimator)
	if err != nil {
		compilationResult.Error = &apiservercel.Error{Type: apiservercel.ErrorTypeInternal, Detail: "cost estimation failed: " + err.Error()}
		return
	}
	compilationResult.MaxCost = costEst.Max
	compilationResult.MaxCardinality = maxCardinality
	compilationResult.Program = prog
	if rule.MessageExpression != "" {
		messageEnv := envLoader.MessageExpressionEnv(envSet, rule.MessageExpression)
		ast, issues := messageEnv.Compile(rule.MessageExpression)
		if issues != nil {
			compilationResult.MessageExpressionError = &apiservercel.Error{Type: apiservercel.ErrorTypeInvalid, Detail: "messageExpression compilation failed: " + issues.String()}
			return
		}
		if ast.OutputType() != cel.StringType {
			compilationResult.MessageExpressionError = &apiservercel.Error{Type: apiservercel.ErrorTypeInvalid, Detail: "messageExpression must evaluate to a string"}
			return
		}

		_, err := cel.AstToCheckedExpr(ast)
		if err != nil {
			compilationResult.MessageExpressionError = &apiservercel.Error{Type: apiservercel.ErrorTypeInternal, Detail: "unexpected messageExpression compilation error: " + err.Error()}
			return
		}

		msgProg, err := messageEnv.Program(ast,
			cel.CostLimit(perCallLimit),
			cel.CostTracking(estimator),
			cel.InterruptCheckFrequency(celconfig.CheckFrequency),
		)
		if err != nil {
			compilationResult.MessageExpressionError = &apiservercel.Error{Type: apiservercel.ErrorTypeInvalid, Detail: "messageExpression instantiation failed: " + err.Error()}
			return
		}
		costEst, err := messageEnv.EstimateCost(ast, estimator)
		if err != nil {
			compilationResult.MessageExpressionError = &apiservercel.Error{Type: apiservercel.ErrorTypeInternal, Detail: "cost estimation failed for messageExpression: " + err.Error()}
			return
		}
		compilationResult.MessageExpression = msgProg
		compilationResult.MessageExpressionMaxCost = costEst.Max
	}
	if rule.FieldPath != "" {
		// 归一化失败的路径会在校验阶段报错，这里只在成功时记录。
		validFieldPath, _, err := ValidFieldPath(rule.FieldPath, s)
		if err == nil {
			compilationResult.NormalizedRuleFieldPath = validFieldPath.String()
		}
	}
	return
}

// generateUniqueSelfTypeName 生成一个占位类型名，用于不希望向 CEL 规则作者暴露
// 稳定类型名的场景。为了真正防止开发者依赖生成的名字（例如在 CEL 程序中直接使用），
// 每次创建或更新 CRD 时都必须改变它。
func generateUniqueSelfTypeName() string {
	return fmt.Sprintf("selfType%d", time.Now().Nanosecond())
}

// newCostEstimator 创建带 schema 感知的大小估算器的成本估算器。
func newCostEstimator(root *apiservercel.DeclType) *library.CostEstimator {
	return &library.CostEstimator{SizeEstimator: &sizeEstimator{root: root}}
}

// sizeEstimator 根据 DeclType 估算 CEL AST 节点的集合大小。
type sizeEstimator struct {
	root *apiservercel.DeclType
}

// EstimateSize 沿 AST 路径下探 DeclType，返回节点的最大元素数估算。
func (c *sizeEstimator) EstimateSize(element checker.AstNode) *checker.SizeEstimate {
	if len(element.Path()) == 0 {
		// Path() 可能返回空列表，此时无法给出大小估算，直接返回。
		return nil
	}
	currentNode := c.root
	// 路径总以 "self" 开头，先切掉
	for _, name := range element.Path()[1:] {
		switch name {
		case "@items", "@values":
			if currentNode.ElemType == nil {
				return nil
			}
			currentNode = currentNode.ElemType
		case "@keys":
			if currentNode.KeyType == nil {
				return nil
			}
			currentNode = currentNode.KeyType
		default:
			field, ok := currentNode.Fields[name]
			if !ok {
				return nil
			}
			if field.Type == nil {
				return nil
			}
			currentNode = field.Type
		}
	}
	return &checker.SizeEstimate{Min: 0, Max: uint64(currentNode.MaxElements)}
}

// EstimateCallCost 不做函数调用成本估算。
func (c *sizeEstimator) EstimateCallCost(function, overloadID string, target *checker.AstNode, args []checker.AstNode) *checker.CallEstimate {
	return nil
}

// maxCardinality 返回在序列化为 JSON 的对象中，符合给定最小尺寸的数据最多可能
// 出现多少次。当 schema 位于无界的 map 或数组 schema 下时，可用它估算匹配该
// schema 的数据最坏重复次数。
// 注意这里只假设数据元素之间有一个逗号；若 schema 仅位于 map 下，估算会偏大。
// 应把 DeclType.MinSerializedSize 传入此函数。
func maxCardinality(minSize int64) uint64 {
	sz := minSize + 1 // 假设元素之间至少有一个逗号
	return uint64(celconfig.MaxRequestSizeBytes / sz)
}
