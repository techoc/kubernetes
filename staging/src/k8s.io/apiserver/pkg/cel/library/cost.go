/*
Copyright 2022 The Kubernetes Authors.

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

package library

import (
	"fmt"
	"math"

	"github.com/google/cel-go/checker"
	"github.com/google/cel-go/common"
	"github.com/google/cel-go/common/ast"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/common/types/traits"

	"k8s.io/apiserver/pkg/cel"
)

// panicOnUnknown 让成本估算函数在遇到未识别的函数时 panic，仅单元测试会置为 true。
//
// 生产环境遇到未建模的函数时静默回退，保证升级/扩展库不会直接打挂 apiserver；
// 测试里开启 panic 是为了强制新增函数必须补上成本模型。
var panicOnUnknown = false

// knownUnhandledFunctions 列出成本测试用到、但不交给 CostEstimator 处理的函数。
// 这些是 CEL 内建/基础运算符，成本由 CEL 自身计费，因此这里显式列为“无需处理”，
// 以免测试模式下的 panic 误报。
var knownUnhandledFunctions = map[string]bool{
	"@not_strictly_false": true,
	"uint":                true,
	"duration":            true,
	"bytes":               true,
	"cel.@mapInsert":      true,
	"timestamp":           true,
	"strings.quote":       true,
	"value":               true,
	"_==_":                true,
	"_&&_":                true,
	"_||_":                true,
	"_>_":                 true,
	"_>=_":                true,
	"_<_":                 true,
	"_<=_":                true,
	"!_":                  true,
	"_?_:_":               true,
	"_+_":                 true,
	"_-_":                 true,
}

// CostEstimator 实现 CEL 的 interpretable.ActualCostEstimator 和 checker.CostEstimator 接口。
//
// 两个接口合在一起意味着：同一套规则既用于编译期估算（决定表达式是否可写进
// CRD/VAP），也用于运行期实际计费（决定表达式是否超预算）。
type CostEstimator struct {
	// SizeEstimator 提供一个 CostEstimator.EstimateSize：当大小未知（即非常量）时，
	// 本 CostEstimator 会把大小估算委托给它。
	SizeEstimator checker.CostEstimator
}

const (
	// 能触发分配 values 切片的最短可重复选择器要求是 2 个字符：k，
	selectorLengthToRequirementCount = float64(.5)
	// 表示每个要求最昂贵的部分是结构体和 values 切片
	costPerRequirement = float64(common.ListCreateBaseCost + common.StructCreateBaseCost)
)

// 一个选择器由存放在切片中的一组要求构成
var baseSelectorCost = checker.CostEstimate{Min: common.ListCreateBaseCost, Max: common.ListCreateBaseCost}

func selectorCostEstimate(selectorLength checker.SizeEstimate) checker.CostEstimate {
	parseCost := selectorLength.MultiplyByCostFactor(common.StringTraversalCostFactor)

	requirementCount := selectorLength.MultiplyByCostFactor(selectorLengthToRequirementCount)
	requirementCost := requirementCount.MultiplyByCostFactor(costPerRequirement)

	return baseSelectorCost.Add(parseCost).Add(requirementCost)
}

// CallCost 返回给定函数在运行期（已求得实参）的一次调用成本。
// 返回 nil 表示交给 CEL 的默认计费逻辑处理。
// 成本根据实参的实际长度/元素数量计算，因此这里针对字符串、列表、正则等
// 分别建模；选择器解析等操作还与规则文本长度相关。
func (l *CostEstimator) CallCost(function, overloadId string, args []ref.Val, result ref.Val) *uint64 {
	switch function {
	case "check":
		// 授权检查的成本是固定的
		// 该成本设定为每条表达式只允许两次授权检查
		cost := uint64(350000)
		return &cost
	case "serviceAccount", "path", "group", "resource", "subresource", "namespace", "name", "allowed", "reason", "error", "errored":
		// 所有授权构建与访问函数都是名义成本
		cost := uint64(1)
		return &cost
	case "fieldSelector", "labelSelector":
		// fieldSelector 和 labelSelector 解析是把字符串解析成一组结构化要求
		if len(args) >= 2 {
			selectorLength := actualSize(args[1])
			cost := selectorCostEstimate(checker.SizeEstimate{Min: selectorLength, Max: selectorLength})
			return &cost.Max
		}
	case "isSorted", "sum", "max", "min", "indexOf", "lastIndexOf", "includes":
		var cost uint64
		if len(args) > 0 {
			cost += traversalCost(args[0]) // 这些 O(n) 操作的成本都约等于单次遍历的成本
		}
		return &cost
	case "url", "lowerAscii", "upperAscii", "substring", "trim", "jsonpatch.escapeKey":
		if len(args) >= 1 {
			cost := uint64(math.Ceil(float64(actualSize(args[0])) * common.StringTraversalCostFactor))
			return &cost
		}
	case "replace", "split":
		if len(args) >= 1 {
			// 成本是遍历加上构造结果
			cost := uint64(math.Ceil(float64(actualSize(args[0])) * 2 * common.StringTraversalCostFactor))
			return &cost
		}
	case "join":
		if len(args) >= 1 {
			cost := uint64(math.Ceil(float64(actualSize(result)) * 2 * common.StringTraversalCostFactor))
			return &cost
		}
	case "find", "findAll":
		if len(args) >= 2 {
			strCost := uint64(math.Ceil((1.0 + float64(actualSize(args[0]))) * common.StringTraversalCostFactor))
			// 我们不知道正则里有多少个表达式，只知道字符串长度（这里有个巨大的
			// 改进方向：想办法统计正则中表达式数量或正则状态机的状态数，并用它来度量正则成本）。
			// 目前我们猜测正则中每个表达式通常至少 4 个字符长。
			regexCost := uint64(math.Ceil(float64(actualSize(args[1])) * common.RegexStringLengthCostFactor))
			cost := strCost * regexCost
			return &cost
		}
	case "cidr", "isIP", "isCIDR":
		// IP 和 CIDR 解析是一次字符串遍历。
		if len(args) >= 1 {
			cost := uint64(math.Ceil(float64(actualSize(args[0])) * common.StringTraversalCostFactor))
			return &cost
		}
	case "ip":
		// IP 和 CIDR 解析是一次字符串遍历。
		if len(args) >= 1 {
			if overloadId == "cidr_ip" {
				// CIDR 对象的 IP 成员只是访问一个字段。
				// 名义成本。
				cost := uint64(1)
				return &cost
			}

			cost := uint64(math.Ceil(float64(actualSize(args[0])) * common.StringTraversalCostFactor))
			return &cost
		}
	case "ip.isCanonical":
		if len(args) >= 1 {
			// 我们必须先解析字符串，再把解析结果与原字符串比较。
			// 因此解析字符串的成本翻倍。
			cost := uint64(math.Ceil(float64(actualSize(args[0])) * 2 * common.StringTraversalCostFactor))
			return &cost
		}
	case "masked", "prefixLength", "family", "isUnspecified", "isLoopback", "isLinkLocalMulticast", "isLinkLocalUnicast", "isGlobalUnicast":
		// IP 和 CIDR 访问器是名义成本。
		cost := uint64(1)
		return &cost
	case "containsIP":
		if len(args) >= 2 {
			cidrSize := actualSize(args[0])
			otherSize := actualSize(args[1])

			// 这是比较两个字节列表的基础成本。
			// 我们只比较到 CIDR 前缀长度的字节数，所以 cidrSize 计两次。
			cost := uint64(math.Ceil(float64(cidrSize+cidrSize) * common.StringTraversalCostFactor))

			if overloadId == "cidr_contains_ip_string" {
				// 如果比较对象是字符串，必须先把字符串解析成正确的类型，因此再加一次字符串遍历成本。
				cost += uint64(math.Ceil(float64(otherSize) * common.StringTraversalCostFactor))

			}

			return &cost
		}
	case "containsCIDR":
		if len(args) >= 2 {
			cidrSize := actualSize(args[0])
			otherSize := actualSize(args[1])

			// 这是比较两个字节列表的基础成本。
			// 我们只比较到 CIDR 前缀长度的字节数，所以 cidrSize 计两次。
			cost := uint64(math.Ceil(float64(cidrSize+cidrSize) * common.StringTraversalCostFactor))

			// 由于是比较一个 CIDR 是否落在另一个 CIDR 内，我们会先对基础 CIDR 做掩码，
			// 并比较 CIDR 位数。
			// 这额外包含一次 IP 长度遍历的成本，再加 1。
			cost += uint64(math.Ceil(float64(cidrSize)*common.StringTraversalCostFactor)) + 1

			if overloadId == "cidr_contains_cidr_string" {
				// 如果比较对象是字符串，必须先把字符串解析成正确的类型，因此再加一次字符串遍历成本。
				cost += uint64(math.Ceil(float64(otherSize) * common.StringTraversalCostFactor))
			}

			return &cost
		}
	case "quantity", "isQuantity", "semver", "isSemver":
		if len(args) >= 1 {
			cost := uint64(math.Ceil(float64(actualSize(args[0])) * common.StringTraversalCostFactor))
			return &cost
		}
	case "validate":
		if len(args) >= 2 {
			format, isFormat := args[0].Value().(cel.Format)
			if isFormat {
				strSize := actualSize(args[1])

				// 拿不到底层正则，按较长的正则估算
				regexSize := format.MaxRegexSize

				// 以下复制自 CEL 实现的正则成本计算
				//
				// https://swtch.com/~rsc/regexp/regexp1.html 适用于 CEL 支持的 RE2 实现
				// 计算成本时给字符串长度加 1，避免字符串为空但正则仍然很昂贵时，
				// 字符串与正则的乘积为 0。
				strCost := uint64(math.Ceil((1.0 + float64(strSize)) * common.StringTraversalCostFactor))
				// 我们不知道正则里有多少个表达式，只知道字符串长度（这里有个巨大的
				// 改进方向：想办法统计正则中表达式数量或正则状态机的状态数，并用它来度量正则成本）。
				// 目前我们猜测正则中每个表达式通常至少 4 个字符长。
				regexCost := uint64(math.Ceil(float64(regexSize) * common.RegexStringLengthCostFactor))
				cost := strCost * regexCost
				return &cost
			}
		}
	case "format.named":
		// 就是一次字典查找
		cost := uint64(1)
		return &cost
	case "sign", "asInteger", "isInteger", "asApproximateFloat", "isGreaterThan", "isLessThan", "compareTo", "add", "sub", "major", "minor", "patch":
		cost := uint64(1)
		return &cost
	case "getScheme", "getHostname", "getHost", "getPort", "getEscapedPath", "getQuery":
		// url 访问器
		cost := uint64(1)
		return &cost
	case "_==_":
		if len(args) == 2 {
			unitCost := uint64(1)
			lhs := args[0]
			switch lhs.(type) {
			case *cel.Quantity, cel.Quantity,
				*cel.IP, cel.IP,
				*cel.CIDR, cel.CIDR,
				*cel.Format, cel.Format, // Formats have a small max size. Format takes pointer receiver.
				*cel.URL, cel.URL, // TODO: Computing the actual cost is expensive, and changing this would be a breaking change
				*cel.Semver, cel.Semver,
				*authorizerVal, authorizerVal, *pathCheckVal, pathCheckVal, *groupCheckVal, groupCheckVal,
				*resourceCheckVal, resourceCheckVal, *decisionVal, decisionVal:
				return &unitCost
			default:
				if panicOnUnknown && lhs.Type() != nil && isRegisteredType(lhs.Type().TypeName()) {
					panic(fmt.Errorf("CallCost: unhandled equality for Kubernetes type %T", lhs))
				}
			}
		}
	}
	if panicOnUnknown && !knownUnhandledFunctions[function] {
		panic(fmt.Errorf("CallCost: unhandled function %q or args %v", function, args))
	}
	return nil
}

// EstimateCallCost 返回编译期对一次函数调用的成本上界。
// 与 CallCost 不同，这里只有 AST（没有实参），因此只能依赖类型与大小估算。
//
// 警告：任何改动这段代码都会影响 API 兼容性！估算成本用于决定哪些 CEL 规则可以写入 CRD，
// 成本的任何变化（升高或降低）都是 breaking 变更，因此任何调整都必须视为 API 变更，需要走 KEP/版本化流程。
func (l *CostEstimator) EstimateCallCost(function, overloadId string, target *checker.AstNode, args []checker.AstNode) *checker.CallEstimate {
	switch function {
	case "check":
		// 授权检查的成本是固定的
		// 该成本设定为每条表达式只允许两次授权检查
		return &checker.CallEstimate{CostEstimate: checker.CostEstimate{Min: 350000, Max: 350000}}
	case "serviceAccount", "path", "group", "resource", "subresource", "namespace", "name", "allowed", "reason", "error", "errored":
		// 所有授权构建与访问函数都是名义成本
		return &checker.CallEstimate{CostEstimate: checker.CostEstimate{Min: 1, Max: 1}}
	case "fieldSelector", "labelSelector":
		// fieldSelector 和 labelSelector 解析是把字符串解析成一组结构化要求
		if len(args) == 1 {
			return &checker.CallEstimate{CostEstimate: selectorCostEstimate(l.sizeEstimate(args[0]))}
		}
	case "isSorted", "sum", "max", "min", "indexOf", "lastIndexOf", "includes":
		if target != nil {
			// 列表中每个元素的比较计 1 成本
			elCost := checker.CostEstimate{Min: 1, Max: 1}
			// 如果列表包含字符串或字节，则加上遍历所有字符串/字节的成本，
			// 以此估算额外的比较成本。
			if elNode := l.listElementNode(*target); elNode != nil {
				k := elNode.Type().Kind()
				if k == types.StringKind || k == types.BytesKind {
					sz := l.sizeEstimate(elNode)
					elCost = elCost.Add(sz.MultiplyByCostFactor(common.StringTraversalCostFactor))
				}
				return &checker.CallEstimate{CostEstimate: l.sizeEstimate(*target).MultiplyByCost(elCost)}
			} else if function == "includes" {
				// 由于在 DynType 下 target 可能是列表，最坏情况是大小为 n 的列表比较，
				// 每次比较成本为 1。
				return &checker.CallEstimate{CostEstimate: l.sizeEstimate(*target).MultiplyByCost(elCost)}
			} else {
				// target 是字符串，indexOf 和 lastIndexOf 支持这种用法
				return &checker.CallEstimate{CostEstimate: l.sizeEstimate(*target).MultiplyByCostFactor(common.StringTraversalCostFactor)}
			}
		}
	case "url", "jsonpatch.escapeKey":
		if len(args) == 1 {
			sz := l.sizeEstimate(args[0])
			return &checker.CallEstimate{CostEstimate: sz.MultiplyByCostFactor(common.StringTraversalCostFactor), ResultSize: &sz}
		}
	case "lowerAscii", "upperAscii", "substring", "trim":
		if target != nil {
			sz := l.sizeEstimate(*target)
			return &checker.CallEstimate{CostEstimate: sz.MultiplyByCostFactor(common.StringTraversalCostFactor), ResultSize: &sz}
		}
	case "replace":
		if target != nil && len(args) >= 2 {
			sz := l.sizeEstimate(*target)
			toReplaceSz := l.sizeEstimate(args[0])
			replaceWithSz := l.sizeEstimate(args[1])

			var replaceCount, retainedSz checker.SizeEstimate
			// 找出最长的替换结果：
			if toReplaceSz.Min == 0 {
				// 如果被替换的字符串为空，replace 会用替换串包裹输入串中的每个字符。
				if sz.Max < math.MaxUint64 {
					replaceCount.Max = sz.Max + 1
				} else {
					replaceCount.Max = sz.Max
				}
				// 计入最长可能的原始字符串长度。
				retainedSz.Max = sz.Max
			} else if replaceWithSz.Max <= toReplaceSz.Min {
				// 如果替换不会让结果变长，就使用原字符串长度。
				replaceCount.Max = 0
				retainedSz.Max = sz.Max
			} else {
				// 用尽可能大的替换串替换尽可能小的子串，
				// 并替换尽可能多次。
				replaceCount.Max = uint64(math.Ceil(float64(sz.Max) / float64(toReplaceSz.Min)))
			}

			// 找出最短的替换结果：
			if toReplaceSz.Max == 0 {
				// 如果被替换的字符串为空，replace 会用替换串包裹输入串中的每个字符。
				if sz.Min < math.MaxUint64 {
					replaceCount.Min = sz.Min + 1
				} else {
					replaceCount.Min = sz.Min
				}
				// 计入最短可能的原始字符串长度。
				retainedSz.Min = sz.Min
			} else if toReplaceSz.Max <= replaceWithSz.Min {
				// 如果替换不会让结果变短，就使用原字符串长度。
				replaceCount.Min = 0
				retainedSz.Min = sz.Min
			} else {
				// 用尽可能小的替换串替换尽可能大的子串，
				// 并替换尽可能多次。
				replaceCount.Min = uint64(math.Ceil(float64(sz.Min) / float64(toReplaceSz.Max)))
			}
			size := replaceCount.Multiply(replaceWithSz).Add(retainedSz)

			// 成本是遍历加上构造结果
			return &checker.CallEstimate{CostEstimate: sz.MultiplyByCostFactor(2 * common.StringTraversalCostFactor), ResultSize: &size}
		}
	case "split":
		if target != nil {
			sz := l.sizeEstimate(*target)

			// 最坏情况是使用分隔符 ""，每个字符都作为列表元素返回。
			max := sz.Max
			if len(args) > 1 {
				if v := args[1].Expr().AsLiteral(); v != nil {
					if i, ok := v.Value().(int64); ok {
						max = uint64(i)
					}
				}
			}
			// 成本是遍历加上构造结果。
			return &checker.CallEstimate{CostEstimate: sz.MultiplyByCostFactor(2 * common.StringTraversalCostFactor), ResultSize: &checker.SizeEstimate{Min: 0, Max: max}}
		}
	case "join":
		if target != nil {
			var sz checker.SizeEstimate
			listSize := l.sizeEstimate(*target)
			if elNode := l.listElementNode(*target); elNode != nil {
				elemSize := l.sizeEstimate(elNode)
				sz = listSize.Multiply(elemSize)
			}

			if len(args) > 0 {
				sepSize := l.sizeEstimate(args[0])
				minSeparators := uint64(0)
				maxSeparators := uint64(0)
				if listSize.Min > 0 {
					minSeparators = listSize.Min - 1
				}
				if listSize.Max > 0 {
					maxSeparators = listSize.Max - 1
				}
				sz = sz.Add(sepSize.Multiply(checker.SizeEstimate{Min: minSeparators, Max: maxSeparators}))
			}

			return &checker.CallEstimate{CostEstimate: sz.MultiplyByCostFactor(common.StringTraversalCostFactor), ResultSize: &sz}
		}
	case "find", "findAll":
		if target != nil && len(args) >= 1 {
			sz := l.sizeEstimate(*target)
			// 计算成本时给字符串长度加 1，避免字符串为空但正则仍然很昂贵时，
			// 字符串与正则的乘积为 0。
			strCost := sz.Add(checker.SizeEstimate{Min: 1, Max: 1}).MultiplyByCostFactor(common.StringTraversalCostFactor)
			// 我们不知道正则里有多少个表达式，只知道字符串长度（这里有个巨大的
			// 改进方向：想办法统计正则中表达式数量或正则状态机的状态数，并用它来度量正则成本）。
			// 目前我们猜测正则中每个表达式通常至少 4 个字符长。
			regexCost := l.sizeEstimate(args[0]).MultiplyByCostFactor(common.RegexStringLengthCostFactor)
			// 结果的最坏情况是每个字符都作为单独的 find 结果返回。
			return &checker.CallEstimate{CostEstimate: strCost.Multiply(regexCost), ResultSize: &checker.SizeEstimate{Min: 0, Max: sz.Max}}
		}
	case "cidr", "isIP", "isCIDR":
		if len(args) >= 1 {
			sz := l.sizeEstimate(args[0])
			return &checker.CallEstimate{CostEstimate: sz.MultiplyByCostFactor(common.StringTraversalCostFactor)}
		}
	case "ip":
		if overloadId == "cidr_ip" {
			// CIDR 对象的 IP 成员只是访问一个字段。
			// 名义成本。
			return &checker.CallEstimate{CostEstimate: checker.CostEstimate{Min: 1, Max: 1}}
		}
		if len(args) >= 1 {
			sz := l.sizeEstimate(args[0])
			return &checker.CallEstimate{CostEstimate: sz.MultiplyByCostFactor(common.StringTraversalCostFactor)}
		}
	case "ip.isCanonical":
		if len(args) >= 1 {
			sz := l.sizeEstimate(args[0])
			// 我们必须先解析字符串，再把解析结果与原字符串比较。
			// 因此解析字符串的成本翻倍。
			return &checker.CallEstimate{CostEstimate: sz.MultiplyByCostFactor(2 * common.StringTraversalCostFactor)}
		}
	case "masked", "prefixLength", "family", "isUnspecified", "isLoopback", "isLinkLocalMulticast", "isLinkLocalUnicast", "isGlobalUnicast":
		// IP 和 CIDR 访问器是名义成本。
		return &checker.CallEstimate{CostEstimate: checker.CostEstimate{Min: 1, Max: 1}}
	case "containsIP":
		if len(args) >= 1 {
			// 该函数的基础成本是比较两个字节列表的成本。
			// 字节列表要么是 ipv4 要么是 ipv6，所以长度为 4 或 16 字节。
			sz := checker.SizeEstimate{Min: 4, Max: 16}

			// 必须比较这两个字符串，才能判断 CIDR/IP 是否属于另一个 CIDR。
			ipCompCost := sz.Add(sz).MultiplyByCostFactor(common.StringTraversalCostFactor)

			if overloadId == "cidr_contains_ip_string" {
				// 如果比较对象是字符串，必须先把字符串解析成正确的类型，因此再加一次字符串遍历成本。
				ipCompCost = ipCompCost.Add(checker.CostEstimate(l.sizeEstimate(args[0])).MultiplyByCostFactor(common.StringTraversalCostFactor))
			}

			return &checker.CallEstimate{CostEstimate: ipCompCost}
		}
	case "containsCIDR":
		if len(args) >= 1 {
			// 该函数的基础成本是比较两个字节列表的成本。
			// 字节列表要么是 ipv4 要么是 ipv6，所以长度为 4 或 16 字节。
			sz := checker.SizeEstimate{Min: 4, Max: 16}

			// 必须比较这两个字符串，才能判断 CIDR/IP 是否属于另一个 CIDR。
			ipCompCost := sz.Add(sz).MultiplyByCostFactor(common.StringTraversalCostFactor)

			// 由于是比较一个 CIDR 是否落在另一个 CIDR 内，我们会先对基础 CIDR 做掩码，
			// 并比较 CIDR 位数。
			// 这额外包含一次 IP 长度遍历的成本，再加 1。
			ipCompCost = ipCompCost.Add(sz.MultiplyByCostFactor(common.StringTraversalCostFactor))
			ipCompCost = ipCompCost.Add(checker.CostEstimate{Min: 1, Max: 1})

			if overloadId == "cidr_contains_cidr_string" {
				// 如果比较对象是字符串，必须先把字符串解析成正确的类型，因此再加一次字符串遍历成本。
				ipCompCost = ipCompCost.Add(checker.CostEstimate(l.sizeEstimate(args[0])).MultiplyByCostFactor(common.StringTraversalCostFactor))
			}

			return &checker.CallEstimate{CostEstimate: ipCompCost}
		}
	case "quantity", "isQuantity", "semver", "isSemver":
		if len(args) >= 1 {
			sz := l.sizeEstimate(args[0])
			return &checker.CallEstimate{CostEstimate: sz.MultiplyByCostFactor(common.StringTraversalCostFactor)}
		}
	case "validate":
		if len(args) >= 1 {
			sz := l.sizeEstimate(args[0])
			return &checker.CallEstimate{CostEstimate: sz.MultiplyByCostFactor(common.StringTraversalCostFactor).MultiplyByCostFactor(cel.MaxNameFormatRegexSize * common.RegexStringLengthCostFactor)}
		}
	case "format.named":
		return &checker.CallEstimate{CostEstimate: checker.CostEstimate{Min: 1, Max: 1}}
	case "sign", "asInteger", "isInteger", "asApproximateFloat", "isGreaterThan", "isLessThan", "compareTo", "add", "sub", "major", "minor", "patch":
		return &checker.CallEstimate{CostEstimate: checker.CostEstimate{Min: 1, Max: 1}}
	case "getScheme", "getHostname", "getHost", "getPort", "getEscapedPath", "getQuery":
		// url 访问器
		return &checker.CallEstimate{CostEstimate: checker.CostEstimate{Min: 1, Max: 1}}
	case "_==_":
		if len(args) == 2 {
			lhs := args[0]
			rhs := args[1]
			if lhs.Type().Equal(rhs.Type()) == types.True {
				t := lhs.Type()
				if t.Kind() == types.OpaqueKind {
					switch t.TypeName() {
					case cel.IPType.TypeName(), cel.CIDRType.TypeName():
						return &checker.CallEstimate{CostEstimate: checker.CostEstimate{Min: 1, Max: 1}}
					}
				}
				if t.Kind() == types.StructKind {
					switch t {
					case cel.QuantityType, AuthorizerType, PathCheckType, // 成本为 O(1) 的相等性检查
						GroupCheckType, ResourceCheckType, DecisionType, cel.SemverType:
						return &checker.CallEstimate{CostEstimate: checker.CostEstimate{Min: 1, Max: 1}}
					case cel.FormatType:
						return &checker.CallEstimate{CostEstimate: checker.CostEstimate{Min: 1, Max: cel.MaxFormatSize}.MultiplyByCostFactor(common.StringTraversalCostFactor)}
					case cel.URLType:
						size := checker.SizeEstimate{Min: 1, Max: 1}
						rhSize := rhs.ComputedSize()
						lhSize := rhs.ComputedSize()
						if rhSize != nil && lhSize != nil {
							size = rhSize.Union(*lhSize)
						}
						return &checker.CallEstimate{CostEstimate: checker.CostEstimate{Min: 1, Max: size.Max}.MultiplyByCostFactor(common.StringTraversalCostFactor)}
					}
				}
				if panicOnUnknown && isRegisteredType(t.TypeName()) {
					panic(fmt.Errorf("EstimateCallCost: unhandled equality for Kubernetes type %v", t))
				}
			}
		}
	}
	if panicOnUnknown && !knownUnhandledFunctions[function] {
		panic(fmt.Errorf("EstimateCallCost: unhandled function %q, target %v, args %v", function, target, args))
	}
	return nil
}

// actualSize 返回 CEL 值的实际大小（字符串/字节长度、列表/映射元素数）。
// 非 Sizer 类型按 1 计；测试模式下 panic，以便尽早发现未建模的类型。
func actualSize(value ref.Val) uint64 {
	if sz, ok := value.(traits.Sizer); ok {
		return uint64(sz.Size().(types.Int))
	}
	if panicOnUnknown {
		// debug.PrintStack()
		panic(fmt.Errorf("actualSize: non-sizer type %T", value))
	}
	return 1
}

// sizeEstimate 依次尝试 AST 上已计算的大小、注册的 SizeEstimator，
// 最后退化为 [0, MaxUint64] 表示“未知大小”的最坏情况。
func (l *CostEstimator) sizeEstimate(t checker.AstNode) checker.SizeEstimate {
	if sz := t.ComputedSize(); sz != nil {
		return *sz
	}
	if sz := l.EstimateSize(t); sz != nil {
		return *sz
	}
	return checker.SizeEstimate{Min: 0, Max: math.MaxUint64}
}

// listElementNode 为列表的元素构造一个虚拟 AST 节点。
// 若列表节点带有路径，则追加 "@items" 段，使 OpenAPI 的 maxLength 等约束
// 可以被元素类型估算器查到；没有路径时只能按类型给最坏估计。
func (l *CostEstimator) listElementNode(list checker.AstNode) checker.AstNode {
	if params := list.Type().Parameters(); len(params) > 0 {
		lt := params[0]
		nodePath := list.Path()
		if nodePath != nil {
			// 如果我们有路径就提供路径，这样就能查到该节点上（如果存在的）
			// OpenAPIv3 maxLength 校验。
			path := make([]string, len(nodePath)+1)
			copy(path, nodePath)
			path[len(nodePath)] = "@items"
			return &itemsNode{path: path, t: lt, expr: nil}
		} else {
			// 没有路径时只提供类型，以便基于类型查出最坏情况大小。
			return &itemsNode{t: lt, expr: nil}
		}
	}
	return nil
}

// EstimateSize 委托给可选的 SizeEstimator；未配置时返回 nil，
// 由 sizeEstimate 退化为未知大小的最坏估计。
func (l *CostEstimator) EstimateSize(element checker.AstNode) *checker.SizeEstimate {
	if l.SizeEstimator != nil {
		return l.SizeEstimator.EstimateSize(element)
	}
	return nil
}

// itemsNode 是元素的伪 AST 节点，只用于把“元素类型 + 元素路径”传给大小估算器。
type itemsNode struct {
	path []string
	t    *types.Type
	expr ast.Expr
}

func (i *itemsNode) Path() []string {
	return i.path
}

func (i *itemsNode) Type() *types.Type {
	return i.t
}

func (i *itemsNode) Expr() ast.Expr {
	return i.expr
}

func (i *itemsNode) ComputedSize() *checker.SizeEstimate {
	return nil
}

var _ checker.AstNode = (*itemsNode)(nil)

// traversalCost 计算把 ref.Val 作为数据树遍历一次的代价。
//
// 字符串/字节按长度计费；列表与映射递归累加，因此成本与嵌套数据量成正比，
// 这正是限制表达式遍历整个对象的目的。
func traversalCost(v ref.Val) uint64 {
	// TODO: 这里可以优化：对映射和列表做采样而不是完整遍历。
	switch vt := v.(type) {
	case types.String:
		return uint64(float64(len(string(vt))) * common.StringTraversalCostFactor)
	case types.Bytes:
		return uint64(float64(len([]byte(vt))) * common.StringTraversalCostFactor)
	case traits.Lister:
		cost := uint64(0)
		for it := vt.Iterator(); it.HasNext() == types.True; {
			i := it.Next()
			cost += traversalCost(i)
		}
		return cost
	case traits.Mapper: // 映射与对象
		cost := uint64(0)
		for it := vt.Iterator(); it.HasNext() == types.True; {
			k := it.Next()
			cost += traversalCost(k) + traversalCost(vt.Get(k))
		}
		return cost
	default:
		return 1
	}
}
