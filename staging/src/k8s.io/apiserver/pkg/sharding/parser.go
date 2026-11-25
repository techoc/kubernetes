/*
Copyright The Kubernetes Authors.

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

// Package sharding 解析并校验 CEL 形式的分片选择器表达式（shardRange(...) 与 ||），
// 把文本转换为 apimachinery 的 sharding.Selector。
//
// 注意：这里的 CEL 只做语法解析、不做求值；解析出的 AST 被白名单式遍历，
// 任何超出 shardRange/|| 的语法都会被拒绝，从而避免把任意 CEL 带入服务端。
package sharding

import (
	"fmt"
	"strings"

	celparser "github.com/google/cel-go/parser"

	"github.com/google/cel-go/common"
	"github.com/google/cel-go/common/ast"
	"github.com/google/cel-go/common/operators"

	apisharding "k8s.io/apimachinery/pkg/sharding"
)

// Parse 把基于 CEL 的分片选择器表达式解析为 Selector。
//
// 表达式格式为：
//
//	shardRange(object.metadata.uid, '0x0', '0x8000000000000000')
//	shardRange(object.metadata.uid, '0x0', '0x8000000000000000') || shardRange(...)
//
// 仅允许 shardRange() 函数与 || 运算符。CEL 表达式只解析、绝不求值——
// 这里遍历 AST 以提取分片区间要求。
//
// 所有区间必须使用同一个字段，否则无法用单一分片键做一致性路由，直接报错。
func Parse(expr string) (apisharding.Selector, error) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil, fmt.Errorf("empty shard selector is not allowed; omit the parameter for unfiltered lists")
	}

	// 不注册任何宏，防止宏展开引入非预期的 AST 结构。
	p, err := celparser.NewParser(celparser.Macros( /* 不注册任何宏 */ ))
	if err != nil {
		return nil, fmt.Errorf("failed to create CEL parser: %w", err)
	}

	parsed, errs := p.Parse(common.NewTextSource(expr))
	// 只报告首个错误即可：后续错误通常是首个错误的级联。
	if errs != nil && len(errs.GetErrors()) > 0 {
		return nil, fmt.Errorf("CEL parse error: %s", errs.GetErrors()[0].Message)
	}

	reqs, err := walkExpr(parsed.Expr())
	if err != nil {
		return nil, err
	}

	// 校验所有要求使用同一字段键。
	for i := 1; i < len(reqs); i++ {
		if reqs[i].Key != reqs[0].Key {
			return nil, fmt.Errorf("all shard ranges must use the same field, got %q and %q", reqs[0].Key, reqs[i].Key)
		}
	}

	return apisharding.NewSelector(reqs...), nil
}

// walkExpr 递归遍历 CEL AST 并提取 ShardRangeRequirement。
// 根表达式必须是 shardRange() 调用，或由 || 运算符串联的若干 shardRange() 调用。
func walkExpr(e ast.Expr) ([]apisharding.ShardRangeRequirement, error) {
	if e.Kind() == ast.CallKind {
		call := e.AsCall()
		fn := call.FunctionName()

		if fn == operators.LogicalOr {
			// || 运算符：递归处理两侧操作数
			args := call.Args()
			if len(args) != 2 {
				return nil, fmt.Errorf("|| operator requires exactly 2 arguments")
			}
			left, err := walkExpr(args[0])
			if err != nil {
				return nil, err
			}
			right, err := walkExpr(args[1])
			if err != nil {
				return nil, err
			}
			return append(left, right...), nil
		}

		if fn == "shardRange" {
			req, err := parseShardRangeCall(call)
			if err != nil {
				return nil, err
			}
			return []apisharding.ShardRangeRequirement{req}, nil
		}

		// 白名单：其余任何函数（包括合法的 CEL 内置函数）一律拒绝。
		return nil, fmt.Errorf("unsupported function %q; only shardRange() and || are allowed", fn)
	}

	return nil, fmt.Errorf("unexpected expression kind %v; expected shardRange() call or || operator", e.Kind())
}

// parseShardRangeCall 从 shardRange(field, start, end) 调用中提取一个 ShardRangeRequirement。
func parseShardRangeCall(call ast.CallExpr) (apisharding.ShardRangeRequirement, error) {
	args := call.Args()
	if len(args) != 3 {
		return apisharding.ShardRangeRequirement{}, fmt.Errorf("shardRange() requires exactly 3 arguments, got %d", len(args))
	}

	// 参数 0：字段路径（形如 object.metadata.uid 的 select 链）
	fieldPath, err := extractFieldPath(args[0])
	if err != nil {
		return apisharding.ShardRangeRequirement{}, fmt.Errorf("shardRange() first argument: %w", err)
	}

	// 校验字段路径：
	// 只允许能稳定映射到分片键的字段，避免出现无法按对象定位的分片。
	switch fieldPath {
	case "object.metadata.uid", "object.metadata.namespace":
		// 命中受支持的分片键
	default:
		return apisharding.ShardRangeRequirement{}, fmt.Errorf("unsupported field path %q; supported: object.metadata.uid, object.metadata.namespace", fieldPath)
	}

	// 参数 1：十六进制区间起点（字符串字面量）
	hexStart, err := extractHexLiteral(args[1], "hexStart")
	if err != nil {
		return apisharding.ShardRangeRequirement{}, err
	}

	// 参数 2：十六进制区间终点（字符串字面量）
	hexEnd, err := extractHexLiteral(args[2], "hexEnd")
	if err != nil {
		return apisharding.ShardRangeRequirement{}, err
	}

	// 用规范的十六进制比较校验 start < end。
	if !apisharding.HexLess(hexStart, hexEnd) {
		return apisharding.ShardRangeRequirement{}, fmt.Errorf("shard range start %s must be less than end %s", hexStart, hexEnd)
	}

	return apisharding.ShardRangeRequirement{
		Key:   fieldPath,
		Start: hexStart,
		End:   hexEnd,
	}, nil
}

// extractFieldPath 遍历 select 链（object.metadata.uid）并返回以点连接的路径。
func extractFieldPath(e ast.Expr) (string, error) {
	switch e.Kind() {
	case ast.IdentKind:
		return e.AsIdent(), nil
	case ast.SelectKind:
		sel := e.AsSelect()
		operand, err := extractFieldPath(sel.Operand())
		if err != nil {
			return "", err
		}
		return operand + "." + sel.FieldName(), nil
	default:
		return "", fmt.Errorf("expected field path (e.g. object.metadata.uid), got expression kind %v", e.Kind())
	}
}

// extractHexLiteral 从 CEL 字符串字面量中提取十六进制字符串。
// 字面量必须是带 0x 前缀的单引号字符串，例如 '0xff'。
//
// 严格要求 16 位十六进制，使区间边界可以直接按无符号 64 位整数比较；
// 唯一例外是 2^64（17 位），用作开区间上界。
func extractHexLiteral(e ast.Expr, name string) (string, error) {
	if e.Kind() != ast.LiteralKind {
		return "", fmt.Errorf("%s must be a string literal (e.g. '0xff'), got expression kind %v", name, e.Kind())
	}

	val := e.AsLiteral()
	s, ok := val.Value().(string)
	if !ok {
		return "", fmt.Errorf("%s must be a string literal, got %T", name, val.Value())
	}

	if !strings.HasPrefix(s, "0x") {
		return "", fmt.Errorf("%s must have '0x' prefix, got %q", name, s)
	}

	hex := s[2:]
	if hex == "" {
		return "", fmt.Errorf("%s: hex value is required after '0x'", name)
	}
	if len(hex) > 17 {
		return "", fmt.Errorf("%s: hex value too long (%d chars, max 17): %q", name, len(hex), hex)
	}

	for _, c := range hex {
		// 只接受小写十六进制，保证同一数值只有一种规范写法。
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return "", fmt.Errorf("%s: invalid hex character %q in %q", name, string(c), s)
		}
	}

	// 必须恰好 16 位十六进制；唯一例外是开区间上界
	// "0x10000000000000000"（即 2^64）。
	if len(hex) == 17 {
		if s != "0x10000000000000000" {
			return "", fmt.Errorf("%s: 17-digit hex value must be '0x10000000000000000' (2^64), got %q", name, s)
		}
	} else if len(hex) != 16 {
		// 给出补零后的建议值，降低用户手写分片区间的出错率。
		padded := "0x" + strings.Repeat("0", 16-len(hex)) + hex
		return "", fmt.Errorf("%s must be a 0x-prefixed 16-digit hex value, got %q (did you mean %q?)", name, s, padded)
	}

	return s, nil
}
