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

package protobuf

import (
	"go/ast"
	"testing"
)

/*
	struct fields in go AST:

     type Struct struct {
		// // 有直接字段名（Ident）的字段：
        A X  // regular fields
        B *X // pointer fields
        C    // embedded type field

        // // 带包名的内嵌字段在 AST 中是 SelectorExpr
        v1.TypeMeta // X=v1, Sel=TypeMeta

        // // 没有直接名字、在 AST 中是 StarExpr 的字段：
        // a <StarExpr> in the go-AST
        *D   // type field embedded as pointer
        *v1.ListMeta   // qualified type field embedded as pointer
                       // with <StarExpr> pointing to <SelExpr>
     }
*/

// TestProtoParser 验证 getFieldName 能从各种合法的结构体字段表达式中取出字段名，
// 并对不支持的表达式返回错误。内嵌字段的 AST 形态见上方注释。
func TestProtoParser(t *testing.T) {
	ident := ast.NewIdent("FieldName")
	tests := []struct {
		expr ast.Expr
		err  bool
	}{
		// 合法结构体字段表达式：普通字段
		// // 合法结构体字段表达式：
		{
			expr: ident,
			err:  false,
		},
		{
			// 带包限定的内嵌字段，如 v1.TypeMeta。
			expr: &ast.SelectorExpr{
				Sel: ident,
			},
			err: false,
		},
		{
			// 指针字段，如 *X。
			expr: &ast.StarExpr{
				X: ident,
			},
			err: false,
		},
		{
			// 双重指针 **X。
			expr: &ast.StarExpr{
				X: &ast.StarExpr{
					X: ident,
				},
			},
			err: false,
		},
		{
			// 包限定指针内嵌字段，如 *v1.ListMeta。
			expr: &ast.StarExpr{
				X: &ast.SelectorExpr{
					Sel: ident,
				},
			},
			err: false,
		},

		// 其他表达式应报错
		// // 其他表达式应报错：
		{
			expr: &ast.KeyValueExpr{
				Key:   ident,
				Colon: 0,
				Value: ident,
			},
			err: true,
		},
		{
			expr: &ast.StarExpr{
				X: &ast.KeyValueExpr{
					Key:   ident,
					Colon: 0,
					Value: ident,
				},
			},
			err: true,
		},
	}

	for _, test := range tests {
		actual, err := getFieldName(test.expr, "Struct")
		if !test.err {
			if err != nil {
				t.Errorf("%s: unexpected error %s", test.expr, err)
			} else if actual != ident.Name {
				t.Errorf("%s: expected %s, got %s", test.expr, ident.Name, actual)
			}
		} else {
			if err == nil {
				t.Errorf("%s: expected error did not occur, got %s instead", test.expr, actual)
			}
		}
	}
}
