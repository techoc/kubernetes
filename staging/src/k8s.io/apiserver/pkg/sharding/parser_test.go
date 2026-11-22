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

package sharding

import (
	"testing"

	apisharding "k8s.io/apimachinery/pkg/sharding"
)

// TestParse 覆盖 shardRange 表达式的解析：合法输入产生有序的区间列表，
// 各类畸形输入必须报错。
//
// 解析器是安全边界的一部分：它只接受固定文法（单一函数、字符串十六进制字面量、
// 仅 || 连接），从而杜绝任意 CEL 表达式进入分片逻辑。
func TestParse(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
		wantReq []apisharding.ShardRangeRequirement
	}{
		{
			name:    "empty string",
			input:   "",
			wantErr: true,
		},
		{
			name:  "single requirement with uid",
			input: "shardRange(object.metadata.uid, '0x0000000000000000', '0x8000000000000000')",
			wantReq: []apisharding.ShardRangeRequirement{
				{Key: "object.metadata.uid", Start: "0x0000000000000000", End: "0x8000000000000000"},
			},
		},
		{
			name:  "full range with 2^64 end",
			input: "shardRange(object.metadata.uid, '0x0000000000000000', '0x10000000000000000')",
			wantReq: []apisharding.ShardRangeRequirement{
				{Key: "object.metadata.uid", Start: "0x0000000000000000", End: "0x10000000000000000"},
			},
		},
		{
			name:  "namespace field",
			input: "shardRange(object.metadata.namespace, '0x00000000000000aa', '0x00000000000000ff')",
			wantReq: []apisharding.ShardRangeRequirement{
				{Key: "object.metadata.namespace", Start: "0x00000000000000aa", End: "0x00000000000000ff"},
			},
		},
		{
			name:  "two requirements with ||",
			input: "shardRange(object.metadata.uid, '0x0000000000000000', '0x8000000000000000') || shardRange(object.metadata.uid, '0x8000000000000000', '0x10000000000000000')",
			wantReq: []apisharding.ShardRangeRequirement{
				{Key: "object.metadata.uid", Start: "0x0000000000000000", End: "0x8000000000000000"},
				{Key: "object.metadata.uid", Start: "0x8000000000000000", End: "0x10000000000000000"},
			},
		},
		{
			name:  "three requirements with ||",
			input: "shardRange(object.metadata.uid, '0x0000000000000000', '0x0000000000000005') || shardRange(object.metadata.uid, '0x0000000000000005', '0x000000000000000a') || shardRange(object.metadata.uid, '0x000000000000000a', '0x000000000000000f')",
			wantReq: []apisharding.ShardRangeRequirement{
				{Key: "object.metadata.uid", Start: "0x0000000000000000", End: "0x0000000000000005"},
				{Key: "object.metadata.uid", Start: "0x0000000000000005", End: "0x000000000000000a"},
				{Key: "object.metadata.uid", Start: "0x000000000000000a", End: "0x000000000000000f"},
			},
		},
		{
			name:  "no spaces",
			input: "shardRange(object.metadata.uid,'0x0000000000000000','0x8000000000000000')",
			wantReq: []apisharding.ShardRangeRequirement{
				{Key: "object.metadata.uid", Start: "0x0000000000000000", End: "0x8000000000000000"},
			},
		},
		{
			name:  "extra whitespace",
			input: "  shardRange( object.metadata.uid ,  '0x0000000000000000' ,  '0x8000000000000000' )  ",
			wantReq: []apisharding.ShardRangeRequirement{
				{Key: "object.metadata.uid", Start: "0x0000000000000000", End: "0x8000000000000000"},
			},
		},
		// Error cases
		//
		// 错误用例分四类：十六进制格式（缺 0x、空值、超长、非法字符、位数不足）、
		// 不支持的字段/函数、越界语法（&&、比较、嵌套调用、参数个数）、
		// 区间语义非法（起点等于或大于终点）。
		{
			name:    "missing 0x prefix on start",
			input:   "shardRange(object.metadata.uid, '0000000000000000', '0x8000000000000000')",
			wantErr: true,
		},
		{
			name:    "missing 0x prefix on end",
			input:   "shardRange(object.metadata.uid, '0x0000000000000000', '8000000000000000')",
			wantErr: true,
		},
		{
			name:    "empty after 0x prefix",
			input:   "shardRange(object.metadata.uid, '0x', '0x8000000000000000')",
			wantErr: true,
		},
		{
			name:    "name field unsupported",
			input:   "shardRange(object.metadata.name, '0x00', '0x80')",
			wantErr: true,
		},
		{
			name:    "unsupported field",
			input:   "shardRange(object.metadata.labels, '0x00', '0x80')",
			wantErr: true,
		},
		{
			name:    "unknown function",
			input:   "invalidFunc(object.metadata.uid, '0x00', '0x80')",
			wantErr: true,
		},
		{
			name:    "hex too long (18 chars)",
			input:   "shardRange(object.metadata.uid, '0x000000000000000000', '0x80')",
			wantErr: true,
		},
		{
			name:    "invalid hex char",
			input:   "shardRange(object.metadata.uid, '0x0g', '0x80')",
			wantErr: true,
		},
		{
			name:    "&& operator not allowed",
			input:   "shardRange(object.metadata.uid, '0x0', '0x8') && shardRange(object.metadata.uid, '0x8', '0xf')",
			wantErr: true,
		},
		{
			name:    "integer literal instead of string",
			input:   "shardRange(object.metadata.uid, 0, 8)",
			wantErr: true,
		},
		{
			name:    "comparison operator",
			input:   "object.metadata.uid > '0x0'",
			wantErr: true,
		},
		{
			name:    "wrong number of arguments",
			input:   "shardRange(object.metadata.uid, '0x0')",
			wantErr: true,
		},
		{
			name:    "nested function call",
			input:   "shardRange(object.metadata.uid, hex(0), '0x8')",
			wantErr: true,
		},
		{
			name:    "mixed field keys",
			input:   "shardRange(object.metadata.uid, '0x0000000000000000', '0x8000000000000000') || shardRange(object.metadata.namespace, '0x8000000000000000', '0x10000000000000000')",
			wantErr: true,
		},
		{
			name:    "short hex rejected",
			input:   "shardRange(object.metadata.uid, '0x0', '0x8000000000000000')",
			wantErr: true,
		},
		{
			name:    "short hex rejected on end",
			input:   "shardRange(object.metadata.uid, '0x0000000000000000', '0xff')",
			wantErr: true,
		},
		{
			name:    "start equals end",
			input:   "shardRange(object.metadata.uid, '0x8000000000000000', '0x8000000000000000')",
			wantErr: true,
		},
		{
			name:    "start greater than end",
			input:   "shardRange(object.metadata.uid, '0xffffffffffffffff', '0x0000000000000000')",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sel, err := Parse(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Errorf("Parse(%q) expected error, got nil", tt.input)
				}
				return
			}
			if err != nil {
				t.Fatalf("Parse(%q) unexpected error: %v", tt.input, err)
			}

			reqs := sel.Requirements()
			if len(reqs) != len(tt.wantReq) {
				t.Fatalf("Parse(%q) got %d requirements, want %d", tt.input, len(reqs), len(tt.wantReq))
			}
			for i, req := range reqs {
				if req != tt.wantReq[i] {
					t.Errorf("Parse(%q) requirement[%d] = %+v, want %+v", tt.input, i, req, tt.wantReq[i])
				}
			}
		})
	}
}

// TestParseRoundTrip 验证 Parse 与 String 互逆：规范输入经序列化后应逐字节相同，
// 且再次解析结果稳定。这保证配置比较与去重不会因格式漂移而产生误判。
func TestParseRoundTrip(t *testing.T) {
	tests := []string{
		"shardRange(object.metadata.uid, '0x0000000000000000', '0x8000000000000000')",
		"shardRange(object.metadata.uid, '0x8000000000000000', '0x10000000000000000')",
		"shardRange(object.metadata.uid, '0x0000000000000000', '0x10000000000000000')",
		"shardRange(object.metadata.namespace, '0x00000000000000aa', '0x00000000000000ff')",
		"shardRange(object.metadata.uid, '0x0000000000000000', '0x8000000000000000') || shardRange(object.metadata.uid, '0x8000000000000000', '0x10000000000000000')",
	}

	for _, input := range tests {
		t.Run(input, func(t *testing.T) {
			sel, err := Parse(input)
			if err != nil {
				t.Fatalf("Parse(%q) error: %v", input, err)
			}
			output := sel.String()
			if output != input {
				t.Errorf("Parse(%q).String() = %q, want %q", input, output, input)
			}
			// Round-trip — parse(string(parse(input))) should be stable.
			sel2, err := Parse(output)
			if err != nil {
				t.Fatalf("Parse(%q) (round-trip) error: %v", output, err)
			}
			if sel.String() != sel2.String() {
				t.Errorf("round-trip unstable: %q -> %q -> %q", input, output, sel2.String())
			}
		})
	}
}
