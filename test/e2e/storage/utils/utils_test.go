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

package utils

import (
	"testing"

	e2evolume "k8s.io/kubernetes/test/e2e/framework/volume"
)

// getSizeRangesIntersection 接收两个存储大小范围，求其交集（若存在），
// 并返回交集的最小值作为测试用的 PVC 申请大小。
// 未设置的值表示没有最小或最大限制，此时使用默认值。
// 需考虑如下所有边界情况：
// 第一个范围：A、B 为常规值，? 表示未指定
// 第二个范围：C、D 为常规值，? 表示未指定
// ----------------------------------------------------------------
// |    \second| min=C,max=?| min=C,max=D| min=?,max=D| min=?,max=?|
// |first\     |            |            |            |            |
// |----------------------------------------------------------------
// |min=A,max=?|    #1      |     #2     |     #3     |    #4      |
// -----------------------------------------------------------------
// |min=A,max=B|    #5      |     #6     |     #7     |    #8      |
// -----------------------------------------------------------------
// |min=?,max=B|    #9      |     #10    |     #11    |    #12     |
// -----------------------------------------------------------------
// |min=?,max=?|    #13     |     #14    |     #15    |    #16     |
// |---------------------------------------------------------------|
func Test_GetSizeRangesIntersection(t *testing.T) {
	type args struct {
		first  e2evolume.SizeRange
		second e2evolume.SizeRange
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "case #1: first{min=A,max=?} second{min=C,max=?} where C > A ",
			args: args{
				first: e2evolume.SizeRange{
					Min: "5Gi",
				},
				second: e2evolume.SizeRange{
					Min: "10Gi",
				},
			},
			want:    "10Gi",
			wantErr: false,
		},
		{
			name: "case #1: first{min=A,max=?} second{min=C,max=?} where C < A ",
			args: args{
				first: e2evolume.SizeRange{
					Min: "5Gi",
				},
				second: e2evolume.SizeRange{
					Min: "1Gi",
				},
			},
			want:    "5Gi",
			wantErr: false,
		},
		{
			name: "case #2: first{min=A,max=?} second{min=C,max=D} where A > D ",
			args: args{
				first: e2evolume.SizeRange{
					Min: "5Gi",
				},
				second: e2evolume.SizeRange{
					Min: "1Gi",
					Max: "4Gi",
				},
			},
			want:    "",
			wantErr: true,
		},
		{
			name: "case #2: first{min=A,max=?} second{min=C,max=D} where D > A > C ",
			args: args{
				first: e2evolume.SizeRange{
					Min: "5Gi",
					Max: "",
				},
				second: e2evolume.SizeRange{
					Min: "3Gi",
					Max: "10Gi",
				},
			},
			want:    "5Gi",
			wantErr: false,
		},
		{
			name: "case #2: first{min=A,max=?} second{min=C,max=D} where A < C ",
			args: args{
				first: e2evolume.SizeRange{
					Min: "5Gi",
					Max: "",
				},
				second: e2evolume.SizeRange{
					Min: "6Gi",
					Max: "10Gi",
				},
			},
			want:    "6Gi",
			wantErr: false,
		},
		{
			name: "case #3: first{min=A,max=?} second{min=?,max=D} where A > D",
			args: args{
				first: e2evolume.SizeRange{
					Min: "5Gi",
					Max: "",
				},
				second: e2evolume.SizeRange{
					Max: "1Gi",
				},
			},
			want:    "",
			wantErr: true,
		},
		{
			name: "case #3: first{min=A,max=?} second{min=?,max=D} where A < D",
			args: args{
				first: e2evolume.SizeRange{
					Min: "5Gi",
					Max: "",
				},
				second: e2evolume.SizeRange{
					Max: "10Gi",
				},
			},
			want:    "5Gi",
			wantErr: false,
		},
		{
			name: "case #4: first{min=A,max=?} second{min=?,max=?} ",
			args: args{
				first: e2evolume.SizeRange{
					Min: "5Gi",
					Max: "",
				},
				second: e2evolume.SizeRange{},
			},
			want:    "5Gi",
			wantErr: false,
		},

		{
			name: "case #5: first{min=A,max=B} second{min=C,max=?} where C < A ",
			args: args{
				first: e2evolume.SizeRange{
					Min: "5Gi",
					Max: "10Gi",
				},
				second: e2evolume.SizeRange{
					Min: "1Gi",
				},
			},
			want:    "5Gi",
			wantErr: false,
		},
		{
			name: "case #5: first{min=A,max=B} second{min=C,max=?} where B > C > A ",
			args: args{
				first: e2evolume.SizeRange{
					Min: "5Gi",
					Max: "10Gi",
				},
				second: e2evolume.SizeRange{
					Min: "6Gi",
				},
			},
			want:    "6Gi",
			wantErr: false,
		},
		{
			name: "case #5: first{min=A,max=B} second{min=C,max=?} where C > B ",
			args: args{
				first: e2evolume.SizeRange{
					Min: "5Gi",
					Max: "10Gi",
				},
				second: e2evolume.SizeRange{
					Min: "15Gi",
				},
			},
			want:    "",
			wantErr: true,
		},
		{
			name: "case #6: first{min=A,max=B} second{min=C,max=D} where A < B < C < D",
			args: args{
				first: e2evolume.SizeRange{
					Min: "5Gi",
					Max: "6Gi",
				},
				second: e2evolume.SizeRange{
					Min: "7Gi",
					Max: "8Gi",
				},
			},
			want:    "",
			wantErr: true,
		},
		{
			name: "case #6: first{min=A,max=B} second{min=C,max=D} where A < C < B < D ",
			args: args{
				first: e2evolume.SizeRange{
					Min: "5Gi",
					Max: "10Gi",
				},
				second: e2evolume.SizeRange{
					Min: "8Gi",
					Max: "15Gi",
				},
			},
			want:    "8Gi",
			wantErr: false,
		},
		{
			name: "case #7: first{min=A,max=B} second{min=?,max=D} where D < A",
			args: args{
				first: e2evolume.SizeRange{
					Min: "5Gi",
					Max: "10Gi",
				},
				second: e2evolume.SizeRange{
					Max: "3Gi",
				},
			},
			want:    "",
			wantErr: true,
		},
		{
			name: "case #7: first{min=A,max=B} second{min=?,max=D} where B > D > A",
			args: args{
				first: e2evolume.SizeRange{
					Min: "5Gi",
					Max: "10Gi",
				},
				second: e2evolume.SizeRange{
					Max: "8Gi",
				},
			},
			want:    "5Gi",
			wantErr: false,
		},
		{
			name: "case #7: first{min=A,max=B} second{min=?,max=D} where D > B",
			args: args{
				first: e2evolume.SizeRange{
					Min: "5Gi",
					Max: "10Gi",
				},
				second: e2evolume.SizeRange{
					Max: "15Gi",
				},
			},
			want:    "5Gi",
			wantErr: false,
		},
		{
			name: "case #8: first{min=A,max=B} second{min=?,max=?}",
			args: args{
				first: e2evolume.SizeRange{
					Min: "5Gi",
					Max: "10Gi",
				},
				second: e2evolume.SizeRange{},
			},
			want:    "5Gi",
			wantErr: false,
		},
		{
			name: "case #9: first{min=?,max=B} second{min=C,max=?} where C > B",
			args: args{
				first: e2evolume.SizeRange{
					Max: "5Gi",
				},
				second: e2evolume.SizeRange{
					Min: "10Gi",
				},
			},
			want:    "",
			wantErr: true,
		},
		{
			name: "case #9: first{min=?,max=B} second{min=C,max=?} where C < B",
			args: args{
				first: e2evolume.SizeRange{
					Max: "10Gi",
				},
				second: e2evolume.SizeRange{
					Min: "5Gi",
				},
			},
			want:    "5Gi",
			wantErr: false,
		},
		{
			name: "case #10: first{min=?,max=B} second{min=C,max=D} where B > D",
			args: args{
				first: e2evolume.SizeRange{
					Max: "10Gi",
				},
				second: e2evolume.SizeRange{
					Min: "1Gi",
					Max: "5Gi",
				},
			},
			want:    "1Gi",
			wantErr: false,
		},
		{
			name: "case #10: first{min=?,max=B} second{min=C,max=D} where C < B < D",
			args: args{
				first: e2evolume.SizeRange{
					Max: "10Gi",
				},
				second: e2evolume.SizeRange{
					Min: "5Gi",
					Max: "15Gi",
				},
			},
			want:    "5Gi",
			wantErr: false,
		},
		{
			name: "case #10: first{min=?,max=B} second{min=C,max=D} where B < C",
			args: args{
				first: e2evolume.SizeRange{
					Max: "10Gi",
				},
				second: e2evolume.SizeRange{
					Min: "15Gi",
					Max: "20Gi",
				},
			},
			want:    "",
			wantErr: true,
		},
		{
			name: "case #11: first{min=?,max=B} second{min=?,max=D} where D < B",
			args: args{
				first: e2evolume.SizeRange{
					Max: "10Gi",
				},
				second: e2evolume.SizeRange{
					Max: "5Gi",
				},
			},
			want:    minValidSize,
			wantErr: false,
		},
		{
			name: "case #11: first{min=?,max=B} second{min=?,max=D} where D > B",
			args: args{
				first: e2evolume.SizeRange{
					Max: "10Gi",
				},
				second: e2evolume.SizeRange{
					Max: "15Gi",
				},
			},
			want:    minValidSize,
			wantErr: false,
		},
		{
			name: "case #12: first{min=?,max=B} second{min=?,max=?} ",
			args: args{
				first: e2evolume.SizeRange{
					Max: "10Gi",
				},
				second: e2evolume.SizeRange{},
			},
			want:    minValidSize,
			wantErr: false,
		},
		{
			name: "case #13: first{min=?,max=?} second{min=C,max=?} ",
			args: args{
				first: e2evolume.SizeRange{},
				second: e2evolume.SizeRange{
					Min: "5Gi",
				},
			},
			want:    "5Gi",
			wantErr: false,
		},
		{
			name: "case #14: first{min=?,max=?} second{min=C,max=D} where C < D",
			args: args{
				first: e2evolume.SizeRange{},
				second: e2evolume.SizeRange{
					Min: "5Gi",
					Max: "10Gi",
				},
			},
			want:    "5Gi",
			wantErr: false,
		},
		{
			name: "case #14: first{min=?,max=?} second{min=C,max=D} where C > D",
			args: args{
				first: e2evolume.SizeRange{},
				second: e2evolume.SizeRange{
					Min: "10Gi",
					Max: "5Gi",
				},
			},
			want:    "",
			wantErr: true,
		},
		{
			name: "case #14: first{min=?,max=?} second{min=C,max=D} where C = D",
			args: args{
				first: e2evolume.SizeRange{},
				second: e2evolume.SizeRange{
					Min: "1Mi",
					Max: "1Mi",
				},
			},
			want:    "1Mi",
			wantErr: false,
		},
		{
			name: "case #15: first{min=?,max=?} second{min=?,max=D}",
			args: args{
				first: e2evolume.SizeRange{},
				second: e2evolume.SizeRange{
					Max: "10Gi",
				},
			},
			want:    minValidSize,
			wantErr: false,
		},
		{
			name: "case #16: first{min=?,max=?} second{min=?,max=?}",
			args: args{
				first:  e2evolume.SizeRange{},
				second: e2evolume.SizeRange{},
			},
			want:    minValidSize,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		got, err := GetSizeRangesIntersection(tt.args.first, tt.args.second)
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. GetSizeRangesIntersection() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
		if got != tt.want {
			t.Errorf("%q. GetSizeRangesIntersection() = %v, want %v", tt.name, got, tt.want)
		}
	}
}
