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

// Package helper 提供调度插件共用的辅助工具。
package helper

// FunctionShape 表示一组 FunctionShapePoint，用折线描述函数形状。
type FunctionShape []FunctionShapePoint

// FunctionShapePoint 表示折线上的一个形状点。
type FunctionShapePoint struct {
	// Utilization 是函数的自变量（如资源利用率百分比）。
	Utilization int64
	// Score 是函数在该点的取值。
	Score int64
}

// BuildBrokenLinearFunction 根据 shape 数组定义的线段构造一个分段线性函数。
// shape[i].Utilization 是各线段在"利用率"轴上的交点坐标，
// shape[i].Score 是函数在这些交点处的取值。
//
// 函数 f(p) 定义如下：
//
//	p < shape[0].Utilization 时取 shape[0].Score
//	p > shape[n-1].Utilization 时取 shape[n-1].Score
//
// 各交点之间（p < shape[i].Utilization）按线性插值计算。
// 两端使用常数延拓（clamp），避免超出配置范围时得分失控。
func BuildBrokenLinearFunction(shape FunctionShape) func(int64) int64 {
	return func(p int64) int64 {
		for i := 0; i < len(shape); i++ {
			// 找到第一个交点大于等于 p 的线段，在其左端点与当前点之间插值。
			if p <= int64(shape[i].Utilization) {
				if i == 0 {
					// p 落在最左交点之前，直接取首个得分，防止外插。
					return shape[0].Score
				}
				return shape[i-1].Score + (shape[i].Score-shape[i-1].Score)*(p-shape[i-1].Utilization)/(shape[i].Utilization-shape[i-1].Utilization)
			}
		}
		// p 落在最右交点之后，取末个得分。
		return shape[len(shape)-1].Score
	}
}
