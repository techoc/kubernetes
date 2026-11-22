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

package admission

// Decorator 包装准入处理器，可用于在插件链之外叠加横切逻辑（如指标、
// 限流、审计）。它不改变请求的准入语义，只包裹执行。
type Decorator interface {
	Decorate(handler Interface, name string) Interface
}

// DecoratorFunc 让普通函数满足 Decorator 接口，便于在配置阶段内联装饰逻辑。
type DecoratorFunc func(handler Interface, name string) Interface

// Decorate 调用底层函数完成装饰。
func (d DecoratorFunc) Decorate(handler Interface, name string) Interface {
	return d(handler, name)
}

// Decorators 是可组合的装饰器列表，本身也实现 Decorator。
type Decorators []Decorator

// Decorate applies the decorator in inside-out order, i.e. the first decorator in the slice is first applied to the given handler.
// 按“由内向外”的顺序应用：切片中靠前的装饰器先包裹 handler，后续装饰器再包裹
// 前一步的结果。因此切片的第一个元素是最内层、最后执行，最后一个元素最外层、
// 最先看到请求；顺序会影响副作用发生的先后，配置时需留意。
func (d Decorators) Decorate(handler Interface, name string) Interface {
	result := handler
	for _, d := range d {
		result = d.Decorate(result, name)
	}

	return result
}
