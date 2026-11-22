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

package app

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

// WaitForAPIServer waits for the API Server's /healthz endpoint to report "ok" with timeout.
// WaitForAPIServer 在 timeout 时间内反复探测 apiserver 的 /healthz 端点，
// 直到返回 HTTP 200（"ok"）才认为 apiserver 就绪。
// controller-manager 在启动控制器之前调用它，以免 apiserver 尚未启动时产生大量无意义报错。
func WaitForAPIServer(client clientset.Interface, timeout time.Duration) error {
	var lastErr error
	// 保存最近一次失败原因，超时后一并写入返回的错误，便于定位问题。

	// 先立即探测一次，失败则每秒重试，直到成功或超过 timeout。
	err := wait.PollImmediate(time.Second, timeout, func() (bool, error) {
		healthStatus := 0
		// 用局部变量接收 HTTP 响应状态码。
		// 直接对绝对路径 /healthz 发起 GET；不经过任何具体资源接口。
		result := client.Discovery().RESTClient().Get().AbsPath("/healthz").Do(context.TODO()).StatusCode(&healthStatus)
		if result.Error() != nil {
			// 请求本身失败（网络不通、被拒绝等）：记录错误并继续重试，不终止轮询。
			lastErr = fmt.Errorf("failed to get apiserver /healthz status: %v", result.Error())
			return false, nil
		}
		if healthStatus != http.StatusOK {
			// 收到了响应但状态码不是 200，说明 apiserver 还没有真正健康。
			// 取出响应体，放进错误和日志里，方便查看 apiserver 具体报告了什么。
			content, _ := result.Raw()
			lastErr = fmt.Errorf("APIServer isn't healthy: %v", string(content))
			klog.Warningf("APIServer isn't healthy yet: %v. Waiting a little while.", string(content))
			return false, nil
		}

		// 200 表示健康，条件满足，停止轮询。
		return true, nil
	})

	if err != nil {
		// 轮询超时（或探测函数返回错误）：把 wait 的错误与最后一次失败原因组合后返回。
		return fmt.Errorf("%v: %v", err, lastErr)
	}

	return nil
}

// IsControllerEnabled check if a specified controller enabled or not.
// IsControllerEnabled 判断名为 name 的控制器是否应当启用。
// controllers 来自 --controllers 参数，支持三种写法：
//
//	"foo"  显式启用该控制器；
//	"-foo" 显式禁用该控制器；
//	"*"    启用所有“默认启用”的控制器。
//
// 显式规则优先于通配符；若没有任何显式规则且没有 "*"，则默认不启用任何控制器。
func IsControllerEnabled(name string, disabledByDefaultControllers sets.String, controllers []string) bool {
	hasStar := false
	// 记录用户是否指定了通配符 "*"。
	// 按配置顺序扫描，命中第一条针对 name 的显式规则就立即返回。
	for _, ctrl := range controllers {
		if ctrl == name {
			// 名字完全匹配：显式启用。
			return true
		}
		if ctrl == "-"+name {
			// 名字前带 "-"：显式禁用。
			return false
		}
		if ctrl == "*" {
			// 通配符出现只做标记，最终结果还要结合默认禁用集合判断。
			hasStar = true
		}
	}
	// if we get here, there was no explicit choice
	// 能执行到这里说明没有找到针对该控制器的显式规则。
	if !hasStar {
		// 没有通配符：不在显式名单里的控制器默认全部关闭。
		// nothing on by default
		return false
	}

	// 有通配符：默认开启，但“默认禁用”集合中的控制器除外。
	return !disabledByDefaultControllers.Has(name)
}
