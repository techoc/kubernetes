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

package routes

import (
	"fmt"
	"html/template" //nolint:depguard
	"io"
	"net/http"
	"path"
	"sync"

	"k8s.io/klog/v2"

	"k8s.io/apiserver/pkg/server/mux"
)

var (
	// 注册表是进程级共享的：不同组件（apiserver、controller-manager）
	// 可能向各自路由注册同名 flag，需要统一展示。
	lock            = &sync.RWMutex{}
	registeredFlags = map[string]debugFlag{}
)

// DebugFlags 在 /debug/flags 下添加 flag 处理器。
//
// DebugFlags 安装 /debug/flags 索引页，并允许各组件按名称注册可在线
// 调整的 flag。索引页与具体 flag 路由都不列入根路径清单。
type DebugFlags struct {
}

// Install 注册 APIServer 的 flags 处理器。
//
// Install 把 /debug/flags 与 /debug/flags/<flag> 注册为未列出路由。
// 同时注册精确路径和前缀，使带尾斜杠的请求也能落到索引页。
func (f DebugFlags) Install(c *mux.PathRecorderMux, flag string, handler func(http.ResponseWriter, *http.Request)) {
	c.UnlistedHandle("/debug/flags", http.HandlerFunc(f.Index))
	c.UnlistedHandlePrefix("/debug/flags/", http.HandlerFunc(f.Index))

	url := path.Join("/debug/flags", flag)
	c.UnlistedHandleFunc(url, handler)

	f.addFlag(flag)
}

// Index 处理 `/debug/flags` 请求。
// 例如 "/debug/flags/v" 对应 "--v" flag。
// Index 对 "/debug/flags/" 的请求返回 HTML 页面，
// 列出所有可用 flag。
//
// Index 输出已注册 flag 的 HTML 列表；读锁保证枚举时不会与注册并发冲突。
func (f DebugFlags) Index(w http.ResponseWriter, r *http.Request) {
	lock.RLock()
	defer lock.RUnlock()
	if err := indexTmpl(w, registeredFlags); err != nil {
		klog.Error(err)
	}
}

// indexTmpl 直接拼接 HTML 而非使用 html/template，以保留编译器的死代码
// 消除能力（见 golang/go#72895）。所有动态内容都经过 HTMLEscapeString，
// 防止 flag 名中的特殊字符造成注入。
func indexTmpl(w io.Writer, flags map[string]debugFlag) error {
	// 不用模板，以免妨碍死代码消除
	// https://github.com/golang/go/issues/72895
	_, err := w.Write([]byte(`<html>
<head>
<title>/debug/flags/</title>
</head>
<body>
/debug/flags/<br>
<br>
flags:<br>
<table>
`))
	if err != nil {
		return err
	}

	for _, flag := range flags {
		_, err := fmt.Fprintf(w, `<tr>%s<br>
`, template.HTMLEscapeString(flag.Flag))
		if err != nil {
			return err
		}
	}

	_, err = w.Write([]byte(`</table>
<br>
full flags configurable<br>
</body>
</html>
`))
	return err
}

// debugFlag 是可在线修改的 flag 的最小描述；保留结构体而非裸字符串，
// 便于未来增加类型、当前值等字段而不破坏接口。
type debugFlag struct {
	Flag string
}

// addFlag 在写锁下登记 flag；重复登记覆盖旧值（展示内容相同）。
func (f DebugFlags) addFlag(flag string) {
	lock.Lock()
	defer lock.Unlock()
	registeredFlags[flag] = debugFlag{flag}
}

// StringFlagSetterFunc 用于设置 string 类型 flag。
type StringFlagSetterFunc func(string) (string, error)

// StringFlagPutHandler 包装 http Handler 以设置 string 类型 flag。
//
// StringFlagPutHandler 只接受 PUT：修改 flag 属于非幂等操作，要求显式
// 方法可以避免被链接预览、爬虫等 GET 请求意外触发。响应统一为纯文本，
// 并设置 nosniff，防止浏览器把错误内容当作 HTML 解析。
func StringFlagPutHandler(setter StringFlagSetterFunc) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch {
		case req.Method == "PUT":
			body, err := io.ReadAll(req.Body)
			if err != nil {
				writePlainText(http.StatusBadRequest, "error reading request body: "+err.Error(), w)
				return
			}
			defer req.Body.Close()
			// setter 负责校验新值是否合法；错误直接回传为 400。
			response, err := setter(string(body))
			if err != nil {
				writePlainText(http.StatusBadRequest, err.Error(), w)
				return
			}
			writePlainText(http.StatusOK, response, w)
			return
		default:
			writePlainText(http.StatusNotAcceptable, "unsupported http method", w)
			return
		}
	})
}

// writePlainText 输出简单的字符串响应。
//
// writePlainText 统一设置 Content-Type 与 nosniff，避免内容嗅探攻击。
func writePlainText(statusCode int, text string, w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/plain")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(statusCode)
	fmt.Fprintln(w, text)
}
