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

package routes

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"path"

	"k8s.io/apimachinery/pkg/util/wait"
)

// DebugSocket installs profiling and debugflag as a Unix-Domain socket.
//
// 使用 Unix socket 而非 TCP 端口，是为了不把 pprof 和调试开关暴露到网络上：
// 只有能访问该 socket 文件的本机进程才能调用，文件权限即是访问控制边界。
type DebugSocket struct {
	path string
	mux  *http.ServeMux
}

// NewDebugSocket creates a new DebugSocket for the given path.
func NewDebugSocket(path string) *DebugSocket {
	return &DebugSocket{
		path: path,
		mux:  http.NewServeMux(),
	}
}

// InstallProfiling installs profiling endpoints in the socket.
// 注册标准的 net/http/pprof 端点；先重定向无斜杠路径，保证两种写法都可用。
func (s *DebugSocket) InstallProfiling() {
	s.mux.HandleFunc("/debug/pprof", redirectTo("/debug/pprof/"))
	s.mux.HandleFunc("/debug/pprof/", pprof.Index)
	s.mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	s.mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	s.mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	s.mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
}

// InstallDebugFlag installs debug flag endpoints in the socket.
//
// 每个开关注册为 /debug/flags/<flag>，并加入索引页；同一 mux 可重复调用以挂载多个开关。
func (s *DebugSocket) InstallDebugFlag(flag string, handler func(http.ResponseWriter, *http.Request)) {
	f := DebugFlags{}
	s.mux.HandleFunc("/debug/flags", f.Index)
	s.mux.HandleFunc("/debug/flags/", f.Index)

	url := path.Join("/debug/flags", flag)
	s.mux.HandleFunc(url, handler)

	f.addFlag(flag)
}

// Run starts the server and waits for stopCh to be closed to close the server.
//
//logcheck:context // RunWithContext should be used instead of Run in code which supports contextual logging.
func (s *DebugSocket) Run(stopCh <-chan struct{}) error {
	return s.RunWithContext(wait.ContextForChannel(stopCh))
}

// RunWithContext starts the server and waits for the context to be cancelled to close the server.
func (s *DebugSocket) RunWithContext(ctx context.Context) error {
	// 先删除残留的 socket 文件：进程异常退出时文件会留下，不清理会导致 bind 失败。
	if err := os.Remove(s.path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove (%v): %v", s.path, err)
	}

	l, err := net.Listen("unix", s.path)
	if err != nil {
		return fmt.Errorf("listen error (%v): %v", s.path, err)
	}
	defer l.Close()

	srv := http.Server{Handler: s.mux}
	// 上下文取消后关闭服务器，使 Serve 返回；否则调试 socket 会阻止进程退出。
	go func() {
		<-ctx.Done()
		srv.Close()
	}()
	return srv.Serve(l)
}
