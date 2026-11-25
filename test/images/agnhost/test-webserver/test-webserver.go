/*
Copyright 2014 The Kubernetes Authors.

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

// Package testwebserver 提供一个提供静态文件服务的小型 Web 服务器。
package testwebserver

import (
	"fmt"
	"log"
	"net/http"

	"github.com/spf13/cobra"
)

// CmdTestWebserver 由 agnhost Cobra 使用。
var CmdTestWebserver = &cobra.Command{
	Use:   "test-webserver",
	Short: "Starts a simple HTTP fileserver",
	Long:  "Starts a simple HTTP fileserver on the given --port, which serves any file specified in the URL path, if it exists.",
	Args:  cobra.MaximumNArgs(0),
	Run:   main,
}

var (
	port int
)

func init() {
	CmdTestWebserver.Flags().IntVar(&port, "port", 80, "Port number.")
}

func main(cmd *cobra.Command, args []string) {
	fs := http.StripPrefix("/", http.FileServer(http.Dir("/")))

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "private")
		// 本地代理访问 Kubernetes API Server 时需要此配置。
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Credentials", "true")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "DNT,X-Mx-ReqToken,Keep-Alive,User-Agent,X-Requested-With,Cache-Control,Content-Type")
		// 禁用 If-Modified-Since，避免 304 响应破坏 update-demo
		r.Header.Del("If-Modified-Since")
		fs.ServeHTTP(w, r)
	})

	go log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", port), nil))

	select {}
}
