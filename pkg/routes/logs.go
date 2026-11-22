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

package routes

import (
	"net/http"
	"os"
	"path"

	"github.com/emicklei/go-restful/v3"
)

// Logs 提供 /logs 路径下的日志文件服务，从节点的 /var/log 目录读取并返回日志文件。
// 主要用于 API Server 所在节点的日志访问，方便调试和排查问题。
type Logs struct{}

// Install 将日志文件服务路由注册到 restful.Container 中。
// 注册两个路由：
//
//   - GET /logs/{logpath:*} — 获取指定日志文件的内容（单文件）
//   - GET /logs/ — 列出 /var/log 目录下的所有日志文件（目录列表）
func (l Logs) Install(c *restful.Container) {
	ws := new(restful.WebService)
	ws.Path("/logs")
	ws.Doc("get log files")
	// 通配符 {logpath:*} 匹配任意深度的路径，如 /logs/kube-apiserver/kube-apiserver.log
	ws.Route(ws.GET("/{logpath:*}").To(logFileHandler).Param(ws.PathParameter("logpath", "path to the log").DataType("string")))
	// 访问 /logs/ 根路径时返回目录列表
	ws.Route(ws.GET("/").To(logFileListHandler))

	c.Add(ws)
}

// logFileHandler 处理单个日志文件的读取请求。
// 从 URL 路径参数中提取日志文件路径，拼接 /var/log 前缀后返回文件内容。
func logFileHandler(req *restful.Request, resp *restful.Response) {
	logdir := "/var/log"
	actual := path.Join(logdir, req.PathParameter("logpath"))

	// 先检查文件名长度，避免 http.ServeFile 对超长文件名返回 500 而不是 404。
	// 超长文件名不是服务器内部错误，应返回 404。
	if logFileNameIsTooLong(actual) {
		http.Error(resp, "file not found", http.StatusNotFound)
		return
	}
	http.ServeFile(resp.ResponseWriter, req.Request, actual)
}

// logFileListHandler 处理 /logs/ 根路径的请求，返回 /var/log 目录的文件列表。
// 浏览器访问时会以目录索引页面形式展示所有日志文件。
func logFileListHandler(req *restful.Request, resp *restful.Response) {
	logdir := "/var/log"
	http.ServeFile(resp.ResponseWriter, req.Request, logdir)
}

// logFileNameIsTooLong 检查文件路径是否超过系统限制（通常 255 字符）。
// http.ServeFile 对 NotExist 和 Forbidden 返回 404/403，但其他错误统一返回 500。
// 文件名过长属于 ENAMETOOLONG 错误，需要区分出真正的 500 和超长文件名，返回 404 而非 500。
func logFileNameIsTooLong(filePath string) bool {
	_, err := os.Stat(filePath)
	if err != nil {
		if e, ok := err.(*os.PathError); ok && e.Err == fileNameTooLong {
			return true
		}
	}
	return false
}
