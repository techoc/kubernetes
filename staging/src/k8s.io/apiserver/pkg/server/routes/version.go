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

	"github.com/emicklei/go-restful/v3"

	"k8s.io/apimachinery/pkg/version"
	"k8s.io/apiserver/pkg/endpoints/handlers/responsewriters"
)

// Version provides a webservice with version information.
// Version 是可选的 /version 路由配置；Version 为 nil 时 Install 不注册任何
// 路由，便于测试或裁剪端点。
type Version struct {
	Version *version.Info
}

// Install registers the APIServer's `/version` handler.
func (v Version) Install(c *restful.Container) {
	if v.Version == nil {
		return
	}

	// Set up a service to return the git code version.
	// 单独的 WebService 挂载在根路径 /version 下，不参与 /api 的组版本协商。
	versionWS := new(restful.WebService)
	versionWS.Path("/version")
	versionWS.Doc("get the version information for this server.")
	versionWS.Route(
		versionWS.GET("/").To(v.handleVersion).
			Doc("get the version information for this server").
			Operation("getCodeVersion").
			Produces(restful.MIME_JSON).
			Consumes(restful.MIME_JSON).
			Writes(version.Info{}))

	c.Add(versionWS)
}

// handleVersion writes the server's version information.
// 直接写原始 JSON 而不是走协商/序列化链路，保证 /version 始终返回同一形态，
// 不受请求 Accept 头影响。
func (v Version) handleVersion(req *restful.Request, resp *restful.Response) {
	responsewriters.WriteRawJSON(http.StatusOK, *v.Version, resp.ResponseWriter)
}
