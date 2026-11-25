/*
Copyright 2017 The Kubernetes Authors.

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

package discovery

import (
	"context"
	"net/http"
	"sync"

	restful "github.com/emicklei/go-restful/v3"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/apiserver/pkg/endpoints/handlers/negotiation"
	"k8s.io/apiserver/pkg/endpoints/handlers/responsewriters"
)

// GroupManager 是一个接口，允许在运行时动态增删 API 组并同步修改已有的 WebService。
type GroupManager interface {
	GroupLister

	AddGroup(apiGroup metav1.APIGroup)
	RemoveGroup(groupName string)
	ServeHTTP(resp http.ResponseWriter, req *http.Request)
	WebService() *restful.WebService
}

// GroupLister 提供发现所需的 APIGroup 列表。
type GroupLister interface {
	// Groups 返回用于发现的 APIGroup 列表，并根据 req 中的信息填充
	// ServerAddressByClientCIDRs。
	Groups(ctx context.Context, req *http.Request) ([]metav1.APIGroup, error)
}

// rootAPIsHandler 创建一个 WebService，服务于 API 组发现。服务器运行期间，
// API 组列表会因资源的注册或移除而变化，缓存这些值并不安全。
//
// 因此读路径统一加 RLock，写路径加 Lock；apiGroupNames 单独维护插入顺序，
// 因为 map 迭代顺序不稳定，而发现输出需要可预测的顺序。
type rootAPIsHandler struct {
	// addresses 用于在发现响应中构造集群 IP
	addresses Addresses

	serializer runtime.NegotiatedSerializer

	// 记录所有将在发现响应中暴露的组，键为组名、值为对应的 APIGroup
	lock      sync.RWMutex
	apiGroups map[string]metav1.APIGroup
	// apiGroupNames 保持组的插入顺序
	apiGroupNames []string
}

// NewRootAPIsHandler 构造 /apis 发现处理器，并沿用旧版空 APIVersion 的兼容序列化。
func NewRootAPIsHandler(addresses Addresses, serializer runtime.NegotiatedSerializer) *rootAPIsHandler {
	// 1.1 版本中 /apis 返回的响应里 APIVersion 为空，这里用
	// stripVersionNegotiatedSerializer 保持响应向后兼容。
	serializer = stripVersionNegotiatedSerializer{serializer}

	return &rootAPIsHandler{
		addresses:  addresses,
		serializer: serializer,
		apiGroups:  map[string]metav1.APIGroup{},
	}
}

// AddGroup 新增或更新一个 API 组；已存在时只覆盖内容，不重复追加顺序项。
func (s *rootAPIsHandler) AddGroup(apiGroup metav1.APIGroup) {
	s.lock.Lock()
	defer s.lock.Unlock()

	_, alreadyExists := s.apiGroups[apiGroup.Name]

	s.apiGroups[apiGroup.Name] = apiGroup
	if !alreadyExists {
		s.apiGroupNames = append(s.apiGroupNames, apiGroup.Name)
	}
}

// RemoveGroup 删除 API 组，并同步从顺序列表中移除，保持两个结构一致。
func (s *rootAPIsHandler) RemoveGroup(groupName string) {
	s.lock.Lock()
	defer s.lock.Unlock()

	delete(s.apiGroups, groupName)
	for i := range s.apiGroupNames {
		if s.apiGroupNames[i] == groupName {
			s.apiGroupNames = append(s.apiGroupNames[:i], s.apiGroupNames[i+1:]...)
			break
		}
	}
}

// Groups 返回当前所有 API 组的发现信息；结果按插入顺序排列。
func (s *rootAPIsHandler) Groups(ctx context.Context, req *http.Request) ([]metav1.APIGroup, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	return s.groupsLocked(ctx, req), nil
}

// groupsLocked 返回该处理器的 APIGroupList 发现信息。
// 调用方必须在调用本方法前持有锁，以避免数据竞争。
func (s *rootAPIsHandler) groupsLocked(ctx context.Context, req *http.Request) []metav1.APIGroup {
	clientIP := utilnet.GetClientIP(req)
	serverCIDR := s.addresses.ServerAddressByClientCIDRs(clientIP)

	orderedGroups := []metav1.APIGroup{}
	for _, groupName := range s.apiGroupNames {
		orderedGroups = append(orderedGroups, s.apiGroups[groupName])
	}

	// 复制一份再写入 ServerAddressByClientCIDRs，避免修改缓存中的 APIGroup 对象。
	groups := make([]metav1.APIGroup, len(orderedGroups))
	for i := range orderedGroups {
		groups[i] = orderedGroups[i]
		groups[i].ServerAddressByClientCIDRs = serverCIDR
	}

	return groups
}

// ServeHTTP 输出 APIGroupList；整个序列化过程持有读锁，保证输出的一致性快照。
func (s *rootAPIsHandler) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	groupList := metav1.APIGroupList{Groups: s.groupsLocked(req.Context(), req)}

	responsewriters.WriteObjectNegotiated(s.serializer, negotiation.DefaultEndpointRestrictions, schema.GroupVersion{}, resp, req, http.StatusOK, &groupList, false)
}

// restfulHandle 把 go-restful 请求桥接回标准 net/http 处理逻辑。
func (s *rootAPIsHandler) restfulHandle(req *restful.Request, resp *restful.Response) {
	s.ServeHTTP(resp.ResponseWriter, req.Request)
}

// WebService 返回一个服务于 API 组发现的 WebService。
// 注意：服务器运行期间 apiGroups 可能发生变化。
func (s *rootAPIsHandler) WebService() *restful.WebService {
	mediaTypes, _ := negotiation.MediaTypesForSerializer(s.serializer)
	ws := new(restful.WebService)
	ws.Path(APIGroupPrefix)
	ws.Doc("get available API versions")
	ws.Route(ws.GET("/").To(s.restfulHandle).
		Doc("get available API versions").
		Operation("getAPIVersions").
		Produces(mediaTypes...).
		Consumes(mediaTypes...).
		Writes(metav1.APIGroupList{}))
	return ws
}
