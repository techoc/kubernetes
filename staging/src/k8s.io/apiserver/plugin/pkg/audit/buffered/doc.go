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

// Package buffered 实现 audit.Backend 接口：把进入的审计事件攒成批次，
// 再把批次交给下游（delegate）audit.Backend，以降低下游的调用频率。
// 批处理会引入延迟，因此必须配合缓冲区大小与超时一起调优，
// 避免事件长时间滞留在内存中。
//
// Package buffered 实现 audit.Backend：把收到的审计事件攒成批次，再发给委托的后端。
package buffered
