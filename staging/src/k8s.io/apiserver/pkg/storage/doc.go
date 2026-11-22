/*
Copyright 2015 The Kubernetes Authors.

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

// Interfaces for database-related operations.
//
// storage 包定义 apiserver 与底层存储后端之间的抽象：Interface 负责键值读写与
// watch，Versioner 负责把 resourceVersion 与存储层的 revision 相互转换，
// Preconditions 与 errors.go 中的错误类型一起保证乐观并发（resourceVersion 冲突）
// 和对象不存在等语义在不同后端之间保持一致。
package storage
