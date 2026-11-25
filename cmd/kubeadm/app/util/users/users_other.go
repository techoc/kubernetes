//go:build !linux

/*
Copyright 2021 The Kubernetes Authors.

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

// Package users 包含管理用户的工具。
package users

// EntryMap 在非 Linux 上为空。
type EntryMap struct{}

// UsersAndGroups 在非 Linux 上为空。
type UsersAndGroups struct{}

// ID 在非 Linux 上为空操作。
func (*EntryMap) ID(string) *int64 {
	return nil
}

// String 在非 Linux 上为空操作。
func (*EntryMap) String() string {
	return ""
}

// AddUsersAndGroups 在非 Linux 上为空操作。
func AddUsersAndGroups() (*UsersAndGroups, error) {
	return nil, nil
}

// RemoveUsersAndGroups 在非 Linux 上为空操作。
func RemoveUsersAndGroups() error {
	return nil
}

// UpdatePathOwnerAndPermissions 在非 Linux 上为空操作。
func UpdatePathOwnerAndPermissions(path string, uid, gid int64, perms uint32) error {
	return nil
}

// UpdatePathOwner 在非 Linux 上为空操作。
func UpdatePathOwner(dirPath string, uid, gid int64) error {
	return nil
}
