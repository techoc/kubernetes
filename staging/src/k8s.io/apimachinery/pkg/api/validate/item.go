/*
Copyright 2024 The Kubernetes Authors.

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

package validate

import (
	"context"

	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// MatchItemFunc takes an item and returns true if it matches the criteria.
// It is expected to target a unique key within the list.
type MatchItemFunc[T any] func(T) bool

// ValSliceItem finds the first item in a list of values which satisfies the
// match function, and if found, also looks for a matching item in oldList. If
// the value of the item is the same as the previous value, as per the equiv
// function, then no validation is performed. Otherwise, it invokes
// 'itemValidator' on these items.
//
// This function processes only the *first* matching item found in newList. It
// assumes that the match functions targets a unique identifier (primary key)
// and will match at most one element per list. If this assumption is violated,
// changes in list order can lead this function to have inconsistent behavior.
//
// The match and equiv functions will never be called with nil arguments.
//
// The fldPath passed to itemValidator is indexed to the matched item's
// position in newList.
//
// This function does not validate items that were removed (present in oldList
// but not in newList).
func ValSliceItem[TList ~[]TItem, TItem any](
	ctx context.Context, op operation.Operation, fldPath *field.Path,
	newList, oldList TList,
	match MatchItemFunc[*TItem],
	equiv MatchFunc[*TItem],
	itemValidator func(ctx context.Context, op operation.Operation, fldPath *field.Path, newObj, oldObj *TItem) field.ErrorList,
) field.ErrorList {
	var matchedNew, matchedOld *TItem
	var newIndex int

	// Only the first match is processed; later duplicates are ignored by design.
	for i := range newList {
		if match(&newList[i]) {
			matchedNew = &newList[i]
			newIndex = i
			break
		}
	}
	// The item is absent from the new list, so there is nothing to validate.
	if matchedNew == nil {
		return nil
	}

	// Find the same item in the old list to enable ratcheting.
	for i := range oldList {
		if match(&oldList[i]) {
			matchedOld = &oldList[i]
			break
		}
	}

	// Ratcheting: on update, an unchanged item skips validation so that
	// previously persisted data is not rejected by newer, stricter rules.
	if op.Type == operation.Update && matchedOld != nil && equiv(matchedNew, matchedOld) {
		return nil
	}

	return itemValidator(ctx, op, fldPath.Index(newIndex), matchedNew, matchedOld)
}

// PtrSliceItem finds the first item in a list of pointers which satisfies the
// match function, and if found, also looks for a matching item in oldList. If
// the value of the item is the same as the previous value, as per the equiv
// function, then no validation is performed. Otherwise, it invokes
// 'itemValidator' on these items.
//
// This function processes only the *first* matching item found in newList. It
// assumes that the match functions targets a unique identifier (primary key)
// and will match at most one element per list. If this assumption is violated,
// changes in list order can lead this function to have inconsistent behavior.
//
// The match and equiv functions will never be called with nil arguments.
//
// The fldPath passed to itemValidator is indexed to the matched item's
// position in newList.
//
// This function does not validate items that were removed (present in oldList
// but not in newList).
func PtrSliceItem[TList ~[]*TItem, TItem any](
	ctx context.Context, op operation.Operation, fldPath *field.Path,
	newList, oldList TList,
	match MatchItemFunc[*TItem],
	equiv MatchFunc[*TItem],
	itemValidator func(ctx context.Context, op operation.Operation, fldPath *field.Path, newObj, oldObj *TItem) field.ErrorList,
) field.ErrorList {
	var matchedNew, matchedOld *TItem
	var newIndex int

	// Nil entries are tolerated here; PtrSliceNoNils reports them separately.
	for i := range newList {
		if newList[i] == nil {
			// Ignore nil items; they are supposed to have been checked by PtrSliceNoNils.
			continue
		}
		if match(newList[i]) {
			matchedNew = newList[i]
			newIndex = i
			break
		}
	}
	if matchedNew == nil {
		return nil
	}

	// Nil entries in the old list cannot match and are skipped.
	for i := range oldList {
		if oldList[i] == nil {
			continue
		}
		if match(oldList[i]) {
			matchedOld = oldList[i]
			break
		}
	}

	// Same ratcheting rule as ValSliceItem: unchanged items skip validation.
	if op.Type == operation.Update && matchedOld != nil && equiv(matchedNew, matchedOld) {
		return nil
	}

	return itemValidator(ctx, op, fldPath.Index(newIndex), matchedNew, matchedOld)
}
