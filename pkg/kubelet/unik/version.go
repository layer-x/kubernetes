/*
Copyright 2014 The Kubernetes Authors All rights reserved.

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

package unik

import (
	"fmt"
	"github.com/layer-x/layerx-commons/lxerrors"
	"strconv"
)

type unikVersion struct {
	version int
}

func newUnikVersion(version int) *unikVersion {
	return &unikVersion{
		version: version,
	}
}

func (v *unikVersion) Compare(other string) (int, error) {
	otherVersion, err := strconv.Atoi(other)
	if err != nil {
		return 0, lxerrors.New("could not convert other to int", err)
	}
	diff := v.version - otherVersion
	switch {
	case diff < 0:
		return -1, nil
	case diff > 0:
		return 1, nil
	default:
		return 0, nil
	}
}

func (v *unikVersion) String() string {
	return fmt.Sprintf("%v", v.version)
}
