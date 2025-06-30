/*
Copyright 2025 The KCP Authors.

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

package apidomainkey

import (
	"fmt"
	"strings"

	"github.com/kcp-dev/logicalcluster/v3"

	dynamiccontext "github.com/kcp-dev/kcp/pkg/virtual/framework/dynamic/context"
)

func New(clusterName logicalcluster.Name, apiExport string) dynamiccontext.APIDomainKey {
	return dynamiccontext.APIDomainKey(fmt.Sprintf("%s/%s", clusterName, apiExport))
}

type Key struct {
	APIExportCluster logicalcluster.Name
	APIExportName    string
}

func Parse(key dynamiccontext.APIDomainKey) (*Key, error) {
	parts := strings.Split(string(key), "/")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid APIDomainKey %q for replication VW", string(key))
	}

	return &Key{
		APIExportCluster: logicalcluster.Name(parts[0]),
		APIExportName:    parts[1],
	}, nil
}
