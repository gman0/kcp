/*
Copyright 2022 The KCP Authors.

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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	conditionsv1alpha1 "github.com/kcp-dev/kcp/sdk/apis/third_party/conditions/apis/conditions/v1alpha1"
)

// +crd
// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster,categories=kcp,path=cachedresourceendpointslices,singular=cachedresourceendpointslice
// +kubebuilder:printcolumn:name="Export",type="string",JSONPath=".spec.export.name"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"

// CachedResourceEndpointSlice is a sink for the endpoints of an APIExport.
// They get consumed by the managers to start controllers and informers for the respective APIExport services.
type CachedResourceEndpointSlice struct {
	metav1.TypeMeta `json:",inline"`
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// spec holds the desired state:
	// - the targeted CachedResource
	Spec CachedResourceEndpointSliceSpec `json:"spec,omitempty"`

	// status communicates the observed state:
	// the filtered list of endpoints for the APIExport service.
	// +optional
	Status CachedResourceEndpointSliceStatus `json:"status,omitempty"`
}

// CachedResourceEndpointSliceSpec defines the desired state of the CachedResourceEndpointSlice.
type CachedResourceEndpointSliceSpec struct {
	// CachedResource points to the real CachedResource the slice is created for.
	//
	// +required
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="APIExport reference must not be changed"
	CachedResource CachedResourceReference `json:"cachedResource"`
}

// CachedResourceEndpointSliceStatus defines the observed state of CachedResourceEndpointSlice.
type CachedResourceEndpointSliceStatus struct {
	// +optional

	// conditions is a list of conditions that apply to the CachedResourceEndpointSlice.
	Conditions conditionsv1alpha1.Conditions `json:"conditions,omitempty"`

	// endpoints contains all the URLs of the APIExport service.
	//
	// +optional
	// +listType=map
	// +listMapKey=url
	CachedResourceEndpoints []CachedResourceEndpoint `json:"endpoints"`
}

// Using a struct provides an extension point

// APIExportEndpoint contains the endpoint information of an APIExport service for a specific shard.
type CachedResourceEndpoint struct {

	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:format:URL
	// +required

	// url is an APIExport virtual workspace URL.
	URL string `json:"url"`
}

func (in *CachedResourceEndpointSlice) GetConditions() conditionsv1alpha1.Conditions {
	return in.Status.Conditions
}

func (in *CachedResourceEndpointSlice) SetConditions(conditions conditionsv1alpha1.Conditions) {
	in.Status.Conditions = conditions
}

// These are valid conditions of CachedResourceEndpointSlice in addition to
// APIExportValid and related reasons defined with the APIBinding type.
const (
	// EndpointURLsReady is a condition for CachedResourceEndpointSlice that reflects the readiness of the URLs.
	// Deprecated: This condition is deprecated and will be removed in a future release.
	CachedResourceEndpointSliceURLsReady conditionsv1alpha1.ConditionType = "EndpointURLsReady"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// CachedResourceEndpointSliceList is a list of CachedResourceEndpointSlice resources.
type CachedResourceEndpointSliceList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []CachedResourceEndpointSlice `json:"items"`
}
