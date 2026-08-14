/*
Copyright 2025 The kcp Authors.

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

package dynamicrestmapper

import (
	"slices"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// This file adds mutable methods to our fork of upstream's DefaultRESTMapper.

func (m *DefaultRESTMapper) empty() bool {
	// If one of the maps is empty, all of the maps are empty.
	return len(m.resourceToKind) == 0
}

func (m *DefaultRESTMapper) add(typeMeta typeMeta) {
	kind := typeMeta.groupVersionKind()
	singular := typeMeta.groupVersionResourceSingular()
	plural := typeMeta.groupVersionResourcePlural()

	m.singularToPlural[singular] = plural
	m.pluralToSingular[plural] = singular

	m.resourceToKind[singular] = kind
	m.resourceToKind[plural] = kind

	m.kindToPluralResource[kind] = plural
	m.kindToScope[kind] = typeMeta.Scope

	foundDefaultVersion := false
	for i := range m.defaultGroupVersions {
		if m.defaultGroupVersions[i].Group == typeMeta.Group {
			if isPreferredAPIVersion(typeMeta.Version, m.defaultGroupVersions[i].Version) {
				m.defaultGroupVersions[i].Version = typeMeta.Version
			}
			foundDefaultVersion = true
			break
		}
	}

	if !foundDefaultVersion {
		m.defaultGroupVersions = append(m.defaultGroupVersions, schema.GroupVersion{
			Group:   typeMeta.Group,
			Version: typeMeta.Version,
		})
	}
}

func (m *DefaultRESTMapper) remove(typeMeta typeMeta) {
	kind := typeMeta.groupVersionKind()
	singular := typeMeta.groupVersionResourceSingular()
	plural := typeMeta.groupVersionResourcePlural()

	delete(m.singularToPlural, singular)
	delete(m.pluralToSingular, plural)

	delete(m.resourceToKind, singular)
	delete(m.resourceToKind, plural)

	delete(m.kindToPluralResource, kind)
	delete(m.kindToScope, kind)

	versionIdx := slices.IndexFunc(m.defaultGroupVersions, func(gv schema.GroupVersion) bool {
		return gv.Group == typeMeta.Group
	})
	if versionIdx < 0 {
		return
	}

	// Fixup the default version too.
	// We don't know what the current latest version is, so we need to find it.

	latestGroupVersion := ""
	for gvr := range m.pluralToSingular {
		if gvr.Group != typeMeta.Group {
			continue
		}
		if isPreferredAPIVersion(gvr.Version, latestGroupVersion) {
			latestGroupVersion = gvr.Version
		}
	}

	if latestGroupVersion != "" {
		m.defaultGroupVersions[versionIdx].Version = latestGroupVersion
	} else {
		// There are no more resources in this group.
		// Delete the default version too.
		m.defaultGroupVersions = slices.Delete(m.defaultGroupVersions, versionIdx, versionIdx+1)
	}
}

func (m *DefaultRESTMapper) getGVKR(gvr schema.GroupVersionResource) typeMeta {
	kind := m.resourceToKind[gvr]
	singular := m.pluralToSingular[gvr]
	if singular.Empty() {
		singular = gvr
	}
	plural := m.singularToPlural[gvr]
	if plural.Empty() {
		plural = gvr
	}
	scope := m.kindToScope[kind]

	return newTypeMeta(
		kind.Group,
		kind.Version,
		kind.Kind,
		singular.Resource,
		plural.Resource,
		scope,
	)
}

func (m *DefaultRESTMapper) getGVKRs(gr schema.GroupResource) ([]typeMeta, error) {
	gvrs, err := m.ResourcesFor(gr.WithVersion(""))
	if err != nil {
		return nil, err
	}
	gvkrs := make([]typeMeta, len(gvrs))
	for i := range gvrs {
		gvkrs[i] = m.getGVKR(gvrs[i])
	}
	return gvkrs, nil
}

// Applies the two slices to the known mappings. It is assumed there is
// no overlap between toRemove and toAdd.
func (m *DefaultRESTMapper) apply(toRemove []typeMeta, toAdd []typeMeta) {
	for i := range toRemove {
		m.remove(toRemove[i])
	}

	for i := range toAdd {
		m.add(toAdd[i])
	}
}

// isPreferredAPIVersion reports whether Kubernetes API version a is semantically
// preferred over b. Stability tiers (highest first): GA (v1, v2) > Beta (v1beta1)
// > Alpha (v1alpha1) > non-standard. Within a tier, higher major/iteration wins.
func isPreferredAPIVersion(a, b string) bool {
	aTier, aMajor, aIter := parseKubeAPIVersion(a)
	bTier, bMajor, bIter := parseKubeAPIVersion(b)
	if aTier != bTier {
		return aTier > bTier
	}
	if aMajor != bMajor {
		return aMajor > bMajor
	}
	return aIter > bIter
}

// parseKubeAPIVersion parses a Kubernetes API version string (v1, v1beta2, v1alpha3).
// Returns (tier, major, iter): tier is 3 for GA, 2 for beta, 1 for alpha, 0 for other.
func parseKubeAPIVersion(v string) (tier, major, iter int) {
	if len(v) == 0 || v[0] != 'v' {
		return 0, 0, 0
	}
	rest := v[1:]

	end := 0
	for end < len(rest) && rest[end] >= '0' && rest[end] <= '9' {
		end++
	}
	if end == 0 {
		return 0, 0, 0
	}
	maj, err := strconv.Atoi(rest[:end])
	if err != nil {
		return 0, 0, 0
	}
	rest = rest[end:]

	if len(rest) == 0 {
		return 3, maj, 0 // GA
	}

	var t int
	switch {
	case strings.HasPrefix(rest, "beta"):
		t, rest = 2, rest[4:]
	case strings.HasPrefix(rest, "alpha"):
		t, rest = 1, rest[5:]
	default:
		return 0, 0, 0
	}

	it, err := strconv.Atoi(rest)
	if err != nil || it < 0 {
		return 0, 0, 0
	}
	return t, maj, it
}
