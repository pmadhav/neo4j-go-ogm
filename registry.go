// MIT License
//
// Copyright (c) 2022 pmadhav
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package gogm

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
)

type registry struct {
	objects map[string]metadata //domain object struct name to metadata

	labels     map[string][]metadata //labels to metadata
	registered map[reflect.Type]map[string]metadata

	cypherExecuter cypherExecuter
	objectsMu      sync.Mutex
}

func newRegistry(cypherExecuter cypherExecuter) *registry {
	registered := map[reflect.Type]map[string]metadata{}
	registered[reflect.TypeOf(&nodeMetadata{})] = map[string]metadata{}
	registered[reflect.TypeOf(&relationshipMetadata{})] = map[string]metadata{}

	return &registry{map[string]metadata{}, map[string][]metadata{}, registered, cypherExecuter, sync.Mutex{}}
}

func (r *registry) get(t reflect.Type, dbName string) (metadata, error) {
	var err error
	m := r.getMetadata(t.String())
	if m == nil {
		if m, err = getMetadata(t, r, dbName); err != nil {
			return nil, err
		}
		r.setMetadata(t.String(), m)
		for _, label := range strings.Split(m.getStructLabel(), labelsDelim) {
			r.labels[label] = append(r.labels[label], m)
		}
		if r.registered[reflect.TypeOf(m)][m.getStructLabel()] != nil {
			return nil, errors.New(fmt.Sprint("Duplicate labels for an entity type. Type ", r.registered[reflect.TypeOf(m)][m.getStructLabel()].getType().String(), " with label ", r.registered[reflect.TypeOf(m)][m.getStructLabel()].getStructLabel(), " conflicts with ", m.getType().String(), " with label ", m.getStructLabel()))
		}
		r.registered[reflect.TypeOf(m)][m.getStructLabel()] = m
		for _, statement := range getCreateSchemaStatement(m) {
			if _, err = r.cypherExecuter.exec(dbName, statement, nil, false, false); err != nil {
				return nil, err
			}
		}

	}
	return m, err
}

func (r *registry) getMetadata(id string) metadata {
	r.objectsMu.Lock()
	defer r.objectsMu.Unlock()
	return r.objects[id]
}

func (r *registry) setMetadata(id string, metadata metadata) {
	r.objectsMu.Lock()
	defer r.objectsMu.Unlock()
	r.objects[id] = metadata
}

func (r *registry) getLabelMetadatas(label string) []metadata {
	return r.labels[label]
}
