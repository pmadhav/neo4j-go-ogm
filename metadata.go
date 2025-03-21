// MIT License
//
// Copyright (c) 2020 codingfinest
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
	"reflect"
	"sort"
	"strings"
)

type metadata interface {
	getLabel(reflect.Value) (string, error)
	getProperties(reflect.Value) map[string]interface{}
	getCustomID(reflect.Value) (string, reflect.Value)
	loadRelatedGraphs(g graph, ID func(graph), registry *registry, dbName string) (map[int64]graph, error)
	getGraphField(ref graph, relatedGraph graph) (*field, error)
	getPropertyStructFields() map[string]*reflect.StructField
	getStructLabel() string
	getType() reflect.Type
}

type commonMetadata struct {
	name                 string
	structLabel          string
	registry             *registry
	propertyStructFields map[string]*reflect.StructField
	customIDBackendName  string
	_type                reflect.Type
}

func (c *commonMetadata) getType() reflect.Type {
	return c._type
}

func (c *commonMetadata) getStructLabel() string {
	return c.structLabel
}

func (c *commonMetadata) getCustomID(v reflect.Value) (string, reflect.Value) {
	if c.customIDBackendName != emptyString {
		return c.customIDBackendName, v.Elem().FieldByName(c.propertyStructFields[c.customIDBackendName].Name)
	}
	return emptyString, invalidValue
}

func (c *commonMetadata) getPropertyStructFields() map[string]*reflect.StructField {
	return c.propertyStructFields
}

func (c *commonMetadata) getProperties(v reflect.Value) map[string]interface{} {

	if v.IsZero() {
		return nil
	}

	properties := map[string]interface{}{}
	for backendName, structField := range c.propertyStructFields {
		if structField.Type.Kind() == reflect.Map {
			for mappedKey, value := range getMapProperties(backendName, structField, v) {
				properties[mappedKey] = value
			}
		} else {
			properties[backendName] = v.Elem().FieldByName(structField.Name).Interface()
		}
	}
	return properties
}

func getMetadata(t reflect.Type, registry *registry, dbName string) (metadata, error) {

	var (
		typeOfInternalGraph  reflect.Type
		metadata             metadata
		err                  error
		propertyStructFields map[string]*reflect.StructField
	)

	if t.Kind() != reflect.Ptr || t.Elem().Kind() != reflect.Struct {
		return nil, errors.New("Metadata of type " + t.String() + " can't be generated. Expecting a type 'pointer to struct'")
	}

	typeOfObject := t
	valueOfObject := reflect.New(t.Elem()) //Dummy value

	if typeOfInternalGraph = getInternalGraphType(typeOfObject.Elem()); typeOfInternalGraph == nil {
		return nil, errors.New("Metadata of type " + t.String() + " can't be generated. It must embed the OGM Node or Relationship Object directly or indirectly")
	}

	if propertyStructFields, err = getPropertyStructField(typeOfObject.Elem()); err != nil {
		return nil, err
	}
	var customIDBackendName string
	if customIDBackendName, err = getCustomIDBackendName(propertyStructFields); err != nil {
		return nil, err
	}

	if typeOfInternalGraph == typeOfPrivateRelationship {
		r := newRelationshipMetadata()
		r.registry = registry
		r.name = typeOfObject.String()
		r.structLabel = getRelationshipType(typeOfObject.Elem())
		r.propertyStructFields = propertyStructFields
		r.customIDBackendName = customIDBackendName
		r._type = typeOfObject

		endpointFields, _ := getFeilds(valueOfObject.Elem(), isRelationshipEndPointFieldFilter(startNodeTag), isRelationshipEndPointFieldFilter(endNodeTag))

		if len(endpointFields[startNode]) != 1 {
			return nil, errors.New("Expected 1 field to be tagged 'startNode' in type " + typeOfObject.String())
		}
		if len(endpointFields[endNode]) != 1 {
			return nil, errors.New("Expected 1 field to be tagged 'endNode' in type " + typeOfObject.String())
		}

		if endpointFields[startNode][0].getStructField().Type.Kind() != reflect.Ptr || endpointFields[startNode][0].getStructField().Type.Elem().Kind() != reflect.Struct || getInternalGraphType(endpointFields[startNode][0].getStructField().Type.Elem()) != typeOfPrivateNode {
			return nil, errors.New("Start node for relationship " + typeOfObject.String() + " must be a point to a Node struct")
		}

		if endpointFields[endNode][0].getStructField().Type.Kind() != reflect.Ptr || endpointFields[endNode][0].getStructField().Type.Elem().Kind() != reflect.Struct || getInternalGraphType(endpointFields[startNode][0].getStructField().Type.Elem()) != typeOfPrivateNode {
			return nil, errors.New("End node for relationship " + typeOfObject.String() + " must be a point to a Node struct")
		}

		r.endpoints[startNode] = endpointFields[startNode][0].getStructField()
		r.endpoints[endNode] = endpointFields[endNode][0].getStructField()

		metadata = r
	} else {
		n := newNodeMetadata()
		n.registry = registry
		n.name = typeOfObject.String()
		n.customIDBackendName = customIDBackendName
		n.thisStructLabel = getThisStructLabels(typeOfObject.Elem())
		n._type = typeOfObject

		labels := getNodeLabels(typeOfObject.Elem())
		sort.Strings(labels)
		n.structLabel = strings.Join(labels, labelsDelim)
		n.blacklistLabels(labels)

		n.propertyStructFields = propertyStructFields
		n.runtimeLabelsStructField = getRuntimeLabelsStructFeild(propertyStructFields)

		relationships, _ := getFeilds(valueOfObject.Elem(), isRelationshipFieldFilter(typeOfPrivateNode), isRelationshipFieldFilter(typeOfPrivateRelationship))

		for _, relationshipFieldA := range relationships[0] {

			relationshipAStructField := relationshipFieldA.getStructField()
			n.relationshipAStructFields = append(n.relationshipAStructFields, relationshipAStructField)

			labels = getNodeLabels(elem(relationshipAStructField.Type).Elem())
			sort.Strings(labels)
			n.blacklistLabels(labels)
			relationshipANodeLabel := strings.Join(labels, labelsDelim)

			relType := relationshipFieldA.getRelType()
			relDirection := relationshipFieldA.getEffectiveDirection()

			if n.structLabel == relationshipANodeLabel {
				if relDirection == undirected {
					if err = n.setSameEntityRelStructFields(typeOfObject, relType, incoming, &relationshipAStructField); err != nil {
						return nil, err
					}

					if err = n.setSameEntityRelStructFields(typeOfObject, relType, outgoing, &relationshipAStructField); err != nil {
						return nil, err
					}
				} else {
					if err = n.setSameEntityRelStructFields(typeOfObject, relType, relDirection, &relationshipAStructField); err != nil {
						return nil, err
					}
				}
			} else {
				if relDirection == undirected {
					if err = n.setDifferentEntityRelStructFields(typeOfObject, relType, n.structLabel, relationshipANodeLabel, &relationshipAStructField); err != nil {
						return nil, err
					}

					if err = n.setDifferentEntityRelStructFields(typeOfObject, relType, relationshipANodeLabel, n.structLabel, &relationshipAStructField); err != nil {
						return nil, err
					}

				} else {
					if relDirection == incoming {
						if err = n.setDifferentEntityRelStructFields(typeOfObject, relType, relationshipANodeLabel, n.structLabel, &relationshipAStructField); err != nil {
							return nil, err
						}
					} else {
						if err = n.setDifferentEntityRelStructFields(typeOfObject, relType, n.structLabel, relationshipANodeLabel, &relationshipAStructField); err != nil {
							return nil, err
						}
					}
				}
			}
		}

		for _, relationshipFieldB := range relationships[1] {
			n.relationshipBStructFields = append(n.relationshipBStructFields, relationshipFieldB.getStructField())

			relationshipBStructField := relationshipFieldB.getStructField()
			relationshipEntityType := elem(relationshipBStructField.Type)

			if metadata, err = n.registry.get(relationshipEntityType, dbName); err != nil {
				return nil, err
			}
			rMetadata := metadata.(*relationshipMetadata)
			fromNodeStructField := rMetadata.endpoints[startNode]
			toNodeStructField := rMetadata.endpoints[endNode]

			if fromNodeStructField.Type != typeOfObject && toNodeStructField.Type != typeOfObject {
				return nil, errors.New("Node entity '" + typeOfObject.String() + "' has an unrelated relationship entity '" + relationshipEntityType.String() + "'")
			}

			labels = getNodeLabels(elem(fromNodeStructField.Type).Elem())
			sort.Strings(labels)
			n.blacklistLabels(labels)
			fromNodeLabel := strings.Join(labels, labelsDelim)

			labels = getNodeLabels(elem(toNodeStructField.Type).Elem())
			sort.Strings(labels)
			n.blacklistLabels(labels)
			toNodeLabel := strings.Join(labels, labelsDelim)

			realtionshipType, _ := rMetadata.getLabel(invalidValue)

			if fromNodeLabel == toNodeLabel {
				if err = n.setSameEntityRelStructFields(typeOfObject, realtionshipType, incoming, &relationshipBStructField); err != nil {
					return nil, err
				}
				if err = n.setSameEntityRelStructFields(typeOfObject, realtionshipType, outgoing, &relationshipBStructField); err != nil {
					return nil, err
				}
			} else {
				if err = n.setDifferentEntityRelStructFields(typeOfObject, realtionshipType, fromNodeLabel, toNodeLabel, &relationshipBStructField); err != nil {
					return nil, err
				}
			}
		}
		metadata = n
	}
	return metadata, nil
}
