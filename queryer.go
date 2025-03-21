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
	"sort"
	"strings"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
)

type queryer struct {
	cypherExecuter *cypherExecuter
	graphFactory   graphFactory
	registry       *registry
}

func newQueryer(cypherExecutor *cypherExecuter, graphFactory graphFactory, registry *registry) *queryer {
	return &queryer{cypherExecutor, graphFactory, registry}
}

func (q *queryer) queryForObject(loadOptions *LoadOptions, object interface{}, cypher string, parameters map[string]interface{}) error {
	var (
		err      error
		values   reflect.Value
		metadata metadata
		records  []*neo4j.Record
		dbName   string = ""
	)
	if loadOptions != nil {
		dbName = loadOptions.DatabaseName
	}
	//Object: **DomainObject
	domainObjectType := reflect.TypeOf(object).Elem()
	if metadata, err = q.registry.get(domainObjectType, dbName); err != nil {
		return err
	}
	var label string
	if label, err = metadata.getLabel(invalidValue); err != nil {
		return err
	}
	if records, err = q.cypherExecuter.collect(dbName, cypher, parameters); err != nil {
		return err
	}

	if len(records) == 1 {
		if values, err = q.getObjectsFromRecords(domainObjectType, metadata, label, []*neo4j.Record{records[0]}); err != nil {
			return err
		}

		reflect.ValueOf(object).Elem().Set(values.Index(0))
	}

	if len(records) > 1 {
		return errors.New("result contains more than one record")
	}

	return nil
}

func (q *queryer) queryForObjects(loadOptions *LoadOptions, objects interface{}, cypher string, parameters map[string]interface{}) error {

	var (
		err      error
		records  []*neo4j.Record
		values   reflect.Value
		metadata metadata
		dbName   string = ""
	)

	if loadOptions != nil {
		dbName = loadOptions.DatabaseName
	}

	//Object type is *[]*<DomaoinObject>
	domainObjectType := reflect.TypeOf(objects).Elem().Elem()
	if metadata, err = q.registry.get(domainObjectType, dbName); err != nil {
		return err
	}

	var label string
	if label, err = metadata.getLabel(invalidValue); err != nil {
		return err
	}

	if records, err = q.cypherExecuter.collect(dbName, cypher, parameters); err != nil {
		return err
	}

	if values, err = q.getObjectsFromRecords(domainObjectType, metadata, label, records); err != nil {
		return err
	}
	reflect.ValueOf(objects).Elem().Set(values)
	return nil
}

func (q *queryer) query(loadOptions *LoadOptions, cypher string, parameters map[string]interface{}, objects ...interface{}) ([]map[string]interface{}, error) {
	var dbName string = ""
	if loadOptions != nil {
		dbName = loadOptions.DatabaseName
	}

	//registry all objects
	for _, object := range objects {
		if _, err := q.registry.get(reflect.TypeOf(object).Elem(), dbName); err != nil {
			return nil, err
		}
	}

	records, err := q.cypherExecuter.collect(dbName, cypher, parameters)
	if err != nil {
		return nil, err
	}

	rows := []map[string]interface{}{}
	for _, record := range records {
		columns := map[string]interface{}{}
		for index, key := range record.Keys {
			if neo4jNode, isNeo4jNode := record.Values[index].(neo4j.Node); isNeo4jNode {
				var g graph
				properties := neo4jNode.Props

				//find node struct and add it
				for _, label := range neo4jNode.Labels {
					for _, metadata := range q.registry.getLabelMetadatas(label) {
						if nodeMetadata, ok := metadata.(*nodeMetadata); ok && nodeMetadata.getType() != nil /*chech for nil?*/ {

							nodeLabels := make([]string, len(neo4jNode.Labels))
							copy(nodeLabels, neo4jNode.Labels)

							//remove runtime labels from node labels
							if nodeMetadata.runtimeLabelsStructField != nil {
								name := strings.ToLower(nodeMetadata.runtimeLabelsStructField.Name)
								names := getNamespacedTag(nodeMetadata.runtimeLabelsStructField.Tag).get(propertyNameTag)
								if len(names) > 0 {
									name = names[0]
								}
								for _, runtimeLabel := range properties[name].([]interface{}) {
									var index = indexOfString(nodeLabels, runtimeLabel.(string))
									if index > -1 {
										nodeLabels = removeStringAt(nodeLabels, index)
									}
								}
							}

							sort.Strings(nodeLabels)
							if strings.Join(nodeLabels, labelsDelim) == nodeMetadata.structLabel {
								v := reflect.New(nodeMetadata.getType().Elem())
								g = &node{
									Value:      &v,
									properties: neo4jNode.Props}
								g.getProperties()[idPropertyName] = neo4jNode.Id
								driverPropertiesAsStructFieldValues(g.getProperties(), nodeMetadata.getPropertyStructFields())
								unloadGraphProperties(g, nodeMetadata.getPropertyStructFields())
								break
							}

						}
					}
					if g != nil {
						break
					}
				}
				if g == nil {
					return nil, errors.New(fmt.Sprint("Not found: Runtime object for Node with id:", neo4jNode.Id, " and label:", strings.Join(neo4jNode.Labels, labelsDelim)))
				}
				columns[key] = g.getValue().Interface()
			} else if neo4jRelationship, isNeo4jRelationship := record.Values[index].(neo4j.Relationship); isNeo4jRelationship {

				//find relationship struct and add it
				var g graph
				relType := neo4jRelationship.Type
				for _, metadata := range q.registry.getLabelMetadatas(relType) {
					if relationshipMetadata, ok := metadata.(*relationshipMetadata); ok && relationshipMetadata.getType() != nil /*chech for nil?*/ {
						if relationshipMetadata.structLabel == neo4jRelationship.Type {
							v := reflect.New(relationshipMetadata.getType().Elem())
							g = &relationship{
								Value:      &v,
								properties: neo4jRelationship.Props}
							g.getProperties()[idPropertyName] = neo4jRelationship.Id
							driverPropertiesAsStructFieldValues(g.getProperties(), relationshipMetadata.getPropertyStructFields())
							unloadGraphProperties(g, relationshipMetadata.getPropertyStructFields())
							break
						}
					}
				}
				if g == nil {
					return nil, errors.New(fmt.Sprint("Not found: Runtime object for Node with id:", neo4jRelationship.Id, " and type:", neo4jRelationship.Type))
				}
				columns[key] = g.getValue().Interface()
			} else {
				columns[key] = record.Values[index]
			}
		}
		rows = append(rows, columns)
	}

	return rows, err
}

func (q *queryer) getObjectsFromRecords(domainObjectType reflect.Type, metadata metadata, label string, records []*neo4j.Record) (reflect.Value, error) {

	var (
		g                       graph
		entityLabel             string
		internalGraphEntityType = getInternalGraphType(domainObjectType.Elem())
	)

	sliceOfPtrToObjs := reflect.MakeSlice(reflect.SliceOf(domainObjectType), 0, 0)
	ptrToObjs := reflect.New(sliceOfPtrToObjs.Type())

	for _, record := range records {
		column0 := record.Values[0]
		newPtrToDomainObject := reflect.New(domainObjectType.Elem())

		if neo4jNode, isNeo4jNode := column0.(neo4j.Node); isNeo4jNode {

			if internalGraphEntityType != typeOfPrivateNode {
				return invalidValue, errors.New("expecting a Relationship, but got a Node from the query response")
			}
			nodeMetadata := metadata.(*nodeMetadata)
			labels := neo4jNode.Labels
			sort.Strings(labels)
			g = &node{
				ID:         neo4jNode.Id,
				properties: neo4jNode.Props,
				label:      strings.Join(labels, labelsDelim)}
			g.getProperties()[idPropertyName] = neo4jNode.Id

			entityLabel = nodeMetadata.filterStructLabel(g)
		}

		if neo4jRelationship, isNeo4jRelationship := column0.(neo4j.Relationship); isNeo4jRelationship {
			if internalGraphEntityType != typeOfPrivateRelationship {
				return invalidValue, errors.New("unexpected graph type. Expecting a Node, but got a Relationship from the query response")
			}
			g = &relationship{
				ID:         neo4jRelationship.Id,
				properties: neo4jRelationship.Props,
				relType:    neo4jRelationship.Type}
			g.getProperties()[idPropertyName] = neo4jRelationship.Id
			entityLabel = neo4jRelationship.Type
		}
		g.setValue(&newPtrToDomainObject)
		g.setLabel(label)

		if label != entityLabel {
			return invalidValue, errors.New("label '" + label + "' from `" + domainObjectType.String() + "` don't match with label `" + entityLabel + "` from query result")
		}

		ptrToObjs.Elem().Set(reflect.Append(ptrToObjs.Elem(), newPtrToDomainObject))
		driverPropertiesAsStructFieldValues(g.getProperties(), metadata.getPropertyStructFields())
		unloadGraphProperties(g, metadata.getPropertyStructFields())
	}

	return ptrToObjs.Elem(), nil
}

func (q *queryer) countEntitiesOfType(loadOptions *LoadOptions, object interface{}) (int64, error) {

	var (
		value         = reflect.ValueOf(object)
		record        *neo4j.Record
		count         int64
		cypherBuilder graphQueryBuilder
		graphs        []graph
		cypher        string
		parameters    map[string]interface{}
		err           error
		dbName        string = ""
	)

	if loadOptions != nil {
		dbName = loadOptions.DatabaseName
	}

	//object: **DomainObject
	if graphs, err = q.graphFactory.get(reflect.New(value.Elem().Type()), map[int]bool{labels: true}, dbName); err != nil {
		return -1, err
	}

	if cypherBuilder, err = newCypherBuilder(graphs[0], q.registry, nil, dbName); err != nil {
		return -1, err
	}
	cypher, parameters = cypherBuilder.getCountEntitiesOfType()

	if cypher != emptyString {
		if record, err = q.cypherExecuter.single(dbName, cypher, parameters); err != nil {
			return -1, err
		}
		if record != nil {
			count = record.Values[0].(int64)
		}
	}

	return count, nil
}

func (q *queryer) count(loadOptions *LoadOptions, cypher string, parameters map[string]interface{}) (int64, error) {
	var (
		record *neo4j.Record
		err    error
		dbName string = ""
	)
	if loadOptions != nil {
		dbName = loadOptions.DatabaseName
	}
	if record, err = q.cypherExecuter.single(dbName, cypher, parameters); err != nil {
		return -1, err
	}
	return record.Values[0].(int64), nil
}
