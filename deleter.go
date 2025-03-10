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
	"reflect"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
)

type deleter struct {
	cypherExecuter *cypherExecuter
	store          store
	eventer        eventer
	registry       *registry
	graphFactory   graphFactory
}

func newDeleter(cypherExecuter *cypherExecuter, store store, eventer eventer, registry *registry, graphFactory graphFactory) *deleter {
	return &deleter{cypherExecuter, store, eventer, registry, graphFactory}
}

func (d *deleter) delete(object interface{}, deleteOptions *DeleteOptions) error {

	var (
		value              = reflect.ValueOf(object)
		IDer               = getIDer(nil, nil)
		err                error
		parameters         = []map[string]interface{}{}
		graphDeleteClauses = map[clause][]string{}
		graphs             []graph
		record             *neo4j.Record
		dbName             string = ""
	)

	if deleteOptions != nil {
		dbName = deleteOptions.DatabaseName
	}

	if graphs, err = d.graphFactory.get(value, map[int]bool{labels: true, relatedGraph: true}, dbName); err != nil {
		return err
	}

	for _, graph := range graphs {
		IDer(graph)
	}

	if graphs[0].getID() < 0 {
		return nil
	}

	storedGraph := d.store.get(graphs[0])

	if storedGraph == nil || storedGraph.getID() < 0 {
		return nil
	}

	var cypherBuilder graphQueryBuilder
	if cypherBuilder, err = newCypherBuilder(storedGraph, d.registry, nil, dbName); err != nil {
		return err
	}
	delete, deleteParameters, depedencies := cypherBuilder.getDelete(dbName)
	for _, depedency := range depedencies {
		var depedencyCypherBuilder graphQueryBuilder
		if depedencyCypherBuilder, err = newCypherBuilder(depedency, d.registry, nil, dbName); err != nil {
			return err
		}

		match, matchParameters, _ := depedencyCypherBuilder.getMatch(dbName)
		parameters = append(parameters, matchParameters)
		graphDeleteClauses[matchClause] = append(graphDeleteClauses[matchClause], match)
	}

	parameters = append(parameters, deleteParameters)
	graphDeleteClauses[deleteClause] = append(graphDeleteClauses[deleteClause], delete)

	cypher := getCyhperFromClauses(graphDeleteClauses)
	if cypher != emptyString {

		typeOfGraphToDelete := reflect.TypeOf(storedGraph)
		for _, eventListener := range d.eventer.eventListeners {
			eventListener.OnPreDelete(event{storedGraph.getValue(), DELETE})
			if typeOfPrivateNode == typeOfGraphToDelete {
				for _, relationship := range storedGraph.getRelatedGraphs() {
					if relationship.getValue().IsValid() {
						eventListener.OnPreDelete(event{relationship.getValue(), DELETE})
					}
				}
			}
		}
		if record, err = d.cypherExecuter.single(dbName, cypher, flattenParamters(parameters)); err != nil {
			return err
		}
		if record != nil {
			deletedGraphs, updatedGraphs := d.store.delete(storedGraph, dbName)
			for _, updatedGraph := range updatedGraphs {
				notifyPostDelete(d.eventer, updatedGraph, UPDATE)
			}
			for _, deletedGraph := range deletedGraphs {
				notifyPostDelete(d.eventer, deletedGraph, DELETE)
			}
		}
	}

	return nil
}

func (d *deleter) deleteAll(object interface{}, deleteOptions *DeleteOptions) error {
	var (
		value   = reflect.ValueOf(object)
		graphs  []graph
		err     error
		records []*neo4j.Record
		dbName  string = ""
	)

	if deleteOptions != nil {
		dbName = deleteOptions.DatabaseName
	}

	if graphs, err = d.graphFactory.get(value, map[int]bool{labels: true}, dbName); err != nil {
		return err
	}

	var cypherBuilder graphQueryBuilder
	if cypherBuilder, err = newCypherBuilder(graphs[0], d.registry, nil, dbName); err != nil {
		return err
	}
	cypher, parameter := cypherBuilder.getDeleteAll()

	if cypher != emptyString {
		if records, err = d.cypherExecuter.collect(dbName, cypher, parameter); err != nil {
			return err
		}
		for _, record := range records {
			graphs[0].setID(record.Values[0].(int64))
			deletedGraphs, updatedGraphs := d.store.delete(graphs[0], dbName)
			for _, updatedGraph := range updatedGraphs {
				notifyPostDelete(d.eventer, updatedGraph, UPDATE)
			}
			for _, deletedGraph := range deletedGraphs {
				notifyPostDelete(d.eventer, deletedGraph, DELETE)
			}
		}
	}

	return nil
}

func (d *deleter) purgeDatabase(deleteOptions *DeleteOptions) error {
	var err error
	var dbName string = ""

	if deleteOptions != nil {
		dbName = deleteOptions.DatabaseName
	}

	if _, err = d.cypherExecuter.exec(dbName, "MATCH (n) DETACH DELETE n", nil, false, false); err != nil {
		return err
	}
	for _, deletedGraph := range d.store.purge(dbName) {
		notifyPostDelete(d.eventer, deletedGraph, DELETE)
	}

	return err
}
