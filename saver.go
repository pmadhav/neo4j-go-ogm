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
	"reflect"
	"strings"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
)

type saver struct {
	cypherExecuter *cypherExecuter
	store          store
	eventer        eventer
	registry       *registry
	graphFactory   graphFactory
}

func newSaver(cypherExecuter *cypherExecuter, store store, eventer eventer, registry *registry, graphFactory graphFactory) *saver {
	return &saver{cypherExecuter, store, eventer, registry, graphFactory}
}

func (s *saver) save(object interface{}, saveOptions *SaveOptions) error {
	var (
		graphs        []graph
		record        *neo4j.Record
		savedGraphs   map[string]graph
		deletedGraphs map[string]graph
		err           error
		store         = s.store
		savedDepths   []int
	)

	if saveOptions == nil {
		saveOptions = NewSaveOptions("", maxDepth)
	}

	if saveOptions.Depth > maxDepth {
		return errors.New("cannot save greater than max depth")
	}

	if graphs, err = s.graphFactory.get(reflect.ValueOf(object), nil, saveOptions.DatabaseName); err != nil {
		return err
	}

	if savedDepths, record, savedGraphs, deletedGraphs, err = s.persist(graphs, saveOptions); err != nil {
		return err
	}

	if record != nil {
		createdGraphSignatures := map[string]bool{}
		for index, key := range record.Keys {
			properties := record.Values[index].(map[string]interface{})

			//New graphs have negative IDs. Update the local graphs with database generated IDs
			if savedGraphs[key] != nil && savedGraphs[key].getID() < 0 {
				id := properties[idPropertyName].(int64)
				unloadGraphID(savedGraphs[key], &id)
				createdGraphSignatures[savedGraphs[key].getSignature()] = true
			}

			if deletedGraphs[key] != nil {
				//deletedGraphs[key] has been deleted. Update the local store and notify objects
				for _, relatedGraph := range deletedGraphs[key].getRelatedGraphs() {
					delete(store.get(relatedGraph).getRelatedGraphs(), deletedGraphs[key].getID())
					notifyPostSave(s.eventer, relatedGraph, UPDATE)
				}
				store.delete(deletedGraphs[key], saveOptions.DatabaseName)
				notifyPostDelete(s.eventer, deletedGraphs[key], DELETE)
			}
		}

		for _, g := range savedGraphs {
			for internalID, relatedGraph := range g.getRelatedGraphs() {
				if internalID < 0 {
					//Related graph map is still referencing the tempoary ID. Update with database generated IDs
					delete(g.getRelatedGraphs(), internalID)
					if relatedGraph.getID() > initialGraphID {
						g.setRelatedGraph(relatedGraph)
					}
				}
			}

			if g.getCoordinate() != nil {
				savedDepth := savedDepths[g.getCoordinate().graphIndex]
				if savedDepth >= 0 {
					if g.getCoordinate().depth == 0 {
						g.setDepth(&savedDepth)
					}
					saveLifecycle := UPDATE
					if createdGraphSignatures[g.getSignature()] {
						saveLifecycle = CREATE
					}
					store.save(g, saveOptions.DatabaseName)
					if g.getValue().IsValid() {
						for _, eventListener := range s.eventer.eventListeners {
							eventListener.OnPostSave(event{g.getValue(), saveLifecycle})
						}
					}
				}
			}
		}
	}

	return err
}

func (s *saver) persist(graphs []graph, saveOptions *SaveOptions) ([]int, *neo4j.Record, map[string]graph, map[string]graph, error) {

	var (
		err    error
		record *neo4j.Record
		params map[string]interface{}

		loadedGraphs = newstore(nil)

		savedGraphs   map[string]graph
		deletedGraphs map[string]graph

		grandParams        = map[string]interface{}{}
		grandSavedGraphs   = map[string]graph{}
		grandDeletedGraphs = map[string]graph{}
		saveClausesSlice   []clauses
		savedDepths        []int
		ensureID                  = getIDer(&internalIDGenerator{initialGraphID}, s.store)
		dbName             string = ""
	)

	for index, graph := range graphs {
		ensureID(graph)
		for _, rg := range graph.getRelatedGraphs() {
			ensureID(rg)
		}
		graph.setCoordinate(&coordinate{0, index})

		var graphSaveClauses clauses
		var savedDepth int
		if savedDepth, graphSaveClauses, savedGraphs, deletedGraphs, params, err = s.getSaveMeta(graph, saveOptions, ensureID, loadedGraphs); err != nil {
			return savedDepths, nil, nil, nil, err
		}

		savedDepths = append(savedDepths, savedDepth)

		saveClausesSlice = append(saveClausesSlice, graphSaveClauses)

		for key, value := range params {
			grandParams[key] = value
		}

		for cqlref, graph := range savedGraphs {
			grandSavedGraphs[cqlref] = graph
		}

		for cqlref, graph := range deletedGraphs {
			grandDeletedGraphs[cqlref] = graph
		}
	}

	var grandSaveClauses = make(clauses)
	for _, graphSaveClauses := range saveClausesSlice {
		for clause, grandSaveClause := range graphSaveClauses {
			grandSaveClauses[clause] = append(grandSaveClauses[clause], grandSaveClause...)
		}
	}

	cypher := getCyhperFromClauses(grandSaveClauses)
	graphGroups := [2]map[string]graph{grandSavedGraphs, grandDeletedGraphs}
	_return := ``
	for _, graphGroup := range graphGroups {
		if len(graphGroup) > 0 {
			begin := `, `
			if _return == emptyString {
				begin = `return `
			}
			_return += begin
			for entityCQLRef := range graphGroup {
				_return += entityCQLRef + `{` + idPropertyName + `:ID(` + entityCQLRef + `)},`
			}
			_return = strings.TrimSuffix(_return, ",")
		}
	}

	cypher += _return

	if cypher != emptyString {
		var records []*neo4j.Record
		if saveOptions != nil {
			dbName = saveOptions.DatabaseName
		}
		if records, err = s.cypherExecuter.collect(dbName, cypher, grandParams); err != nil {
			return savedDepths, nil, nil, nil, err
		}
		record = records[0]
	}

	return savedDepths, record, grandSavedGraphs, grandDeletedGraphs, err
}

func (s *saver) getSaveMeta(g graph, saveOptions *SaveOptions, ensureID func(graph), loadedGraphs store) (int, map[clause][]string, map[string]graph, map[string]graph, map[string]interface{}, error) {
	var (
		err error

		savedGraphs      = map[string]graph{}
		deletedGraphs    = map[string]graph{}
		gotten           = map[string]graphQueryBuilder{}
		parameters       = []map[string]interface{}{}
		graphSaveClauses = map[clause][]string{}

		savedDepth  = -1
		depedencies []map[string]graph
		dbName      string = ""
	)

	if saveOptions != nil {
		dbName = saveOptions.DatabaseName
	}

	maxGraphDepth := maxDepth
	if saveOptions.Depth > infiniteDepth {
		maxGraphDepth = 2 * saveOptions.Depth
	}

	if g.getID() == initialGraphID {
		return savedDepth, nil, nil, nil, nil, nil
	}

	queue := []graph{g}

	loadedGraphs.save(g, dbName)

	for len(queue) > 0 {

		savedDepth = queue[0].getCoordinate().depth

		if err = notifyPreSaveGraph(queue[0], s.eventer, s.registry, dbName); err != nil {
			return savedDepth, nil, nil, nil, nil, err
		}

		if reflect.TypeOf(queue[0]) == typeOfPrivateRelationship || queue[0].getCoordinate().depth+1 < maxGraphDepth {
			if err := loadRelatedGraphs(queue[0], ensureID, s.registry, loadedGraphs, s.store, dbName); err != nil {
				return savedDepth, nil, nil, nil, nil, err
			}
		}

		var cBuilder graphQueryBuilder
		if cBuilder, err = newCypherBuilder(queue[0], s.registry, s.store, dbName); err != nil {
			return savedDepth, nil, nil, nil, nil, err
		}
		if cBuilder.isGraphDirty() {

			if queue[0].getID() < 0 {
				nodeCreate, relationshipCreate, createParameters, createDeps := cBuilder.getCreate()
				parameters = append(parameters, createParameters)
				if nodeCreate != emptyString {
					graphSaveClauses[nodeCreateClause] = append(graphSaveClauses[nodeCreateClause], nodeCreate)
				}
				if relationshipCreate != emptyString {
					graphSaveClauses[relationshipCreateClause] = append(graphSaveClauses[relationshipCreateClause], relationshipCreate)
				}

				depedencies = append(depedencies, createDeps)
			} else {
				match, matchParameters, matchDeps := cBuilder.getMatch(dbName)
				parameters = append(parameters, matchParameters)
				graphSaveClauses[matchClause] = append(graphSaveClauses[matchClause], match)

				depedencies = append(depedencies, matchDeps)
			}
			set, setParameters := cBuilder.getSet()
			parameters = append(parameters, setParameters)
			graphSaveClauses[setClause] = append(graphSaveClauses[setClause], set)

			if queue[0].getCoordinate().depth+1 < maxGraphDepth {
				removedRelationships, otherNodes := cBuilder.getRemovedGraphs()

				for _, removedRelationship := range removedRelationships {

					otherNode := otherNodes[removedRelationship.getID()]
					var removedCBuilder, otherGraphCBuilder graphQueryBuilder
					if removedCBuilder, err = newCypherBuilder(removedRelationship, s.registry, nil, dbName); err != nil {
						return savedDepth, nil, nil, nil, nil, err
					}
					if otherGraphCBuilder, err = newCypherBuilder(otherNode, s.registry, nil, dbName); err != nil {
						return savedDepth, nil, nil, nil, nil, err
					}

					match, matchParameters, matchDeps := removedCBuilder.getMatch(dbName)
					parameters = append(parameters, matchParameters)
					graphSaveClauses[matchClause] = append(graphSaveClauses[matchClause], match)
					depedencies = append(depedencies, matchDeps)

					match, matchParameters, matchDeps = otherGraphCBuilder.getMatch(dbName)
					parameters = append(parameters, matchParameters)
					graphSaveClauses[matchClause] = append(graphSaveClauses[matchClause], match)
					depedencies = append(depedencies, matchDeps)

					graphSaveClauses[deleteClause] = append(graphSaveClauses[deleteClause], "DELETE "+removedRelationship.getSignature()+"\n")

					deletedGraphs[removedRelationship.getSignature()] = removedRelationship
					savedGraphs[otherNode.getSignature()] = otherNode
				}
			}
			savedGraphs[queue[0].getSignature()] = queue[0]
		}

		gotten[queue[0].getSignature()] = cBuilder

		for _, relatedGraph := range queue[0].getRelatedGraphs() {
			if gotten[relatedGraph.getSignature()] == nil && relatedGraph.getID() != initialGraphID {
				queue = append(queue, relatedGraph)
			}
		}
		queue[0] = nil
		queue = queue[1:]
	}

	//Relationship match depends on Node match. When relationships are dirty,
	//but node's aren't dirty, node match have to be included to match
	//the relationship for update
	for _, dep := range depedencies {
		for ID := range dep {
			if savedGraphs[ID] == nil {
				match, matchParameters, _ := gotten[ID].getMatch(dbName)
				parameters = append(parameters, matchParameters)
				graphSaveClauses[matchClause] = append(graphSaveClauses[matchClause], match)
				savedGraphs[ID] = gotten[ID].getGraph()
			}
		}
	}

	return savedDepth, graphSaveClauses, savedGraphs, deletedGraphs, flattenParamters(parameters), err
}

func loadRelatedGraphs(g graph, ID func(graph), registry *registry, loadedGraphs store, local store, dbName string) error {
	var (
		err      error
		metadata metadata
	)
	relatedGraphs := g.getRelatedGraphs()
	if g.getValue().IsValid() {
		if metadata, err = registry.get(g.getValue().Type(), dbName); err != nil {
			return err
		}
		if relatedGraphs, err = metadata.loadRelatedGraphs(g, ID, registry, dbName); err != nil {
			return err
		}
	}

	for _, relatedGraph := range relatedGraphs {
		if loadedGraphs.get(relatedGraph) == nil {
			cord := &coordinate{g.getCoordinate().depth + 1, g.getCoordinate().graphIndex}
			relatedGraph.setCoordinate(cord)
			loadedGraphs.save(relatedGraph, dbName)
		}
		g.setRelatedGraph(loadedGraphs.get(relatedGraph))
	}
	return nil
}
