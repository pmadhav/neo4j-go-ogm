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

package gogm_test

import (
	"math"
	"sort"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j"

	. "github.com/onsi/gomega"
	gogm "github.com/pmadhav/neo4j-go-ogm"
	. "github.com/pmadhav/neo4j-go-ogm/tests/models"
)

var config = &gogm.Config{
	"bolt://localhost:7687",
	"neo4j",
	"Pass1234",
	gogm.DEBUG,
	true,
}

var dbName string = ""

var ogm = gogm.New(config)
var session, _ = ogm.NewSession(true)

const deletedID int64 = -1

var eventListener = &TestEventListener{}

var loadOptions *gogm.LoadOptions = gogm.NewLoadOptions(dbName)
var saveOptions *gogm.SaveOptions = gogm.NewSaveOptions(dbName, math.MaxInt32/2)
var deleteOptions *gogm.DeleteOptions = gogm.NewDeleteOptions(dbName)

//Context: when simple node object is saved
//Spec: should have metadata properties updated
func TestNodeSave(t *testing.T) {
	g := NewGomegaWithT(t)
	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.RegisterEventListener(eventListener)).NotTo(HaveOccurred())

	//Context
	simpleNode := &SimpleNode{}
	g.Expect(session.Save(&simpleNode, saveOptions)).NotTo(HaveOccurred())

	//Spec
	g.Expect(simpleNode.CreatedAt).ToNot(BeZero())
	g.Expect(simpleNode.DeletedAt).To(BeZero())
	g.Expect(simpleNode.UpdatedAt).To(BeZero())
	g.Expect(*simpleNode.ID > -1).To(BeTrue())

	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.DisposeEventListener(eventListener)).NotTo(HaveOccurred())
}

//Context:when simple realtionship object is saved
//Spec:should have metadata properties updated
func TestRelationshipSave(t *testing.T) {
	g := NewGomegaWithT(t)
	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.RegisterEventListener(eventListener)).NotTo(HaveOccurred())

	//Context
	simpleRelationship := &SimpleRelationship{}
	n4 := &Node4{}
	n5 := &Node5{}
	simpleRelationship.N4 = n4
	simpleRelationship.N5 = n5

	g.Expect(session.Save(&simpleRelationship, saveOptions)).NotTo(HaveOccurred())

	//Spec
	g.Expect(simpleRelationship.CreatedAt).ToNot(BeZero())
	g.Expect(simpleRelationship.DeletedAt).To(BeZero())
	g.Expect(simpleRelationship.UpdatedAt).To(BeZero())
	g.Expect(*simpleRelationship.ID > -1).To(BeTrue())
	g.Expect(*n4.ID > -1).To(BeTrue())
	g.Expect(*n5.ID > -1).To(BeTrue())

	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.DisposeEventListener(eventListener)).NotTo(HaveOccurred())
}

func TestNodeSaveWithoutUpdate(t *testing.T) {
	g := NewGomegaWithT(t)
	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.RegisterEventListener(eventListener)).NotTo(HaveOccurred())

	//Context
	simpleNode := &SimpleNode{}
	g.Expect(session.Save(&simpleNode, saveOptions)).NotTo(HaveOccurred())
	g.Expect(session.Save(&simpleNode, saveOptions)).NotTo(HaveOccurred())

	//Spec
	g.Expect(*simpleNode.ID > -1).To(BeTrue())
	g.Expect(simpleNode.UpdatedAt).To(BeZero())

	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.DisposeEventListener(eventListener)).NotTo(HaveOccurred())
}

func TestRelationshipSaveWithoutUpdate(t *testing.T) {
	g := NewGomegaWithT(t)
	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.RegisterEventListener(eventListener)).NotTo(HaveOccurred())

	//Context
	simpleRelationship := &SimpleRelationship{}
	n5 := &Node5{}
	n4 := &Node4{}
	simpleRelationship.N4 = n4
	simpleRelationship.N5 = n5
	g.Expect(session.Save(&simpleRelationship, saveOptions)).NotTo(HaveOccurred())
	g.Expect(session.Save(&simpleRelationship, saveOptions)).NotTo(HaveOccurred())

	//Spec
	g.Expect(*simpleRelationship.ID > -1).To(BeTrue())
	g.Expect(*n4.ID > -1).To(BeTrue())
	g.Expect(*n5.ID > -1).To(BeTrue())

	g.Expect(simpleRelationship.UpdatedAt).To(BeZero())
	g.Expect(n4.UpdatedAt).To(BeZero())
	g.Expect(n5.UpdatedAt).To(BeZero())

	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.DisposeEventListener(eventListener)).NotTo(HaveOccurred())
}

func TestNodeSaveWithUpdate(t *testing.T) {
	g := NewGomegaWithT(t)
	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.RegisterEventListener(eventListener)).NotTo(HaveOccurred())

	simpleNode := &SimpleNode{}
	g.Expect(session.Save(&simpleNode, saveOptions)).NotTo(HaveOccurred())
	simpleNode.Prop1 = "test Prop"
	g.Expect(session.Save(&simpleNode, saveOptions)).NotTo(HaveOccurred())

	g.Expect(simpleNode.UpdatedAt).ToNot(BeZero())
	g.Expect(*simpleNode.ID > -1).To(BeTrue())

	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.DisposeEventListener(eventListener)).NotTo(HaveOccurred())
}

func TestRelationshipSaveWithUpdate(t *testing.T) {
	g := NewGomegaWithT(t)
	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.RegisterEventListener(eventListener)).NotTo(HaveOccurred())

	simpleRelationship1 := &SimpleRelationship{}
	n5 := &Node5{}
	n4 := &Node4{}
	simpleRelationship1.N4 = n4
	simpleRelationship1.N5 = n5

	simpleRelationship2 := &SimpleRelationship{}
	simpleRelationship2.N4 = n4
	simpleRelationship2.N5 = n5

	simpleRelationships := []*SimpleRelationship{simpleRelationship1, simpleRelationship2}

	g.Expect(session.Save(&simpleRelationships, saveOptions)).NotTo(HaveOccurred())
	simpleRelationship1.Name = "test Prop"
	g.Expect(session.Save(&simpleRelationship1, saveOptions)).NotTo(HaveOccurred())

	g.Expect(simpleRelationship1.UpdatedAt).ToNot(BeZero())
	g.Expect(*simpleRelationship1.ID > -1).To(BeTrue())

	g.Expect(simpleRelationship2.UpdatedAt).To(BeZero())
	g.Expect(*simpleRelationship2.ID > -1).To(BeTrue())

	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.DisposeEventListener(eventListener)).NotTo(HaveOccurred())
}

func TestSaveSliceOfNode(t *testing.T) {
	g := NewGomegaWithT(t)
	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.RegisterEventListener(eventListener)).NotTo(HaveOccurred())

	simpleNode1 := SimpleNode{}
	simpleNode2 := SimpleNode{}

	simpleNodes := []*SimpleNode{&simpleNode1, &simpleNode2}
	g.Expect(session.Save(&simpleNodes, saveOptions)).NotTo(HaveOccurred())

	g.Expect(simpleNode1.CreatedAt).NotTo(BeZero())
	g.Expect(simpleNode1.DeletedAt).To(BeZero())
	g.Expect(simpleNode1.UpdatedAt).To(BeZero())
	g.Expect(*simpleNode1.ID > -1).To(BeTrue())
	g.Expect(simpleNode2.CreatedAt).NotTo(BeZero())
	g.Expect(simpleNode2.DeletedAt).To(BeZero())
	g.Expect(simpleNode2.UpdatedAt).To(BeZero())
	g.Expect(*simpleNode2.ID > -1).To(BeTrue())

	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.DisposeEventListener(eventListener)).NotTo(HaveOccurred())
}

func TestSaveSliceOfRelationship(t *testing.T) {
	g := NewGomegaWithT(t)
	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.RegisterEventListener(eventListener)).NotTo(HaveOccurred())

	simpleRelationship1 := SimpleRelationship{}
	n5 := &Node5{}
	n4 := &Node4{}
	simpleRelationship1.N4 = n4
	simpleRelationship1.N5 = n5

	simpleRelationship2 := SimpleRelationship{}
	simpleRelationship2.N4 = n4
	simpleRelationship2.N5 = n5

	simpleRelationships := []*SimpleRelationship{&simpleRelationship1, &simpleRelationship2}

	g.Expect(session.Save(&simpleRelationships, saveOptions)).NotTo(HaveOccurred())

	g.Expect(simpleRelationship1.CreatedAt).NotTo(BeZero())
	g.Expect(simpleRelationship1.DeletedAt).To(BeZero())
	g.Expect(simpleRelationship1.UpdatedAt).To(BeZero())
	g.Expect(*simpleRelationship1.ID > -1).To(BeTrue())

	g.Expect(simpleRelationship2.CreatedAt).NotTo(BeZero())
	g.Expect(simpleRelationship2.DeletedAt).To(BeZero())
	g.Expect(simpleRelationship2.UpdatedAt).To(BeZero())
	g.Expect(*simpleRelationship2.ID > -1).To(BeTrue())

	g.Expect(n4.CreatedAt).NotTo(BeZero())
	g.Expect(n4.DeletedAt).To(BeZero())
	g.Expect(n4.UpdatedAt).To(BeZero())
	g.Expect(*n4.ID > -1).To(BeTrue())

	g.Expect(n5.CreatedAt).NotTo(BeZero())
	g.Expect(n5.DeletedAt).To(BeZero())
	g.Expect(n5.UpdatedAt).To(BeZero())
	g.Expect(*n5.ID > -1).To(BeTrue())

	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.DisposeEventListener(eventListener)).NotTo(HaveOccurred())
}

func TestNodeDelete(t *testing.T) {
	g := NewGomegaWithT(t)
	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.RegisterEventListener(eventListener)).NotTo(HaveOccurred())

	simpleNode := &SimpleNode{}
	g.Expect(session.Save(&simpleNode, saveOptions)).NotTo(HaveOccurred())
	g.Expect(session.Delete(&simpleNode, deleteOptions)).NotTo(HaveOccurred())
	simpleNode.Prop1 = "test"
	g.Expect(session.Save(&simpleNode, saveOptions)).NotTo(HaveOccurred())

	g.Expect(simpleNode.CreatedAt).ToNot(BeZero())
	g.Expect(*simpleNode.ID).To(Equal(deletedID), "Deleted node isn't re-saved")

	g.Expect(simpleNode.DeletedAt).ToNot(BeZero())

	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.DisposeEventListener(eventListener)).NotTo(HaveOccurred())
}

func TestRelationshipDelete(t *testing.T) {
	g := NewGomegaWithT(t)
	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.RegisterEventListener(eventListener)).NotTo(HaveOccurred())

	simpleRelationship := &SimpleRelationship{}
	n5 := &Node5{}
	n4 := &Node4{}
	simpleRelationship.N4 = n4
	simpleRelationship.N5 = n5
	g.Expect(session.Save(&simpleRelationship, saveOptions)).NotTo(HaveOccurred())
	g.Expect(session.Delete(&simpleRelationship, deleteOptions)).NotTo(HaveOccurred())
	simpleRelationship.Name = "test"

	g.Expect(*simpleRelationship.ID).To(Equal(deletedID), "Deleted relationship isn't re-saved")
	g.Expect(*n4.ID > deletedID).To(BeTrue())
	g.Expect(*n5.ID > deletedID).To(BeTrue())

	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.DisposeEventListener(eventListener)).NotTo(HaveOccurred())
}

//Context: When a node object is removed from a parent node entity
//Spec: Corresponding relationship should be removed
func TestRemoveNode(t *testing.T) {
	g := NewGomegaWithT(t)
	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.DisposeEventListener(eventListener)).NotTo(HaveOccurred())

	lo := gogm.NewLoadOptions(dbName)
	lo.Depth = -1

	n3 := &Node3{}
	n4 := &Node4{}
	n4.N3 = n3

	n4.Name = "N4"
	n3.Name = "N3"
	g.Expect(session.Save(&n4, saveOptions)).NotTo(HaveOccurred())
	var loadedN4 *Node4
	var loadedN3 *Node3

	n4.N3 = nil
	g.Expect(session.Save(&n4, saveOptions)).NotTo(HaveOccurred())

	g.Expect(session.Clear()).NotTo(HaveOccurred())

	g.Expect(session.Load(&loadedN4, *n4.ID, lo)).NotTo(HaveOccurred())
	g.Expect(session.Load(&loadedN3, *n3.ID, lo)).NotTo(HaveOccurred())

	g.Expect(loadedN4).To(Equal(n4))
	g.Expect(loadedN3).To(Equal(n3))
	g.Expect(*loadedN4).To(Equal(*n4))
	g.Expect(*loadedN3).To(Equal(*n3))

	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.DisposeEventListener(eventListener)).NotTo(HaveOccurred())
}

func TestDeleteRelationshipEndpoint(t *testing.T) {
	g := NewGomegaWithT(t)
	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.RegisterEventListener(eventListener)).NotTo(HaveOccurred())

	simpleRelationship := &SimpleRelationship{}

	n3 := &Node3{}
	n4 := &Node4{}
	n5 := &Node5{}

	n4.N3 = n3

	simpleRelationship.N4 = n4
	simpleRelationship.N5 = n5

	g.Expect(session.Save(&simpleRelationship, saveOptions)).NotTo(HaveOccurred())
	g.Expect(session.Delete(&n4, deleteOptions)).NotTo(HaveOccurred())

	//Spec
	g.Expect(n4.DeletedAt).NotTo(BeZero())
	g.Expect(simpleRelationship.DeletedAt).NotTo(BeZero())
	g.Expect(*n4.ID).To(Equal(deletedID))
	g.Expect(*simpleRelationship.ID).To(Equal(deletedID))
	g.Expect(*n5.ID > -1).To(BeTrue())
	g.Expect(*n3.ID > -1).To(BeTrue())

	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.DisposeEventListener(eventListener)).NotTo(HaveOccurred())
}

func TestDeleteRelationship(t *testing.T) {
	g := NewGomegaWithT(t)
	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.RegisterEventListener(eventListener)).NotTo(HaveOccurred())

	simpleRelationship := &SimpleRelationship{}

	n3 := &Node3{}
	n4 := &Node4{}
	n5 := &Node5{}

	n4.N3 = n3

	simpleRelationship.N4 = n4
	simpleRelationship.N5 = n5

	g.Expect(session.Save(&simpleRelationship, saveOptions)).NotTo(HaveOccurred())
	g.Expect(session.Delete(&simpleRelationship, deleteOptions)).NotTo(HaveOccurred())

	//Spec
	g.Expect(n4.DeletedAt).To(BeZero())
	g.Expect(n4.UpdatedAt).ToNot(BeZero())
	g.Expect(n5.DeletedAt).To(BeZero())
	g.Expect(n5.UpdatedAt).ToNot(BeZero())
	g.Expect(simpleRelationship.DeletedAt).NotTo(BeZero())
	g.Expect(*n4.ID > deletedID).To(BeTrue())
	g.Expect(*simpleRelationship.ID).To(Equal(deletedID))
	g.Expect(*n5.ID > -1).To(BeTrue())
	g.Expect(*n3.ID > -1).To(BeTrue())

	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.DisposeEventListener(eventListener)).NotTo(HaveOccurred())
}

func TestDeleteAllNodes(t *testing.T) {
	g := NewGomegaWithT(t)
	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.RegisterEventListener(eventListener)).NotTo(HaveOccurred())

	n4_1 := &Node4{}
	n4_2 := &Node4{}
	n4_3 := &Node4{}
	n4_4 := &Node4{}
	n4_5 := &Node4{}
	n5 := &Node5{}
	simpleRelationship := &SimpleRelationship{}
	simpleRelationship.N4 = n4_5
	simpleRelationship.N5 = n5
	n4s := [4]*Node4{n4_1, n4_2, n4_3, n4_4}
	g.Expect(session.Save(&n4s, saveOptions)).NotTo(HaveOccurred())
	g.Expect(session.Save(&simpleRelationship, saveOptions)).NotTo(HaveOccurred())

	n4Ref := &Node4{}
	countOfN4, err := session.CountEntitiesOfType(loadOptions, &n4Ref)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(session.DeleteAll(&n4Ref, deleteOptions)).NotTo(HaveOccurred())
	postDeleteCountOfN5, err := session.CountEntitiesOfType(loadOptions, &n4Ref)
	g.Expect(err).NotTo(HaveOccurred())

	g.Expect(countOfN4).To(Equal(int64(5)))
	g.Expect(postDeleteCountOfN5).To(Equal(int64(0)))
	for _, n4 := range n4s {
		g.Expect(*n4.ID).To(Equal(deletedID))
	}
	g.Expect(*simpleRelationship.ID).To(Equal(deletedID))
	g.Expect(simpleRelationship.DeletedAt).NotTo(BeZero(), "Deleting n4_5 should delete this related relationship")
	g.Expect(*n5.ID > deletedID).To(BeTrue())

	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.DisposeEventListener(eventListener)).NotTo(HaveOccurred())
}

func TestDeleteAllRelationships(t *testing.T) {
	g := NewGomegaWithT(t)
	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.RegisterEventListener(eventListener)).NotTo(HaveOccurred())

	simpleRelationship0 := &SimpleRelationship{}
	simpleRelationship1 := &SimpleRelationship{}
	simpleRelationships := [2]*SimpleRelationship{simpleRelationship0, simpleRelationship1}

	n4_0 := &Node4{}
	n5_0 := &Node5{}
	simpleRelationship0.N4 = n4_0
	simpleRelationship0.N5 = n5_0

	n4_1 := &Node4{}
	n5_1 := &Node5{}
	simpleRelationship1.N4 = n4_1
	simpleRelationship1.N5 = n5_1

	g.Expect(session.Save(&simpleRelationships, saveOptions)).NotTo(HaveOccurred())
	g.Expect(session.Clear()).NotTo(HaveOccurred())

	simpleRelationshipRef := &SimpleRelationship{}
	countOfSimpleRelationships, err := session.CountEntitiesOfType(loadOptions, &simpleRelationshipRef)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(session.DeleteAll(&simpleRelationshipRef, deleteOptions)).NotTo(HaveOccurred())
	countOfSimpleRelationshipsPostDelete, _ := session.CountEntitiesOfType(loadOptions, &simpleRelationshipRef)

	g.Expect(countOfSimpleRelationships).To(Equal(int64(2)))
	g.Expect(countOfSimpleRelationshipsPostDelete).To(BeZero())

	g.Expect(*n4_0.ID > -1).To(BeTrue())
	g.Expect(*n5_0.ID > -1).To(BeTrue())
	g.Expect(*n4_1.ID > -1).To(BeTrue())
	g.Expect(*n5_1.ID > -1).To(BeTrue())

	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.DisposeEventListener(eventListener)).NotTo(HaveOccurred())
}

func TestSaveMultiSourceRelationship(t *testing.T) {
	g := NewGomegaWithT(t)
	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.RegisterEventListener(eventListener)).NotTo(HaveOccurred())

	theMatrix := &Movie{}
	theMatrix.Title = "The Matrix"
	theMatrix.Released = 1999
	theMatrix.Tagline = "Welcome to the Real World"

	carrieAnne := &Actor{}
	carrieAnne.Name = "Carrie-Anne Moss"
	carrieAnne.Born = 1967

	keanu := &Actor{}
	keanu.Name = "Keanu Reeves"
	keanu.Born = 1964

	carrieAnneMatrixCharacter := &Character{Movie: theMatrix, Actor: carrieAnne, Roles: []string{"Trinity"}, Name: "carrieAnneMatrixCharacter"}
	keanuReevesMatrixCharacter := &Character{Movie: theMatrix, Actor: keanu, Roles: []string{"Neo"}, Name: "keanuReevesMatrixCharacter"}

	theMatrix.AddCharacter(carrieAnneMatrixCharacter)
	g.Expect(session.Save(&keanuReevesMatrixCharacter, saveOptions)).NotTo(HaveOccurred())

	g.Expect(session.Clear()).NotTo(HaveOccurred())

	lo := gogm.NewLoadOptions(dbName)
	lo.Depth = -1
	var loadedTheMatrix *Movie
	g.Expect(session.Load(&loadedTheMatrix, *theMatrix.ID, lo)).NotTo(HaveOccurred())

	g.Expect(len(loadedTheMatrix.Characters)).To(Equal(2))

	g.Expect(*loadedTheMatrix.Characters[0].ID).To(Equal(*carrieAnneMatrixCharacter.ID))
	g.Expect(*loadedTheMatrix.Characters[1].ID).To(Equal(*keanuReevesMatrixCharacter.ID))

	g.Expect(*loadedTheMatrix.Characters[0].Actor.ID).To(Equal(*carrieAnne.ID))
	g.Expect(*loadedTheMatrix.Characters[1].Actor.ID).To(Equal(*keanu.ID))

	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.DisposeEventListener(eventListener)).NotTo(HaveOccurred())
}

func TestLoadingSameNodeTypesNotNavigableInBothDir(t *testing.T) {
	g := NewGomegaWithT(t)
	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.RegisterEventListener(eventListener)).NotTo(HaveOccurred())

	jamesThompson := &Person{}
	jamesThompson.Name = "James Thompson"
	jamesThompson.Tags = []string{"James", "Followee"}

	jessicaThompson := &Person{}
	jessicaThompson.Name = "Jessica Thompson"
	jessicaThompson.Tags = []string{"Jessica", "Follower"}

	angelaScope := &Person{}
	angelaScope.Name = "Angela Scope"
	angelaScope.Tags = []string{"Angela", "Followee"}

	jessicaThompson.Follows = append(jessicaThompson.Follows, jamesThompson, angelaScope)

	g.Expect(session.Save(&jessicaThompson, saveOptions)).NotTo(HaveOccurred())

	g.Expect(session.Clear()).NotTo(HaveOccurred())
	var loadedJessicaThompson, loadedJamesThompson, loadedAngelaScope *Person
	lo := gogm.NewLoadOptions(dbName)
	lo.Depth = -1
	g.Expect(session.Load(&loadedJessicaThompson, *jessicaThompson.ID, lo)).NotTo(HaveOccurred())
	g.Expect(session.Load(&loadedJamesThompson, *jamesThompson.ID, lo)).NotTo(HaveOccurred())
	g.Expect(session.Load(&loadedAngelaScope, *angelaScope.ID, lo)).NotTo(HaveOccurred())

	g.Expect(loadedJessicaThompson).NotTo(BeNil())
	g.Expect(loadedJamesThompson).NotTo(BeNil())
	g.Expect(loadedAngelaScope).NotTo(BeNil())

	g.Expect(len(loadedJessicaThompson.Follows)).To(Equal(2))
	g.Expect(*loadedJessicaThompson.Follows[0].ID).To(Equal(*angelaScope.ID))
	g.Expect(*loadedJessicaThompson.Follows[1].ID).To(Equal(*jamesThompson.ID))

	g.Expect(len(loadedJamesThompson.Follows)).To(BeZero())
	g.Expect(len(loadedAngelaScope.Follows)).To(BeZero())

	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.DisposeEventListener(eventListener)).NotTo(HaveOccurred())
}

func TestLoadingSameNodeTypesNavigableInBothDir(t *testing.T) {
	g := NewGomegaWithT(t)
	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.RegisterEventListener(eventListener)).NotTo(HaveOccurred())

	jamesThompson := &Person2{}
	jamesThompson.Name = "James Thompson"

	jessicaThompson := &Person2{}
	jessicaThompson.Name = "Jessica Thompson"

	angelaScope := &Person2{}
	angelaScope.Name = "Angela Scope"

	jessicaThompson.Follows = append(jessicaThompson.Follows, jamesThompson, angelaScope)

	g.Expect(session.Save(&jessicaThompson, saveOptions)).NotTo(HaveOccurred())

	g.Expect(session.Clear()).NotTo(HaveOccurred())
	var loadedJessicaThompson, loadedJamesThompson, loadedAngelaScope *Person2
	lo := gogm.NewLoadOptions(dbName)
	lo.Depth = -1
	g.Expect(session.Load(&loadedJessicaThompson, *jessicaThompson.ID, lo)).NotTo(HaveOccurred())
	g.Expect(session.Load(&loadedJamesThompson, *jamesThompson.ID, lo)).NotTo(HaveOccurred())
	g.Expect(session.Load(&loadedAngelaScope, *angelaScope.ID, lo)).NotTo(HaveOccurred())

	g.Expect(loadedJessicaThompson).NotTo(BeNil())
	g.Expect(loadedJamesThompson).NotTo(BeNil())
	g.Expect(loadedAngelaScope).NotTo(BeNil())

	g.Expect(len(loadedJessicaThompson.Follows)).To(Equal(2))
	g.Expect(*loadedJessicaThompson.Follows[0].ID).To(Equal(*angelaScope.ID))
	g.Expect(*loadedJessicaThompson.Follows[1].ID).To(Equal(*jamesThompson.ID))

	g.Expect(len(loadedJamesThompson.Follows)).To(Equal(1))
	g.Expect(*loadedJamesThompson.Follows[0].ID).To(Equal(*loadedJessicaThompson.ID))

	g.Expect(len(loadedAngelaScope.Follows)).To(Equal(1))
	g.Expect(*loadedAngelaScope.Follows[0].ID).To(Equal(*loadedJessicaThompson.ID))

	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.DisposeEventListener(eventListener)).NotTo(HaveOccurred())
}

//Context:when path is saved
//Spec:should be able to log full path
func TestFullPathSaveByOGMIsLoadable(t *testing.T) {
	g := NewGomegaWithT(t)
	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.DisposeEventListener(eventListener)).NotTo(HaveOccurred())

	//(node0)-->(node1)-->(node2)<--(node3)-->(node4)
	n0 := &Node0{}
	n1 := &Node1{}
	n2 := &Node2{}
	n3 := &Node3{}
	n4 := &Node4{}

	n0.Name = "0"
	n1.Name = "1"
	n2.Name = "2"
	n3.Name = "3"
	n4.Name = "4"

	n0.N1 = n1
	n1.N2 = n2
	n2.N3 = n3
	n3.N4 = n4

	var loadedN0 *Node0
	g.Expect(session.Save(&n0, saveOptions)).NotTo(HaveOccurred())
	g.Expect(session.Clear()).NotTo(HaveOccurred())

	lo := gogm.NewLoadOptions(dbName)
	lo.Depth = -1
	g.Expect(session.Load(&loadedN0, *n0.ID, lo)).NotTo(HaveOccurred())

	g.Expect(loadedN0).ToNot(BeNil())
	g.Expect(*loadedN0.ID).To(Equal(*n0.ID))

	g.Expect(*loadedN0.N1.ID).To(Equal(*n1.ID))

	g.Expect(*loadedN0.N1.N2.ID).To(Equal(*n2.ID))

	g.Expect(*loadedN0.N1.N2.N3.ID).To(Equal(*n3.ID))

	g.Expect(*loadedN0.N1.N2.N3.N4.ID).To(Equal(*n4.ID))

	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.DisposeEventListener(eventListener)).NotTo(HaveOccurred())
}

//Context: when path is saved
//Spec: should be able to load path up to depth x
func TestPathSaveByOGMIsLoadable(t *testing.T) {

	g := NewGomegaWithT(t)
	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.DisposeEventListener(eventListener)).NotTo(HaveOccurred())

	//(node0)-->(node1)-->(node2)<--(node3)-->(node4)
	n0 := &Node0{}
	n1 := &Node1{}
	n2 := &Node2{}
	n3 := &Node3{}
	n4 := &Node4{}

	n0.Name = "0"
	n1.Name = "1"
	n2.Name = "2"
	n3.Name = "3"
	n4.Name = "4"

	n0.N1 = n1
	n1.N2 = n2
	n2.N3 = n3
	n3.N4 = n4

	var loadedN0 *Node0
	g.Expect(session.Save(&n0, saveOptions)).NotTo(HaveOccurred())
	g.Expect(session.Clear()).NotTo(HaveOccurred())

	lo := gogm.NewLoadOptions(dbName)
	lo.Depth = 2
	g.Expect(session.Load(&loadedN0, *n0.ID, lo)).NotTo(HaveOccurred())

	g.Expect(loadedN0).ToNot(BeNil())
	g.Expect(*loadedN0.ID).To(Equal(*n0.ID))

	g.Expect(*loadedN0.N1.ID).To(Equal(*n1.ID))

	g.Expect(*loadedN0.N1.N2.ID).To(Equal(*n2.ID))

	g.Expect(loadedN0.N1.N2.N3).To(BeNil())

	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.DisposeEventListener(eventListener)).NotTo(HaveOccurred())

}

func TestLoadFromLocalStore(t *testing.T) {

	g := NewGomegaWithT(t)
	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.DisposeEventListener(eventListener)).NotTo(HaveOccurred())

	//(node0)-->(node1)-->(node2)<--(node3)-->(node4)
	n0 := &Node0{}
	n1 := &Node1{}
	n2 := &Node2{}
	n3 := &Node3{}
	n4 := &Node4{}

	n0.Name = "0"
	n1.Name = "1"
	n2.Name = "2"
	n3.Name = "3"
	n4.Name = "4"

	n0.N1 = n1
	n1.N2 = n2
	n2.N3 = n3
	n3.N4 = n4

	g.Expect(session.Save(&n0, saveOptions)).NotTo(HaveOccurred())
	g.Expect(session.Clear()).NotTo(HaveOccurred())

	var loadedN1 *Node1
	lo := gogm.NewLoadOptions(dbName)
	lo.Depth = 2
	g.Expect(session.Load(&loadedN1, *n1.ID, lo)).NotTo(HaveOccurred())

	g.Expect(loadedN1).ToNot(BeNil())
	g.Expect(*loadedN1.ID).To(Equal(*n1.ID))

	g.Expect(*loadedN1.N0.ID).To(Equal(*n0.ID))
	g.Expect(*loadedN1.N2.ID).To(Equal(*n2.ID))

	g.Expect(*loadedN1.N2.N3.ID).To(Equal(*n3.ID))

	lo.Depth = 1
	var loadedN1_1 *Node1
	g.Expect(session.Load(&loadedN1_1, *n1.ID, lo)).NotTo(HaveOccurred())
	g.Expect(loadedN1_1.N2.N3.N4).To(BeNil())
	g.Expect(loadedN1_1).To(Equal(loadedN1))

	lo.Depth = 2
	g.Expect(session.Load(&loadedN1_1, *n1.ID, lo)).NotTo(HaveOccurred())
	g.Expect(loadedN1_1.N2.N3.N4).To(BeNil())
	g.Expect(loadedN1_1).To(Equal(loadedN1))

	lo.Depth = 3
	g.Expect(session.Load(&loadedN1_1, *n1.ID, lo)).NotTo(HaveOccurred())
	g.Expect(loadedN1_1.N2.N3.N4).ToNot(BeNil())

	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.DisposeEventListener(eventListener)).NotTo(HaveOccurred())

}

func TestSaveToDepthFromNode(t *testing.T) {

	g := NewGomegaWithT(t)
	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.DisposeEventListener(eventListener)).NotTo(HaveOccurred())

	//(node0)-->(node1)-->(node2)<--(node3)-->(node4)
	n0 := &Node0{}
	n1 := &Node1{}
	n2 := &Node2{}
	n3 := &Node3{}
	n4 := &Node4{}

	n0.Name = "0"
	n1.Name = "1"
	n2.Name = "2"
	n3.Name = "3"
	n4.Name = "4"

	n0.N1 = n1
	n1.N2 = n2
	n2.N3 = n3
	n3.N4 = n4

	so := gogm.NewSaveOptions(dbName, 0)
	g.Expect(session.Save(&n0, so)).NotTo(HaveOccurred())

	lo := gogm.NewLoadOptions(dbName)
	lo.Depth = -1
	var loadedN0 *Node0
	g.Expect(session.Clear()).NotTo(HaveOccurred())
	g.Expect(session.Load(&loadedN0, *n0.ID, lo)).NotTo(HaveOccurred())
	g.Expect(loadedN0.Name).To(Equal(n0.Name))
	g.Expect(loadedN0.N1).To(BeNil())

	so.Depth = 2
	g.Expect(session.Save(&n0, so)).NotTo(HaveOccurred())

	loadedN0 = nil
	g.Expect(session.Clear()).NotTo(HaveOccurred())
	g.Expect(session.Load(&loadedN0, *n0.ID, lo)).NotTo(HaveOccurred())
	g.Expect(loadedN0.N1.N2.Name).To(Equal(n0.N1.N2.Name))
	g.Expect(loadedN0.N1.N2.N3).To(BeNil())

	so.Depth = 0
	n0.Name = "31"
	g.Expect(session.Save(&n0, so)).NotTo(HaveOccurred())

	loadedN0 = nil
	g.Expect(session.Clear()).NotTo(HaveOccurred())
	g.Expect(session.Load(&loadedN0, *n0.ID, lo)).NotTo(HaveOccurred())
	g.Expect(loadedN0.Name).To(Equal(n0.Name))
	g.Expect(loadedN0.N1.N2.Name).To(Equal(n0.N1.N2.Name))
	g.Expect(loadedN0.N1.N2.N3).To(BeNil())

	so.Depth = 4
	g.Expect(session.Save(&n0, so)).NotTo(HaveOccurred())

	loadedN0 = nil
	g.Expect(session.Clear()).NotTo(HaveOccurred())
	g.Expect(session.Load(&loadedN0, *n0.ID, lo)).NotTo(HaveOccurred())
	g.Expect(loadedN0.N1.N2.Name).To(Equal(n0.N1.N2.Name))
	g.Expect(loadedN0.N1.N2.N3.N4.Name).To(Equal(n4.Name))

	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.DisposeEventListener(eventListener)).NotTo(HaveOccurred())
}

func TestSaveToDepthFromRelationship(t *testing.T) {
	g := NewGomegaWithT(t)
	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.DisposeEventListener(eventListener)).NotTo(HaveOccurred())

	//Context
	simpleRelationship := &SimpleRelationship{}
	n4 := &Node4{}
	n5 := &Node5{}
	simpleRelationship.N4 = n4
	simpleRelationship.N5 = n5

	n5.N4 = n4

	simpleRelationship.Name = "r1"
	simpleRelationship.TestID = "r1ID"

	n4.Name = "n4"
	n5.Name = "n5"

	so := gogm.NewSaveOptions(dbName, 0)
	g.Expect(session.Save(&simpleRelationship, so)).NotTo(HaveOccurred())

	var loadedSimpleRelationship *SimpleRelationship

	lo := gogm.NewLoadOptions(dbName)
	lo.Depth = -1
	g.Expect(session.Clear()).NotTo(HaveOccurred())
	g.Expect(session.Load(&loadedSimpleRelationship, simpleRelationship.TestID, lo)).NotTo(HaveOccurred())
	g.Expect(loadedSimpleRelationship.Name).To(Equal(simpleRelationship.Name))
	g.Expect(loadedSimpleRelationship.N5.Name).To(Equal(n5.Name))
	g.Expect(loadedSimpleRelationship.N5.N4).To(BeNil())
	g.Expect(loadedSimpleRelationship.N4.Name).To(Equal(n4.Name))

	so.Depth = 2
	g.Expect(session.Save(&simpleRelationship, so)).NotTo(HaveOccurred())

	g.Expect(session.Clear()).NotTo(HaveOccurred())
	g.Expect(session.Load(&loadedSimpleRelationship, simpleRelationship.TestID, lo)).NotTo(HaveOccurred())

	if loadedSimpleRelationship.N5.N4 == nil {
		g.Expect(loadedSimpleRelationship.N4.N5s[0].Name).To(Equal(n5.Name))
	} else {
		g.Expect(loadedSimpleRelationship.N5.N4.Name).To(Equal(n4.Name))
	}

	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.DisposeEventListener(eventListener)).NotTo(HaveOccurred())
}

func TestSaveNodeWithCustomID(t *testing.T) {
	g := NewGomegaWithT(t)
	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.DisposeEventListener(eventListener)).NotTo(HaveOccurred())

	n9 := &Node9{}
	n9.TestId = "r"
	g.Expect(session.Save(&n9, saveOptions)).NotTo(HaveOccurred())

	g.Expect(session.Clear()).NotTo(HaveOccurred())

	var loadedN9 *Node9
	lo := gogm.NewLoadOptions(dbName)
	lo.Depth = 2
	g.Expect(session.Load(&loadedN9, n9.TestId, lo)).NotTo(HaveOccurred())
	g.Expect(*loadedN9.ID).To(Equal(*n9.ID))

	var loadedN9_1 *Node9
	lo.Depth = 0
	g.Expect(session.Load(&loadedN9_1, loadedN9.TestId, lo)).NotTo(HaveOccurred())
	g.Expect(loadedN9_1 == loadedN9).To(BeTrue())

	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.DisposeEventListener(eventListener)).NotTo(HaveOccurred())
}

func TestSaveRelationshipWithCustomID(t *testing.T) {
	g := NewGomegaWithT(t)
	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.DisposeEventListener(eventListener)).NotTo(HaveOccurred())

	//Context
	r := &SimpleRelationship{}
	r.TestID = "TestID"
	n4 := &Node4{}
	n5 := &Node5{}
	r.N4 = n4
	r.N5 = n5
	g.Expect(session.Save(&r, saveOptions)).NotTo(HaveOccurred())

	g.Expect(session.Clear()).NotTo(HaveOccurred())

	var loadedR *SimpleRelationship
	lo := gogm.NewLoadOptions(dbName)
	lo.Depth = 2
	g.Expect(session.Load(&loadedR, r.TestID, lo)).NotTo(HaveOccurred())
	g.Expect(*loadedR.ID).To(Equal(*r.ID))

	var loadedR_1 *SimpleRelationship
	lo.Depth = 0
	g.Expect(session.Load(&loadedR_1, loadedR.TestID, lo)).NotTo(HaveOccurred())
	g.Expect(loadedR_1 == loadedR).To(BeTrue())

	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.DisposeEventListener(eventListener)).NotTo(HaveOccurred())
}

func TestTransactions(t *testing.T) {
	g := NewGomegaWithT(t)
	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.DisposeEventListener(eventListener)).NotTo(HaveOccurred())

	//(node0)-->(node1)-->(node2)<--(node3)-->(node4)
	n0 := &Node0{}
	n1 := &Node1{}
	n2 := &Node2{}
	n3 := &Node3{}
	n4 := &Node4{}

	n0.Name = "0"
	n1.Name = "1"
	n2.Name = "2"
	n3.Name = "3"
	n4.Name = "4"

	n0.N1 = n1
	n1.N2 = n2
	n2.N3 = n3
	n3.N4 = n4

	so := gogm.NewSaveOptions(dbName, 2)
	g.Expect(session.Save(&n0, so)).NotTo(HaveOccurred())
	g.Expect(n3.ID).To(BeNil())
	g.Expect(n4.ID).To(BeNil())

	tx, err := session.BeginTransaction(dbName)
	g.Expect(err).NotTo(HaveOccurred())

	n0.Name = "0Update"
	n2.Name = "2Update"

	//Test Rolling back
	so.Depth = 3
	g.Expect(session.Save(&n0, so)).NotTo(HaveOccurred())
	g.Expect(tx.RollBack()).NotTo(HaveOccurred())
	g.Expect(tx.Close()).NotTo(HaveOccurred())

	var loadedN0 *Node0
	lo := gogm.NewLoadOptions(dbName)
	lo.Depth = 0
	g.Expect(session.Load(&loadedN0, *n0.ID, lo)).NotTo(HaveOccurred())

	g.Expect(n0.Name).To(Equal("0Update"))
	g.Expect(loadedN0.Name).To(Equal(n0.Name))
	g.Expect(*loadedN0.N1.N2.N3.ID).To(Equal(*n3.ID), "Store cache still holds state of rolled back transcation")

	g.Expect(session.Reload(loadOptions, &n0)).NotTo(HaveOccurred(), "Reload to sycn runtime objects with backend")
	g.Expect(n0.Name).To(Equal("0"))
	g.Expect(*n3.ID).To(Equal(deletedID), "n3 gets deleted as it was rolled back. n3 can't ever be saved again. Must create new instance to save")

	//Testing committing
	n3 = &Node3{}
	n3.Name = "3"
	n0.N1.N2.N3 = n3

	n3.N4 = n4
	so.Depth = 3

	tx, err = session.BeginTransaction(dbName)
	g.Expect(err).NotTo(HaveOccurred())
	n0.Name = "0Update"
	n2.Name = "2Update"
	g.Expect(session.Save(&n0, so)).NotTo(HaveOccurred())
	g.Expect(tx.Commit()).NotTo(HaveOccurred())
	g.Expect(tx.Close()).NotTo(HaveOccurred())

	g.Expect(session.Reload(loadOptions, &n0)).NotTo(HaveOccurred())
	g.Expect(n0.Name).To(Equal("0Update"))
	g.Expect(n2.Name).To(Equal("2Update"))
	g.Expect(*n3.ID).NotTo(Equal(deletedID))

	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.DisposeEventListener(eventListener)).NotTo(HaveOccurred())
}

func TestQuery_Nodes(t *testing.T) {
	g := NewGomegaWithT(t)
	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.DisposeEventListener(eventListener)).NotTo(HaveOccurred())

	jamesThompson := &Person{}
	jamesThompson.Name = "James Thompson"
	jamesThompson.Tags = []string{"James", "Followee"}

	jessicaThompson := &Person{}
	jessicaThompson.Name = "Jessica Thompson"
	jessicaThompson.Tags = []string{"Jessica", "Follower"}

	angelaScope := &Person{}
	angelaScope.Name = "Angela Scope"
	angelaScope.Tags = []string{"Angela", "Followee"}

	jessicaThompson.Follows = append(jessicaThompson.Follows, jamesThompson, angelaScope)

	g.Expect(session.Save(&jessicaThompson, saveOptions)).NotTo(HaveOccurred())

	person := &Person{}
	rows, err := session.Query(loadOptions, "MATCH (person) RETURN person", nil, &person)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(len(rows)).To(Equal(3))

	for _, row := range rows {
		g.Expect(row["person"].(*Person).ID).NotTo(BeNil())
	}

	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.DisposeEventListener(eventListener)).NotTo(HaveOccurred())
}

func TestQuery_Node_Relationship(t *testing.T) {
	g := NewGomegaWithT(t)
	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.DisposeEventListener(eventListener)).NotTo(HaveOccurred())
	simpleRelationship1 := SimpleRelationship{}
	n5 := &Node5{}
	n4 := &Node4{}
	simpleRelationship1.N4 = n4
	simpleRelationship1.N5 = n5

	simpleRelationship2 := SimpleRelationship{}
	simpleRelationship2.N4 = n4
	simpleRelationship2.N5 = n5

	simpleRelationship3 := SimpleRelationship{}
	simpleRelationship3.N4 = n4
	simpleRelationship3.N5 = n5

	simpleRelationships := []*SimpleRelationship{&simpleRelationship1, &simpleRelationship2, &simpleRelationship3}

	g.Expect(session.Save(&simpleRelationships, saveOptions)).NotTo(HaveOccurred())
	rows, err := session.Query(loadOptions, "MATCH (n5)-[r:SIMPLERELATIONSHIP]->(n4) RETURN n4, n5, r", nil)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(len(rows)).To(Equal(3))

	for _, row := range rows {
		g.Expect(row["n4"].(*Node4).ID).NotTo(BeNil())
		g.Expect(row["n5"].(*Node5).ID).NotTo(BeNil())
		g.Expect(row["r"].(*SimpleRelationship).ID).NotTo(BeNil())
	}

	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.DisposeEventListener(eventListener)).NotTo(HaveOccurred())
}

func TestQueryForObject_s(t *testing.T) {
	g := NewGomegaWithT(t)
	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.DisposeEventListener(eventListener)).NotTo(HaveOccurred())

	var person *Person
	g.Expect(session.QueryForObject(loadOptions, &person, "MATCH (person) RETURN person", nil)).NotTo(HaveOccurred())

	g.Expect(person).To(BeNil())

	jamesThompson := &Person{}
	jamesThompson.Name = "James Thompson"
	jamesThompson.Tags = []string{"James", "Followee"}

	jessicaThompson := &Person{}
	jessicaThompson.Name = "Jessica Thompson"
	jessicaThompson.Tags = []string{"Jessica", "Follower"}

	angelaScope := &Person{}
	angelaScope.Name = "Angela Scope"
	angelaScope.Tags = []string{"Angela", "Followee"}

	jessicaThompson.Follows = append(jessicaThompson.Follows, jamesThompson, angelaScope)

	g.Expect(session.Save(&jessicaThompson, saveOptions)).NotTo(HaveOccurred())

	g.Expect(session.QueryForObject(loadOptions, &person, "MATCH (person:Person) RETURN person", nil)).To(HaveOccurred())
	g.Expect(person).To(BeNil())

	g.Expect(session.QueryForObject(loadOptions, &person, "MATCH (person:Person) WHERE person.name = $name RETURN person", map[string]interface{}{"name": "Angela Scope"})).ToNot(HaveOccurred())
	g.Expect(person).To(Equal(angelaScope))

	var persons []*Person
	g.Expect(session.QueryForObjects(loadOptions, &persons, "MATCH (person:Person) RETURN person", nil)).ToNot(HaveOccurred())
	g.Expect(len(persons)).To(Equal(3))

	sort.SliceStable(persons, func(i, j int) bool { return persons[i].Name < persons[j].Name })
	g.Expect(persons[0]).To(Equal(angelaScope))
	g.Expect(persons[1]).To(Equal(jamesThompson))

	//Note, just for comparison
	jessicaThompson.Follows = nil
	g.Expect(persons[2]).To(Equal(jessicaThompson))

	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.DisposeEventListener(eventListener)).NotTo(HaveOccurred())
}

func TestCount(t *testing.T) {
	g := NewGomegaWithT(t)
	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.DisposeEventListener(eventListener)).NotTo(HaveOccurred())

	count, err := session.Count(loadOptions, "MATCH (n:INVALID) RETURN COUNT(n)", nil)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(count).To(BeZero())

	simpleRelationship1 := SimpleRelationship{}
	n5 := &Node5{}
	n4 := &Node4{}
	simpleRelationship1.N4 = n4
	simpleRelationship1.N5 = n5

	simpleRelationship2 := SimpleRelationship{}
	simpleRelationship2.N4 = n4
	simpleRelationship2.N5 = n5

	simpleRelationships := []*SimpleRelationship{&simpleRelationship1, &simpleRelationship2}

	g.Expect(session.Save(&simpleRelationships, saveOptions)).NotTo(HaveOccurred())

	count, err = session.Count(loadOptions, "MATCH (n:Node4) RETURN COUNT(n)", nil)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(count).To(Equal(int64(1)))

	count, err = session.Count(loadOptions, "MATCH (n:Node5) RETURN COUNT(n)", nil)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(count).To(Equal(int64(1)))

	n4_1 := &Node4{}
	n4_2 := &Node4{}
	n4_3 := &Node4{}
	n4_4 := &Node4{}
	n4s := [4]*Node4{n4_1, n4_2, n4_3, n4_4}
	g.Expect(session.Save(&n4s, saveOptions)).NotTo(HaveOccurred())

	count, err = session.Count(loadOptions, "MATCH (n:Node4) RETURN COUNT(n)", nil)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(count).To(Equal(int64(5)))

	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.DisposeEventListener(eventListener)).NotTo(HaveOccurred())
}

func TestLoadAll(t *testing.T) {
	g := NewGomegaWithT(t)
	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.DisposeEventListener(eventListener)).NotTo(HaveOccurred())

	simpleRelationship1 := SimpleRelationship{}
	n5 := &Node5{}
	n4 := &Node4{}
	simpleRelationship1.N4 = n4
	simpleRelationship1.N5 = n5

	simpleRelationship2 := SimpleRelationship{}
	simpleRelationship2.N4 = n4
	simpleRelationship2.N5 = n5

	simpleRelationships := []*SimpleRelationship{&simpleRelationship1, &simpleRelationship2}
	loadContainter := []*SimpleRelationship{}

	simpleRelationship1.TestID = "simpleRelationship1"
	simpleRelationship2.TestID = "simpleRelationship2"

	g.Expect(session.Save(&simpleRelationships, saveOptions)).NotTo(HaveOccurred())
	g.Expect(session.LoadAll(&loadContainter, nil, loadOptions)).ToNot(HaveOccurred())

	g.Expect(len(loadContainter)).To(Equal(2))

	loadContainter = []*SimpleRelationship{}
	g.Expect(session.Clear()).NotTo(HaveOccurred())
	g.Expect(session.LoadAll(&loadContainter, []string{simpleRelationship1.TestID}, loadOptions)).ToNot(HaveOccurred())
	g.Expect(len(loadContainter)).To(Equal(1))

	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.DisposeEventListener(eventListener)).NotTo(HaveOccurred())
}

func TestMappedProperties(t *testing.T) {
	g := NewGomegaWithT(t)
	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.DisposeEventListener(eventListener)).NotTo(HaveOccurred())

	n0 := &Node0{}
	n0.MapProps = map[string]int{"hello": 8}
	n0.InvalidIDMapProp = map[int]int{2: 8}
	n0.MapToInterfaces = map[string]interface{}{"0": true, "1": 2.3, "2": "hello"}

	e := "hello"
	y := ",world"
	n0.AliasedMapProps1 = map[string]*string{"hello": &e, "world": &y}

	g.Expect(session.Save(&n0, saveOptions)).NotTo(HaveOccurred())
	g.Expect(session.Save(&n0, saveOptions)).NotTo(HaveOccurred())

	g.Expect(session.Clear()).NotTo(HaveOccurred())

	var loadedN0 *Node0
	g.Expect(session.Load(&loadedN0, *n0.ID, loadOptions)).NotTo(HaveOccurred())

	g.Expect(n0.UpdatedAt).To(BeZero())
	g.Expect(loadedN0.MapProps).To(Equal(n0.MapProps))
	g.Expect(loadedN0.InvalidIDMapProp).To(BeNil())
	g.Expect(loadedN0.MapToInterfaces).To(Equal(n0.MapToInterfaces))

	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.DisposeEventListener(eventListener)).NotTo(HaveOccurred())
}

func TestSavingAndLoadingTime(t *testing.T) {
	g := NewGomegaWithT(t)
	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.RegisterEventListener(eventListener)).NotTo(HaveOccurred())

	n10 := &Node10{}
	location, _ := time.LoadLocation("")
	tValue := time.Now().In(location)
	durationValue := neo4j.DurationOf(2, 3, 6, 7)

	n10.Time = tValue
	n10.Duration = durationValue
	g.Expect(session.Save(&n10, saveOptions)).NotTo(HaveOccurred())
	g.Expect(session.Save(&n10, saveOptions)).NotTo(HaveOccurred())
	g.Expect(n10.UpdatedAt).To(BeZero())

	n10.Duration = neo4j.DurationOf(4, 3, 6, 7)
	g.Expect(session.Save(&n10, saveOptions)).NotTo(HaveOccurred())
	g.Expect(n10.UpdatedAt).NotTo(BeZero())

	var loadedN10 *Node10
	g.Expect(session.Load(&loadedN10, *n10.ID, loadOptions)).NotTo(HaveOccurred())
	n10.ClearMetaTimestamps()
	g.Expect(loadedN10).To(Equal(n10))
	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.DisposeEventListener(eventListener)).NotTo(HaveOccurred())
}

func TestByteProperty(t *testing.T) {
	g := NewGomegaWithT(t)
	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.RegisterEventListener(eventListener)).NotTo(HaveOccurred())
	n0 := &Node0{}
	n0.ByteProp = []byte("seafood")
	g.Expect(session.Save(&n0, saveOptions)).NotTo(HaveOccurred())

	//update
	n0_1 := &Node0{}
	n0_1.ID = n0.ID
	n0_1.ByteProp = []byte("sea")
	g.Expect(session.Save(&n0_1, saveOptions)).NotTo(HaveOccurred())
	g.Expect(n0_1.UpdatedAt).NotTo(BeZero())
	g.Expect(session.Clear()).NotTo(HaveOccurred())

	var loadedN0 *Node0
	g.Expect(session.Load(&loadedN0, *n0.ID, loadOptions)).NotTo(HaveOccurred())

	n0_1.ClearMetaTimestamps()
	g.Expect(loadedN0).To(Equal(n0_1))
	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.DisposeEventListener(eventListener)).NotTo(HaveOccurred())
}

func TestRichRelationshipSameNodes(t *testing.T) {
	g := NewGomegaWithT(t)
	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.RegisterEventListener(eventListener)).NotTo(HaveOccurred())

	n51 := &Node5{}
	n52 := &Node5{}

	n51.Name = "51"
	n52.Name = "52"

	simpleRelationshipSameNode := &SimpleRelationshipSameNode{}
	simpleRelationshipSameNode.N51 = n51
	simpleRelationshipSameNode.N52 = n52

	g.Expect(session.Save(&simpleRelationshipSameNode, saveOptions)).NotTo(HaveOccurred())
	g.Expect(session.Clear()).NotTo(HaveOccurred())

	var loadedSimpleRelationshipSameNode *SimpleRelationshipSameNode
	g.Expect(session.Load(&loadedSimpleRelationshipSameNode, *simpleRelationshipSameNode.ID, loadOptions)).NotTo(HaveOccurred())

	simpleRelationshipSameNode.ClearMetaTimestamps()
	n51.ClearMetaTimestamps()
	n52.ClearMetaTimestamps()

	g.Expect(loadedSimpleRelationshipSameNode.N51.Name).To(Equal("51"))
	g.Expect(loadedSimpleRelationshipSameNode.N52.Name).To(Equal("52"))

	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.DisposeEventListener(eventListener)).NotTo(HaveOccurred())
}

func TestNodeEmbedRichRelationshipWithSameNodes(t *testing.T) {
	g := NewGomegaWithT(t)
	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.RegisterEventListener(eventListener)).NotTo(HaveOccurred())

	n51 := &Node5{}
	n52 := &Node5{}

	n51.Name = "51"
	n52.Name = "52"

	simpleRelationshipSameNode := &SimpleRelationshipSameNode{}
	simpleRelationshipSameNode.N51 = n51
	simpleRelationshipSameNode.N52 = n52

	n51.R2 = simpleRelationshipSameNode

	g.Expect(session.Save(&n51, saveOptions)).NotTo(HaveOccurred())
	g.Expect(session.Clear()).NotTo(HaveOccurred())

	var loadedN51 *Node5
	g.Expect(session.Load(&loadedN51, *n51.ID, loadOptions)).NotTo(HaveOccurred())

	simpleRelationshipSameNode.ClearMetaTimestamps()
	n51.ClearMetaTimestamps()
	n52.ClearMetaTimestamps()

	g.Expect(n51.Name).To(Equal(loadedN51.Name))
	g.Expect(n51.R2.Name).To(Equal(loadedN51.R2.Name))
	g.Expect(n51.R2.N51.Name).To(Equal(loadedN51.R2.N51.Name))
	g.Expect(n51.R2.N52.Name).To(Equal(loadedN51.R2.N52.Name))

	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.DisposeEventListener(eventListener)).NotTo(HaveOccurred())
}

//Error cases
func TestSaveNodeWithInvalidCustomID(t *testing.T) {
	g := NewGomegaWithT(t)
	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.DisposeEventListener(eventListener)).NotTo(HaveOccurred())

	invalidID := &InvalidID{}
	testID := "r"
	invalidID.TestId = &testID
	g.Expect(session.Save(invalidID, saveOptions)).To(HaveOccurred())

	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.DisposeEventListener(eventListener)).NotTo(HaveOccurred())
}

func TestForbiddenLabel(t *testing.T) {
	g := NewGomegaWithT(t)
	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.RegisterEventListener(eventListener)).NotTo(HaveOccurred())

	angelaScope := &Person{}
	angelaScope.Name = "Angela Scope"
	angelaScope.Tags = []string{"Angela"}

	g.Expect(session.Save(&angelaScope, saveOptions)).ToNot(HaveOccurred())
	g.Expect(angelaScope.UpdatedAt).To(BeZero())

	angelaScope.Tags = []string{"Ana"}
	g.Expect(session.Save(&angelaScope, saveOptions)).ToNot(HaveOccurred())
	g.Expect(angelaScope.UpdatedAt).NotTo(BeZero())

	angelaScope.Tags = []string{"Ana", "Person"}
	g.Expect(session.Save(&angelaScope, saveOptions)).To(HaveOccurred())

	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.DisposeEventListener(eventListener)).NotTo(HaveOccurred())
}

func TestQueryForObject_fail(t *testing.T) {
	g := NewGomegaWithT(t)
	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.DisposeEventListener(eventListener)).NotTo(HaveOccurred())

	jamesThompson := &Person{}
	jamesThompson.Name = "James Thompson"
	jamesThompson.Tags = []string{"James", "Followee"}

	jessicaThompson := &Person{}
	jessicaThompson.Name = "Jessica Thompson"
	jessicaThompson.Tags = []string{"Jessica", "Follower"}

	angelaScope := &Person{}
	angelaScope.Name = "Angela Scope"
	angelaScope.Tags = []string{"Angela", "Followee"}

	jessicaThompson.Follows = append(jessicaThompson.Follows, jamesThompson, angelaScope)

	g.Expect(session.Save(&jessicaThompson, saveOptions)).NotTo(HaveOccurred())
	session.Clear()
	var loadedjessicaThompson *Person
	g.Expect(session.Load(&loadedjessicaThompson, *jessicaThompson.ID, loadOptions)).NotTo(HaveOccurred())

	var relationships []*SimpleRelationship
	g.Expect(session.QueryForObjects(loadOptions, &relationships, "MATCH (person:Person) RETURN person", nil)).To(HaveOccurred())

	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.DisposeEventListener(eventListener)).NotTo(HaveOccurred())
}

func TestMoreThanOneEntityWithSameLabel(t *testing.T) {
	g := NewGomegaWithT(t)
	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.DisposeEventListener(eventListener)).NotTo(HaveOccurred())

	n1 := &Node1{}
	n1Prime := &Node1Prime{}

	g.Expect(session.Save(&n1, saveOptions)).NotTo(HaveOccurred())
	g.Expect(session.Save(&n1Prime, saveOptions)).To(HaveOccurred(), "Can't have  2 structs with the same labels")

	simpleRelationship := &SimpleRelationship{}
	n4 := &Node4{}
	n5 := &Node5{}
	simpleRelationship.N4 = n4
	simpleRelationship.N5 = n5

	simpleRelationshipPrime := &SimpleRelationshipPrime{}
	simpleRelationshipPrime.N4 = n4
	simpleRelationshipPrime.N5 = n5
	g.Expect(session.Save(&simpleRelationship, saveOptions)).NotTo(HaveOccurred())
	g.Expect(session.Save(&simpleRelationshipPrime, saveOptions)).To(HaveOccurred(), "Can't have  2 structs with the same relaionship type")

	g.Expect(session.PurgeDatabase(deleteOptions)).NotTo(HaveOccurred())
	g.Expect(session.DisposeEventListener(eventListener)).NotTo(HaveOccurred())
}
