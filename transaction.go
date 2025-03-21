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
	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
)

type transaction struct {
	neo4jTransaction neo4j.Transaction
	session          neo4j.Session
	close            transactionEnder
	dbName           string
}

func newTransaction(driver neo4j.Driver, transactionEnder transactionEnder, accessMode neo4j.AccessMode, dbName string) (*transaction, error) {

	var (
		err     error
		session neo4j.Session
	)

	sessionConfig := neo4j.SessionConfig{
		AccessMode: accessMode,
	}

	if dbName != "" {
		sessionConfig.DatabaseName = dbName
	}

	session = driver.NewSession(sessionConfig)

	var neo4jtransaction neo4j.Transaction
	if neo4jtransaction, err = session.BeginTransaction(); err != nil {
		session.Close()
		return nil, err
	}

	return &transaction{
		neo4jTransaction: neo4jtransaction,
		session:          session,
		close:            transactionEnder,
		dbName:           dbName}, nil
}

func (t *transaction) run(cql string, params map[string]interface{}) (neo4j.Result, error) {
	return t.neo4jTransaction.Run(cql, params)
}

func (t *transaction) Commit() error {
	return t.neo4jTransaction.Commit()
}

func (t *transaction) RollBack() error {
	return t.neo4jTransaction.Rollback()
}

func (t *transaction) Close() error {
	return t.close()
}
