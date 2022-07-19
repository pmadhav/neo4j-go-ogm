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
	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j/db"
)

type transactionExecuter func(work neo4j.TransactionWork, configurers ...func(*neo4j.TransactionConfig)) (interface{}, error)

type cypherExecuter struct {
	driver      neo4j.Driver
	accessMode  neo4j.AccessMode
	transaction *transaction
}

func newCypherExecuter(driver neo4j.Driver, accessMode neo4j.AccessMode, t *transaction) *cypherExecuter {
	return &cypherExecuter{driver, accessMode, nil}
}

func (c *cypherExecuter) execTransaction(te transactionExecuter, cql string, params map[string]interface{}) (neo4j.Result, error) {
	var (
		err    error
		result neo4j.Result
	)

	if _, err = te(func(tx neo4j.Transaction) (interface{}, error) {
		if result, err = tx.Run(cql, params); err != nil {
			return nil, err
		}
		return result, nil
	}); err != nil {
		return nil, err
	}

	return result, nil
}

func (c *cypherExecuter) execTransactionCollect(te transactionExecuter, cql string, params map[string]interface{}) (interface{}, error) {
	var (
		err     error
		result  neo4j.Result
		records interface{}
	)

	if records, err = te(func(tx neo4j.Transaction) (interface{}, error) {
		if result, err = tx.Run(cql, params); err != nil {
			return nil, err
		}
		return result.Collect()
	}); err != nil {
		return nil, err
	}

	return records, nil
}

func (c *cypherExecuter) execTransactionSingle(te transactionExecuter, cql string, params map[string]interface{}) (interface{}, error) {
	var (
		err    error
		result neo4j.Result
		record interface{}
	)

	if record, err = te(func(tx neo4j.Transaction) (interface{}, error) {
		if result, err = tx.Run(cql, params); err != nil {
			return nil, err
		}
		return result.Single()
	}); err != nil {
		return nil, err
	}

	return record, nil
}

func (c *cypherExecuter) exec(dbName string, cql string, params map[string]interface{}, single bool, collect bool) (interface{}, error) {
	var (
		result   interface{}
		txResult neo4j.Result
		session  neo4j.Session
		err      error
	)
	if c.transaction != nil {
		if txResult, err = c.transaction.run(cql, params); err != nil {
			return nil, err
		}

		if single {
			return txResult.Single()
		} else if collect {
			return txResult.Collect()
		}
		return txResult, nil
	}

	sessionConfig := neo4j.SessionConfig{
		AccessMode: c.accessMode,
	}

	if dbName != "" {
		sessionConfig.DatabaseName = dbName
	}

	session = c.driver.NewSession(sessionConfig)
	defer session.Close()
	transactionMode := session.ReadTransaction
	if c.accessMode == neo4j.AccessModeWrite {
		transactionMode = session.WriteTransaction
	}

	if single {
		result, err = c.execTransactionSingle(transactionMode, cql, params)
	} else if collect {
		result, err = c.execTransactionCollect(transactionMode, cql, params)
	} else {
		result, err = c.execTransaction(transactionMode, cql, params)
	}
	return result, err
}

func (c *cypherExecuter) single(dbName string, cql string, params map[string]interface{}) (*db.Record, error) {
	record, err := c.exec(dbName, cql, params, true, false)
	return record.(*db.Record), err
}

func (c *cypherExecuter) collect(dbName string, cql string, params map[string]interface{}) ([]*db.Record, error) {
	record, err := c.exec(dbName, cql, params, false, true)
	return record.([]*db.Record), err
}

func (c *cypherExecuter) setTransaction(transaction *transaction) {
	c.transaction = transaction
}
