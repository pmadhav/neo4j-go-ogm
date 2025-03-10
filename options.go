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

//LoadOptions represents options used for loading database objects
type LoadOptions struct {
	Depth        int
	DatabaseName string
}

//SaveOptions represents options used for saving database objects
type SaveOptions struct {
	Depth        int
	DatabaseName string
}

//DeleteOptions represents options used for saving database objects. Currently, not applicatble to this version of the OGM
type DeleteOptions struct {
	DatabaseName string
}

//NewLoadOptions creates LoadOptions with defaults
func NewLoadOptions(dbName string) *LoadOptions {
	lo := &LoadOptions{}
	lo.Depth = 1
	lo.DatabaseName = dbName
	return lo
}

//NewSaveOptions creates SaveOptions with defaults
func NewSaveOptions(dbName string, depth int) *SaveOptions {
	so := &SaveOptions{}
	so.Depth = depth
	so.DatabaseName = dbName
	return so
}

//NewLoadOptions creates LoadOptions with defaults
func NewDeleteOptions(dbName string) *DeleteOptions {
	lo := &DeleteOptions{}
	lo.DatabaseName = dbName
	return lo
}
