package dataapi

import (
	"database/sql/driver"
	"errors"
	"io"
	"regexp"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/rdsdataservice"
)

var errNoLastInsertID = errors.New("no LastInsertId available after the empty statement")

type dataAPIStmt struct {
	conn  *dataAPIConn
	query string
}

var argRE = regexp.MustCompile(`\$(\d*)`)

func newStmt(conn *dataAPIConn, query string) (driver.Stmt, error) {
	return &dataAPIStmt{
			conn:  conn,
			query: query},
		nil
}

// Close closes the statement.
//
// As of Go 1.1, a Stmt will not be closed if it's in use
// by any queries.
func (s *dataAPIStmt) Close() error {
	s.conn = nil
	return nil
}

// NumInput returns the number of placeholder parameters.
//
// If NumInput returns >= 0, the sql package will sanity check
// argument counts from callers and return errors to the caller
// before the statement's Exec or Query methods are called.
//
// NumInput may also return -1, if the driver doesn't know
// its number of placeholders. In that case, the sql package
// will not sanity check Exec or Query argument counts.
func (s *dataAPIStmt) NumInput() int {
	return -1
}

// Exec executes a query that doesn't return rows, such
// as an INSERT or UPDATE.
//
// Deprecated: Drivers should implement StmtExecContext instead (or additionally).
func (s *dataAPIStmt) Exec(args []driver.Value) (driver.Result, error) {
	eso, err := s.execute(args)
	if err != nil {
		return nil, err
	}

	return &dataAPIResult{eso: eso}, nil
}

// Query executes a query that may return rows, such as a
// SELECT.
//
// Deprecated: Drivers should implement StmtQueryContext instead (or additionally).
func (s *dataAPIStmt) Query(args []driver.Value) (driver.Rows, error) {
	eso, err := s.execute(args)
	if err != nil {
		return nil, err
	}

	return &dataAPIRows{eso: eso}, nil
}

func (s *dataAPIStmt) execute(args []driver.Value) (*rdsdataservice.ExecuteStatementOutput, error) {
	params := make([]*rdsdataservice.SqlParameter, len(args))
	for n, arg := range args {
		f := rdsdataservice.Field{}
		params[n] = &rdsdataservice.SqlParameter{
			Name:  aws.String(strconv.Itoa(n + 1)),
			Value: &f,
		}
		switch v := arg.(type) {
		case int64:
			f.SetLongValue(v)
		case float64:
			f.SetDoubleValue(v)
		case bool:
			f.SetBooleanValue(v)
		case []byte:
			f.SetBlobValue(v)
		case string:
			f.SetStringValue(v)
		case time.Time:
			// DATA API format is YYYY-MM-DD HH:MM:SS[.FFF]
			f.SetStringValue(v.Format("2006-01-02 15:04:05.999"))
			// params[n].SetTypeHint("TIMESTAMP")
		}
	}
	// log.Printf("Executing query '%s' with args: %#v", s.query, params)
	esi := &rdsdataservice.ExecuteStatementInput{
		ResourceArn:           &s.conn.clusterARN,
		Database:              &s.conn.database,
		SecretArn:             &s.conn.secretARN,
		Sql:                   &s.query,
		Parameters:            params,
		IncludeResultMetadata: aws.Bool(true),
	}
	if s.conn.activeTx != nil {
		esi.SetTransactionId(s.conn.activeTx.id)
	}
	return s.conn.service.ExecuteStatement(esi)
}

type dataAPIRows struct {
	eso *rdsdataservice.ExecuteStatementOutput
	n   int
}

// Columns returns the names of the columns. The number of
// columns of the result is inferred from the length of the
// slice. If a particular column name isn't known, an empty
// string should be returned for that entry.
func (r *dataAPIRows) Columns() []string {
	cols := make([]string, len(r.eso.ColumnMetadata))
	for i, cm := range r.eso.ColumnMetadata {
		cols[i] = *cm.Name
	}
	return cols
}

// Close closes the rows iterator.
func (r *dataAPIRows) Close() error {
	r.eso = nil
	return nil
}

// Next is called to populate the next row of data into
// the provided slice. The provided slice will be the same
// size as the Columns() are wide.
//
// Next should return io.EOF when there are no more rows.
//
// The dest should not be written to outside of Next. Care
// should be taken when closing Rows not to modify
// a buffer held in dest.
func (r *dataAPIRows) Next(dest []driver.Value) error {
	if r.n >= len(r.eso.Records) {
		// log.Print("Next() no rows")
		return io.EOF
	}
	row := ""
	for n, c := range r.eso.Records[r.n] {
		if c.IsNull != nil && *c.IsNull {
			dest[n] = nil
		} else if c.BlobValue != nil {
			dest[n] = c.BlobValue
		} else if c.BooleanValue != nil {
			dest[n] = *c.BooleanValue
		} else if c.DoubleValue != nil {
			dest[n] = *c.DoubleValue
		} else if c.LongValue != nil {
			dest[n] = *c.LongValue
		} else if c.StringValue != nil {
			dest[n] = *c.StringValue
		}
		row += c.String() + " "
	}
	// log.Printf("Next() row %d: %s", r.n, row)
	r.n++
	return nil
}

type dataAPIResult struct {
	eso *rdsdataservice.ExecuteStatementOutput
}

// LastInsertId returns the database's auto-generated ID
// after, for example, an INSERT into a table with primary
// key.
func (r *dataAPIResult) LastInsertId() (int64, error) {
	return 0, errNoLastInsertID
}

// RowsAffected returns the number of rows affected by the
// query.
func (r *dataAPIResult) RowsAffected() (int64, error) {
	return *r.eso.NumberOfRecordsUpdated, nil
}
