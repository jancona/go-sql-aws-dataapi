package dataapi

import (
	"database/sql/driver"

	"github.com/aws/aws-sdk-go/service/rdsdataservice"
)

type dataAPIConn struct {
	service    *rdsdataservice.RDSDataService
	clusterARN string
	secretARN  string
	database   string
	activeTx   *dataAPITx
}

func (dc *dataAPIConn) Begin() (driver.Tx, error) {
	return newTx(dc)
}

func (dc *dataAPIConn) Prepare(query string) (driver.Stmt, error) {
	return newStmt(dc, query)
}

func (dc *dataAPIConn) Close() (err error) {
	dc.service = nil
	dc.activeTx = nil
	return nil
}
