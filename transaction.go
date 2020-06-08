package dataapi

import "github.com/aws/aws-sdk-go/service/rdsdataservice"

type dataAPITx struct {
	conn *dataAPIConn
	id   string
}

func newTx(conn *dataAPIConn) (*dataAPITx, error) {
	bto, err := conn.service.BeginTransaction(&rdsdataservice.BeginTransactionInput{
		Database:    &conn.database,
		ResourceArn: &conn.clusterARN,
		SecretArn:   &conn.secretARN,
	})
	if err != nil {
		return nil, err
	}
	return &dataAPITx{
			conn: conn,
			id:   *bto.TransactionId},
		nil
}

func (tx *dataAPITx) Commit() error {
	_, err := tx.conn.service.CommitTransaction(&rdsdataservice.CommitTransactionInput{
		ResourceArn:   &tx.conn.clusterARN,
		SecretArn:     &tx.conn.secretARN,
		TransactionId: &tx.id,
	})
	tx.id = ""
	return err
}

func (tx *dataAPITx) Rollback() error {
	_, err := tx.conn.service.RollbackTransaction(&rdsdataservice.RollbackTransactionInput{
		ResourceArn:   &tx.conn.clusterARN,
		SecretArn:     &tx.conn.secretARN,
		TransactionId: &tx.id,
	})
	tx.id = ""
	return err
}
