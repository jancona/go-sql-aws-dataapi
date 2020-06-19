// Package dataapi provides a driver for Go's database/sql package supporting
// the AWS Data API.
package dataapi

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"log"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/rdsdataservice"
)

const DriverName = "dataapi"

// Driver is exported to make the driver directly accessible where
// needed. General usage is expected to be constrained to the database/sql
// APIs.
type Driver struct{}

func init() {
	log.Printf("Registering driver %s", DriverName)
	sql.Register(DriverName, &Driver{})
}

// Open takes a connection string in the format `dataapi:///<database>?clusterARN=<cluster ARN>&secretARN=<secret ARN>`
func (d Driver) Open(connString string) (driver.Conn, error) {
	if !strings.HasPrefix(connString, "dataapi:") {
		return nil, fmt.Errorf("Expected connection string with the format 'dataapi:///<database>?clusterARN=<cluster ARN>&secretARN=<secret ARN>', got '%s'", connString)
	}
	u, err := url.Parse(connString)
	if err != nil {
		return nil, fmt.Errorf("Error parsing connection URL '%s': %v", connString, err)
	}
	clusterARN := u.Query().Get("clusterARN")
	if clusterARN == "" {
		return nil, fmt.Errorf("Missing clusterARN in connection URL '%s'", connString)
	}
	secretARN := u.Query().Get("secretARN")
	if secretARN == "" {
		return nil, fmt.Errorf("Missing secretARN in connection URL '%s'", connString)
	}
	database := strings.TrimPrefix(u.Path, "/")
	if database == "" {
		return nil, fmt.Errorf("Missing database in connection URL '%s'", connString)
	}

	sess, err := session.NewSession()
	if err != nil {
		return nil, fmt.Errorf("Error creating session: %w", err)
	}
	conn := &dataAPIConn{
		service:    rdsdataservice.New(sess),
		clusterARN: clusterARN,
		secretARN:  secretARN,
		database:   database,
	}

	return conn, nil
}
