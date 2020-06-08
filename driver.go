// Package dataapi provides a driver for Go's database/sql package supporting
// the AWS Data API.
package dataapi

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/rdsdataservice"
)

// Driver is exported to make the driver directly accessible where
// needed. General usage is expected to be constrained to the database/sql
// APIs.
type Driver struct {
}

// Open takes a connection string in the format `dataapi:<cluster ARN>|<secret ARN>|<database>`
func (d Driver) Open(connString string) (driver.Conn, error) {
	if !strings.HasPrefix(connString, "dataapi:") {
		return nil, fmt.Errorf("Expected connection string with the format 'dataapi:<cluster ARN>|<secret ARN>|<database>', got '%s'", connString)
	}
	parts := strings.Split(strings.TrimPrefix(connString, "dataapi:"), "|")
	if len(parts) != 3 {
		return nil, fmt.Errorf("Expected connection string with the format 'dataapi:<cluster ARN>|<secret ARN>|<database>', got '%s'", connString)
	}
	sess, err := session.NewSession()
	if err != nil {
		return nil, fmt.Errorf("Error creating session: %w", err)
	}
	conn := &dataAPIConn{
		service:    rdsdataservice.New(sess),
		clusterARN: parts[0],
		secretARN:  parts[1],
		database:   parts[2],
	}

	return conn, nil
}

func init() {
	sql.Register("dataapi", &Driver{})
}
