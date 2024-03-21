package db

import (
	"log"

	"github.com/couchbase/gocb/v2"

	"github.com/couchbaselabs/sirius/internal/sdk_columnar"
)

type columnarOperationResult struct {
	key    string
	result perDocResult
}

func newColumnarOperationResult(key string, value interface{}, err error, status bool, offset int64) *columnarOperationResult {
	return &columnarOperationResult{
		key: key,
		result: perDocResult{
			value:  value,
			error:  err,
			status: status,
			offset: offset,
		},
	}
}

func (c *columnarOperationResult) Key() string {
	return c.key
}

func (c *columnarOperationResult) Value() interface{} {
	return c.result.value
}

func (c *columnarOperationResult) GetStatus() bool {
	return c.result.status
}

func (c *columnarOperationResult) GetError() error {
	return c.result.error
}

func (c *columnarOperationResult) GetExtra() map[string]any {
	return map[string]any{}
}

func (c *columnarOperationResult) GetOffset() int64 {
	return c.result.offset
}

type Columnar struct {
	ConnectionManager *sdk_columnar.ConnectionManager
}

func NewColumnarConnectionManager() *Columnar {
	return &Columnar{
		ConnectionManager: sdk_columnar.ConfigConnectionManager(),
	}
}

func (c *Columnar) Connect(connStr, username, password string, extra Extras) error {
	if err := validateStrings(connStr, username, password); err != nil {
		return err
	}
	clusterConfig := &sdk_columnar.ClusterConfig{}

	if _, err := c.ConnectionManager.GetCluster(connStr, username, password, clusterConfig); err != nil {
		log.Println("In Columnar Connect(), error in GetCluster()")
		return err
	}

	return nil
}

func (c *Columnar) Warmup(connStr, username, password string, extra Extras) error {

	if err := validateStrings(connStr, username, password); err != nil {
		log.Println("In Columnar Warmup(), error:", err)
		return err
	}

	log.Println("In Columnar Warmup()")

	// Pinging the Cluster
	cbCluster := c.ConnectionManager.Clusters[connStr].Cluster

	pingRes, errPing := cbCluster.Ping(&gocb.PingOptions{
		ServiceTypes: []gocb.ServiceType{gocb.ServiceTypeAnalytics},
	})
	if errPing != nil {
		log.Print("In Columnar Warmup(), error while pinging:", errPing)
		return errPing
	}

	for service, pingReports := range pingRes.Services {
		if service != gocb.ServiceTypeAnalytics {
			log.Println("We got a service type that we didn't ask for!")
		}
		for _, pingReport := range pingReports {
			if pingReport.State != gocb.PingStateOk {
				log.Printf(
					"Node %s at remote %s is not OK, error: %s, latency: %s\n",
					pingReport.ID, pingReport.Remote, pingReport.Error, pingReport.Latency.String(),
				)
			} else {
				log.Printf(
					"Node %s at remote %s is OK, latency: %s\n",
					pingReport.ID, pingReport.Remote, pingReport.Latency.String(),
				)
			}
		}
	}

	return nil
}

func (c *Columnar) Close(connStr string, extra Extras) error {
	return c.ConnectionManager.Disconnect(connStr)
}

func (c *Columnar) Create(connStr, username, password string, keyValue KeyValue, extra Extras) OperationResult {
	// TODO
	panic("Implement the function")
}

func (c *Columnar) Update(connStr, username, password string, keyValue KeyValue, extra Extras) OperationResult {
	// TODO
	panic("Implement the function")
}

func (c *Columnar) Read(connStr, username, password, key string, offset int64, extra Extras) OperationResult {

	cbCluster := c.ConnectionManager.Clusters[connStr].Cluster
	//log.Println("Cluster:", cbCluster)

	results, errAnalyticsQuery := cbCluster.AnalyticsQuery(extra.Query, nil)
	if errAnalyticsQuery != nil {
		log.Println("In Columnar Read(), unable to execute query")
		log.Println(errAnalyticsQuery)
		return newColumnarOperationResult(key, nil, nil, false, offset)
	}

	log.Println("Analytics Query Result:")
	if results != nil {
		var resultDisplay interface{}
		for results.Next() {
			err := results.Row(&resultDisplay)
			if err != nil {
				log.Println("In Columnar Read(), unable to decode result")
				log.Println(err)
				return newColumnarOperationResult(key, nil, nil, false, offset)
			}
			log.Println(resultDisplay)
		}
	}
	errIterCursor := results.Err()
	if errIterCursor != nil {
		log.Println("In Columnar Read(), error while iterating cursor")
		log.Println(errIterCursor)
		return newColumnarOperationResult(key, nil, nil, false, offset)
	}
	return newColumnarOperationResult(key, nil, nil, true, offset)
}

func (c *Columnar) Delete(connStr, username, password, key string, offset int64, extra Extras) OperationResult {
	// TODO
	panic("Implement the function")
}

func (c *Columnar) Touch(connStr, username, password, key string, offset int64, extra Extras) OperationResult {
	// TODO
	panic("Implement the function")
}

func (c *Columnar) InsertSubDoc(connStr, username, password, key string, keyValues []KeyValue, offset int64,
	extra Extras) SubDocOperationResult {
	// TODO
	panic("Implement the function")
}

func (c *Columnar) UpsertSubDoc(connStr, username, password, key string, keyValues []KeyValue, offset int64,
	extra Extras) SubDocOperationResult {
	// TODO
	panic("Implement the function")
}

func (c *Columnar) Increment(connStr, username, password, key string, keyValues []KeyValue, offset int64,
	extra Extras) SubDocOperationResult {
	// TODO
	panic("Implement the function")
}

func (c *Columnar) ReplaceSubDoc(connStr, username, password, key string, keyValues []KeyValue, offset int64,
	extra Extras) SubDocOperationResult {
	// TODO
	panic("Implement the function")
}

func (c *Columnar) ReadSubDoc(connStr, username, password, key string, keyValues []KeyValue, offset int64,
	extra Extras) SubDocOperationResult {
	// TODO
	panic("Implement the function")
}

func (c *Columnar) DeleteSubDoc(connStr, username, password, key string, keyValues []KeyValue, offset int64,
	extra Extras) SubDocOperationResult {
	// TODO
	panic("Implement the function")
}

func (c *Columnar) CreateBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult {
	// TODO
	panic("Implement the function")
}

func (c *Columnar) UpdateBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult {
	// TODO
	panic("Implement the function")
}

func (c *Columnar) ReadBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult {
	// TODO
	panic("Implement the function")
}

func (c *Columnar) DeleteBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult {
	// TODO
	panic("Implement the function")
}

func (c *Columnar) TouchBulk(connStr, username, password string, keyValues []KeyValue, extra Extras) BulkOperationResult {
	// TODO
	panic("Implement the function")
}

func (c *Columnar) CreateDatabase(connStr, username, password string, extra Extras, templateName string, docSize int) (string, error) {
	// TODO
	panic("Implement the function")
}
func (c *Columnar) DeleteDatabase(connStr, username, password string, extra Extras) (string, error) {
	// TODO
	panic("Implement the function")
}
func (c *Columnar) Count(connStr, username, password string, extra Extras) (int64, error) {
	// TODO
	panic("Implement the function")
}
func (c *Columnar) ListDatabase(connStr, username, password string, extra Extras) (any, error) {
	// TODO
	panic("Implement the function")
}
