package sdk_columnar

import (
	"encoding/json"
	"github.com/couchbase/gocb/v2"
	"log"
	"testing"
)

func TestConfigConnectionManager(t *testing.T) {
	cConfig := &ClusterConfig{}
	cmObj := ConfigConnectionManager()

	cbConnStr := "=== Add Connection String Key ===" // replace 'https://' with 'couchbases://'
	cbUsername := "=== Add Access Key ==="
	cbPassword := "=== Add Secret Key ==="

	cbCluster, err := cmObj.GetCluster(cbConnStr, cbUsername, cbPassword, cConfig)
	if err != nil {
		log.Println(err)
		t.Fail()
	}
	log.Println("Connection Manager Object:\n", cmObj)
	log.Println("Cluster Object:\n", cbCluster)

	// Pinging the Cluster
	pingRes, errPing := cbCluster.Ping(&gocb.PingOptions{
		ServiceTypes: []gocb.ServiceType{gocb.ServiceTypeAnalytics},
	})
	if errPing != nil {
		log.Print("error while pinging")
	}
	log.Println("Ping Result")
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

	pingResJSON, errJSON := json.MarshalIndent(pingRes, "", "  ")
	if errJSON != nil {
		log.Println(errJSON)
	}
	log.Printf("Ping report JSON: \n%s\n", string(pingResJSON))

	// Executing an Analytics Query
	results, errAnalyticsQuery := cbCluster.AnalyticsQuery("SELECT 2;", nil)
	if errAnalyticsQuery != nil {
		log.Println("unable to execute query")
		log.Println(errAnalyticsQuery)
		t.Fail()
	}

	log.Println("Analytics Query Result:")
	if results != nil {
		var resultDisplay interface{}
		for results.Next() {
			err := results.Row(&resultDisplay)
			if err != nil {
				log.Println(err)
			}
			log.Println(resultDisplay)
		}
	}

	// Closing the connection
	errClose := cmObj.Clusters[cbConnStr].Cluster.Close(nil)
	if errClose != nil {
		log.Println("Unable to close connection")
		log.Println(errClose)
	}
	log.Println("Closed Connection")
}
