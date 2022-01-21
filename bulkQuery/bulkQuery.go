package bulkQuery

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"
)

// submit query job
// wait for query job to complete (async while loop)
// get results from query job
// emit records in batches

func SubmitQueryJob(query string, conn Connection) (QueryInformationResponse, error) {

	req := CreateQueryRequest{Operation: "query", Query: query}
	body, err := json.Marshal(req)
	r, _ := http.NewRequest(http.MethodPost, conn.InstanceUrl+"/services/data/v"+conn.ApiVersion+"/jobs/query", bytes.NewBuffer(body))
	r.Header.Add("Authorization", "Bearer "+conn.AccessToken)
	r.Header.Add("Content-Type", "application/json")
	r.Header.Add("Content-Length", strconv.Itoa(len(body)))

	resp, err := conn.Client.Do(r)
	cqResp := QueryInformationResponse{}
	if err != nil {
		return cqResp, err
	}
	defer resp.Body.Close()
	b, err := ioutil.ReadAll(resp.Body)
	err = json.NewDecoder(bytes.NewBuffer(b)).Decode(&cqResp)
	if err != nil {
		return cqResp, errors.New(err.Error() + ":" + string(b))
	}
	return cqResp, nil
}

func WaitForQueryJobCompletion(jobID string, conn Connection) (QueryInformationResponse, error) {
	for {
		r, err := CheckBulkQueryJobStatus(jobID, conn)
		if err != nil {
			return r, err
		}
		// TODO: do the of many states check correctly
		if r.State == "Aborted" || r.State == "JobComplete" || r.State == "Failed" {
			return r, nil
		}
		time.Sleep(10 * time.Second)
	}

}

func CheckBulkQueryJobStatus(jobID string, conn Connection) (QueryInformationResponse, error) {
	r, _ := http.NewRequest(http.MethodGet, conn.InstanceUrl+"/services/data/v"+conn.ApiVersion+"/jobs/query/"+jobID, nil)
	r.Header.Add("Authorization", "Bearer "+conn.AccessToken)
	r.Header.Add("Content-Type", "application/json")

	resp, err := conn.Client.Do(r)
	cqResp := QueryInformationResponse{}
	if err != nil {
		return cqResp, err
	}
	defer resp.Body.Close()
	err = json.NewDecoder(resp.Body).Decode(&cqResp)
	if err != nil {
		return cqResp, err
	}
	return cqResp, nil
}

func RetrieveBulkQueryResults(jobID string, conn Connection, maxRecords int, locator string) (BulkQueryResults, error) {
	url := conn.InstanceUrl + "/services/data/v" + conn.ApiVersion + "/jobs/query/" + jobID + "/results?maxRecords=" + strconv.Itoa(maxRecords)
	if locator != "" {
		url += "&locator=" + locator
	}
	r, _ := http.NewRequest(http.MethodGet, url, nil)
	r.Header.Add("Authorization", "Bearer "+conn.AccessToken)
	r.Header.Add("Accept", "text/csv")

	resp, err := conn.Client.Do(r)
	bqResults := BulkQueryResults{conn: conn, JobID: jobID, maxRecords: maxRecords}
	if err != nil {
		return bqResults, err
	}
	bqResults.SforceLocator = resp.Header.Get("Sforce-Locator")
	bqResults.SforceNumberOfRecords, _ = strconv.Atoi(resp.Header.Get("Sforce-Numberofrecords"))
	bqResults.Records = csv.NewReader(resp.Body)
	bqResults.respBody = resp.Body
	header, err := bqResults.Records.Read()
	if err != nil {
		return bqResults, err
	}
	headerMap := make(map[string]int)
	for i, v := range header {
		headerMap[v] = i
	}
	bqResults.ColumnMap = headerMap

	return bqResults, nil
}

type BulkQueryResults struct {
	SforceLocator         string
	SforceNumberOfRecords int
	ColumnMap             map[string]int
	Records               *csv.Reader
	JobID                 string
	conn                  Connection
	maxRecords            int
	respBody              io.ReadCloser
}

func (r *BulkQueryResults) CloseCSV() {
	r.respBody.Close()
}
func (r *BulkQueryResults) Done() bool {
	return len(r.SforceLocator) == 0 || r.SforceLocator == "null"
}

func (r *BulkQueryResults) Next() (BulkQueryResults, error) {
	return RetrieveBulkQueryResults(r.JobID, r.conn, r.maxRecords, r.SforceLocator)
}

type CreateQueryRequest struct {
	Operation string `json:"operation"`
	Query     string `json:"query"`
}

type QueryInformationResponse struct {
	ID                     string  `json:"id"`
	Operation              string  `json:"operation"`
	Object                 string  `json:"object"`
	CreatedByID            string  `json:"createdById"`
	CreatedDate            string  `json:"createdDate"`
	SystemModStamp         string  `json:"systemModstamp"`
	State                  string  `json:"state"`
	ConcurrencyMode        string  `json:"concurrencyMode"`
	ContentType            string  `json:"contentType"`
	ApiVersion             float32 `json:"apiVersion"`
	LineEnding             string  `json:"lineEnding"`
	ColumnDelimiter        string  `json:"columnDelimiter"`
	NumberRecordsProcessed uint64  `json:"numberRecordsProcessed"`
	Retries                uint    `json:"retries"`
	TotalProcessingTime    uint64  `json:"totalProcessingTime"`
}

type Connection struct {
	AccessToken string
	InstanceUrl string
	ApiVersion  string
	Client      http.Client
}

type BulkQueryConfig struct {
	LineEnding string
}
