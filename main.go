package main

// TODO: convert to using a proper logging package
// TODO: embed the unicode csv
import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"time"

	"git.doterra.net/salesforce/hierarchy-calculation-engine/bulkQuery"
	"git.doterra.net/salesforce/hierarchy-calculation-engine/engine"
	"git.doterra.net/salesforce/hierarchy-calculation-engine/sessionovd"

	"github.com/aheber/go-sfdc/bulk"
	"github.com/aheber/go-sfdc/session"
	"github.com/aheber/go-sfdc/soql"
)

const (
	defaultPort         = "8080"
	charFile            = "./assets/unicodechars.csv"
	version             = "53.0"
	retrieveRecordCount = 1000000
)

var (
	chars        []string
	invalidChars = []string{"%", "_", ",", "\"", "'", "\\", "*", "?"}
	work         chan sessionovd.Session
)

func main() {
	chars = loadChars()
	fmt.Printf("Loaded %d characters\n", len(chars))
	work = make(chan sessionovd.Session, 5)
	go func() {
		for {
			session := <-work
			calculateSalesforceLineageChains(&session)
		}
	}()
	http.HandleFunc("/calculatehierarchy", handleCalculateRequest)
	portNum := os.Getenv("PORT")
	if len(portNum) == 0 {
		portNum = defaultPort
	}
	// open web server
	if err := http.ListenAndServe(":"+portNum, nil); err != nil {
		panic(err)
	}
}

func handleCalculateRequest(w http.ResponseWriter, r *http.Request) {
	// TODO: Some kind of org-level locking until an existing job for that org is done
	// might need to monitor until the bulk job is finished
	// If we do have active work going or queued for that org we should reject the request
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		panic(err)
	}
	// Get session id and org url from payload
	var s sessionovd.Session
	err = json.Unmarshal(body, &s)
	if err != nil {
		panic(err)
	}
	s.TokenType = "Bearer"
	s.Version = "v" + version
	s.HTTPClient = &http.Client{}
	// Enqueue the work
	// TODO: test the session before telling the caller that we're good to go
	work <- s
	// send back a success message
	w.WriteHeader(201)
	w.Write([]byte("{\"status\": 201, \"message\":\"Org queued for processing\"}"))
}

func calculateSalesforceLineageChains(session *sessionovd.Session) {
	conn := bulkQuery.Connection{AccessToken: session.AccessToken,
		InstanceUrl: session.InstanceURL(),
		ApiVersion:  version,
		Client:      *session.HTTPClient}

	idLookupTable := make(map[string]uint32)

	// Need Id, match field, reference field, chain storage field, depth storage field
	input := soql.QueryInput{
		ObjectType: "Account",
		FieldList: []string{
			"Id",
			"Customer_Number__c",
			"Parent_1__c",
			"Parent_2__c",
			"Parent_1_Lineage_Chain__c",
			"Parent_2_Lineage_Chain__c",
		},
	}
	queryStmt, err := soql.NewQuery(input)
	if err != nil {
		fmt.Printf("SOQL Query Statement Error %s\n", err.Error())
		return
	}

	q, err := queryStmt.Format()
	if err != nil {
		fmt.Printf("Error formatting Query:%s", err)
	}
	beforeQueryTime := time.Now()
	engine.PrintMemUsage()
	// q = q + " ORDER BY Parent_1__c NULLS FIRST LIMIT 10"
	resp, err := bulkQuery.SubmitQueryJob(q, conn)
	if err != nil {
		fmt.Printf("Error creating Bulk Query: %s", err)
		return
	}

	resp, err = bulkQuery.WaitForQueryJobCompletion(resp.ID, conn)
	if err != nil {
		fmt.Printf("Error waiting for Bulk Query: %s", err)
		return
	}

	engine.TimeTrack(beforeQueryTime, "Initial query and response")

	result, err := bulkQuery.RetrieveBulkQueryResults(resp.ID, conn, retrieveRecordCount, "")

	totalSize := resp.NumberRecordsProcessed
	fmt.Printf("Retrieving %d records from Salesforce\n", totalSize)
	// process query results into container and pass to function that will drive the engine
	g := &engine.Group{Members: make(map[uint32]engine.Record, totalSize)}
	g.SetChars(chars)
	// loop through results, querying for additional records as needed
	digestingRecordsTime := time.Now()

	resultsChan := make(chan [][]string, 5)
	resultsDoneChan := make(chan bool)

	// Processor reading from the channel of data
	go func() {
		// Process the current set of records from the API
		for results := range resultsChan {
			fmt.Printf("Completed %v/%v: %0.2f%%\r", len(idLookupTable), totalSize, (float32(len(idLookupTable))/float32(totalSize))*100)
			for _, r := range results {

				// fmt.Printf("Record raw: %v\n", r)
				dIDString := r[result.ColumnMap["Customer_Number__c"]]
				parent1BranchID := r[result.ColumnMap["Parent_1_Lineage_Chain__c"]]
				parent2BranchID := r[result.ColumnMap["Parent_2_Lineage_Chain__c"]]

				dIDInt, err := strconv.Atoi(dIDString[:len(dIDString)-2])
				// fmt.Printf("%v -- %v", dIDString, dIDInt)
				dID := uint32(dIDInt)
				if err != nil {
					// fmt.Printf("Error %v", err)
					continue
				}
				sfID := r[result.ColumnMap["Id"]]
				idLookupTable[sfID] = dID

				p2ID := r[result.ColumnMap["Parent_2__c"]]
				p1ID := r[result.ColumnMap["Parent_1__c"]]

				rec := record{
					id:              dID,
					sfID:            sfID,
					parent1SFID:     p1ID,
					parent2SFID:     p2ID,
					parent1BranchID: parent1BranchID,
					parent2BranchID: parent2BranchID,
					parentMode:      parent1,
					idLookupTable:   &idLookupTable,
				}
				// fmt.Printf("Record:%v\n", rec)
				rec.ChangeParentMode(parent1)
				// fmt.Printf("Record: %v\n", rec)
				g.Members[dID] = &rec
			}
		}
		fmt.Println("Finished all work and returning to synchronous processing")
		resultsDoneChan <- true
	}()

	// Loop through results and add to the channel for async processing
	// Should allow us to drain the data from Salesforce as fast as possible
	for {

		// fmt.Print(".")
		records, err := result.Records.ReadAll()
		if err != nil {
			fmt.Printf("ReadAll CSV error:%s", err)
		}
		result.CloseCSV()
		// fmt.Printf("Records: %v\n", records)
		resultsChan <- records
		// If we're done processing records then break the SOQL queryMore loop
		if result.Done() {
			close(resultsChan)
			break
		}

		// If we are not done go get next set of records from Salesforce and start the loop over again
		result, err = result.Next()
		if err != nil {
			fmt.Printf("SOQL Query Statement Error %s\n", err.Error())
			return
		}
	}

	// Wait for all async processing to complete by waiting for a message on this channel
	fmt.Println("Waiting for results to finish processing")
	<-resultsDoneChan
	fmt.Printf("All data processed and ready to go with %d records\n", len(g.Members))

	engine.TimeTrack(digestingRecordsTime, "Digest records from API")
	engine.PrintMemUsage()
	// fmt.Printf("Members count %v", len(g.Members))
	// Calculate the parent hierarchy
	g.CalculateHierarchy()

	updateSize := 0
	for _, v := range g.Members {
		if v.(*record).GetIsChanged() {
			updateSize++
		}
	}

	fmt.Printf("First tree generated %v record updates\n", updateSize)

	// Swap values to the other side and run it again
	beforeShuffleRecords := time.Now()
	for _, v := range g.Members {
		v.(*record).ChangeParentMode(parent2)
		g.Members[v.GetID()] = v
	}
	engine.TimeTrack(beforeShuffleRecords, "Shuffled Records to Sponsor")
	// Calculate the Parent2 hierarchy
	g.CalculateHierarchy()

	updateSize = 0
	for _, v := range g.Members {
		if v.(*record).GetIsChanged() {
			updateSize++
		}
	}

	fmt.Printf("Both trees together generated %v record updates\n", updateSize)

	// process changed records and submit back to Salesforce
	err = updateRecords(session, &g.Members)
	if err != nil {
		fmt.Printf("Error updating records %v\n", err)
	}
}

func updateRecords(session session.ServiceFormatter, data *map[uint32]engine.Record) error {
	// determine how many records need to be updated
	updateSize := 0
	for _, v := range *data {
		if v.(*record).GetIsChanged() {
			updateSize++
		}
	}
	if updateSize == 0 {
		fmt.Printf("No updates needed to data\n")
		return nil
	}
	fmt.Printf("Processing updates to %v records\n", updateSize)
	// TODO: break into chunks and submit as separate jobs
	recordsToUpdate := make([]bulk.Record, 0, updateSize)
	for _, v := range *data {
		if v.(*record).GetIsChanged() {
			recordsToUpdate = append(recordsToUpdate, v.(*record))
		}
	}

	fields := []string{
		"Id",
		"Parent_1_Lineage_Chain__c",
		"Parent_1_Lineage_Depth__c",
		"Parent_2_Lineage_Chain__c",
		"Parent_2_Lineage_Depth__c",
	}
	jobOpts := bulk.Options{
		ColumnDelimiter: bulk.Comma,
		Operation:       bulk.Update,
		Object:          "Account",
	}

	beforeProcessDataAsBulk := time.Now()
	jobs, err := bulk.ProcessDataAsBulkJobs(session, jobOpts, fields, recordsToUpdate)
	if err != nil {
		return err
	}
	fmt.Printf("Completed sending data in %d jobs\n", len(jobs))
	engine.TimeTrack(beforeProcessDataAsBulk, "Process records and start bulk job(s)")
	return nil
}

// Load characters from CSV to behave like a multi-thousand based number system
func loadChars() []string {
	var mChars = make([]string, 0)
	// Open CSV file
	f, err := os.Open(charFile)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	// Read File into a Variable
	lines, err := csv.NewReader(f).ReadAll()
	if err != nil {
		panic(err)
	}
	// Drop the header line
	lines = lines[1:]
	// Loop through lines & turn into object

	invalidCharMap := make(map[string]bool)
	for _, c := range invalidChars {
		invalidCharMap[c] = true
	}
	for _, line := range lines {
		if _, invalid := invalidCharMap[line[0]]; invalid {
			// Skip the char because we've outlawed it
			continue
		}
		mChars = append(mChars, line[0])
	}
	return mChars
}
