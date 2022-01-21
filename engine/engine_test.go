package engine

import (
	"math"
	"math/rand"
	"strings"
	"testing"
)

var (
	// Build list of characters for use in building character string for lineage chain
	chars = []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m",
		"n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z",
		"0", "1", "2", "3", "4", "5", "6", "7", "8", "9",
		"A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M",
		"N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z", "늌"}
	// TODO: build using more interesting unicode characters
)

type dataSeed struct {
	ID             uint32
	parentID       uint32
	branchID       string
	parentBranchID string
}

func TestBuildBasicTree(t *testing.T) {
	reportMem = false
	reportTimeTracking = false

	dataTable := []dataSeed{
		{1, Uint32Max, "", ""},
		{2, Uint32Max, "", ""},
		{3, Uint32Max, "", ""},
		{4, 1, "", ""},
		{5, 1, "", ""},
		{6, 1, "", ""},
		{7, 4, "", ""},
		{8, 5, "", ""},
		{9, 6, "", ""},
		{10, 6, "", ""},
		{11, 10, "", ""},
		{12, 11, "", ""},
		{13, 11, "", ""},
		{14, 13, "", ""},
		{15, 14, "", ""},
	}

	data := verifyHierarchy(t, dataTable)

	branchIDs := make(map[uint32]string)

	// Run it again and make sure branch IDs didn't shift around as they should all show as
	// non-conflicting preferred keys
	for _, tt := range dataTable {
		branchIDs[tt.ID] = data.Members[tt.ID].GetBranchID()
	}
	data.CalculateHierarchy()
	for _, tt := range dataTable {
		if branchIDs[tt.ID] != data.Members[tt.ID].GetBranchID() {
			t.Errorf("Branch Keys changed between subsequent runs old: --%v-- new: --%v--", branchIDs[tt.ID], data.Members[tt.ID].GetBranchID())
		}
	}

}

func TestBuildLargeTree(t *testing.T) {

	reportMem = false
	reportTimeTracking = false
	recordCount := 150000 // increase for heavier workloads

	memberCount := uint32(recordCount)
	addedMember := 0
	dataTable := make([]dataSeed, recordCount)

	for i := uint32(0); i < memberCount; i++ {
		// Naieve parent assignment among the list of available parents
		parentID := Uint32Max
		if addedMember != 0 {
			parentID = uint32(math.Mod(rand.Float64()*float64(memberCount), float64(addedMember)))
		}
		dataTable[i] = dataSeed{ID: i, parentID: parentID, branchID: ""}
		addedMember++
	}

	verifyHierarchy(t, dataTable)

}

// test partial updates, allow keeping keys to reduce thrashing

func TestPreserveBranch(t *testing.T) {
	reportMem = false
	reportTimeTracking = false

	dataTable := []dataSeed{
		{1, Uint32Max, "늌", ""}, // keep
		{2, Uint32Max, "y", ""}, // keep
		{3, Uint32Max, "", ""},
		{4, 1, "늌n", ""}, // keep
		{5, 1, "늌a", ""}, // keep
		{6, 1, "ff", ""},
		{7, 4, "늌nv", ""}, // keep
		{8, 4, "", ""},
		{9, 6, "ffa", ""},
		{10, 6, "ffb", ""},
		{11, 10, "ffba", ""},
		{12, 4, "늌nvyyz", ""}, // lose
		{13, 4, "늌nv", ""},    // lose
		{14, 13, "", ""},
		{15, 14, "", ""},
	}

	data := verifyHierarchy(t, dataTable)
	verifyBranchID(t, "늌", data.Members[1].GetBranchID())
	assertBoolean(t, false, data.Members[1].(*record).GetIsChanged())
	verifyBranchID(t, "y", data.Members[2].GetBranchID())
	assertBoolean(t, false, data.Members[2].(*record).GetIsChanged())
	verifyBranchID(t, "a", data.Members[3].GetBranchID())
	assertBoolean(t, true, data.Members[3].(*record).GetIsChanged())
	verifyBranchID(t, "늌n", data.Members[4].GetBranchID())
	assertBoolean(t, false, data.Members[4].(*record).GetIsChanged())
	verifyBranchID(t, "늌a", data.Members[5].GetBranchID())
	assertBoolean(t, false, data.Members[5].(*record).GetIsChanged())
	verifyBranchID(t, "늌b", data.Members[6].GetBranchID())
	assertBoolean(t, true, data.Members[6].(*record).GetIsChanged())
	ids := make(map[string]bool)
	ids["늌nv"] = true
	ids["늌na"] = true
	ids["늌nb"] = true
	ids["늌nc"] = true

	verifyBranchIDInMap(t, ids, data.Members[7].GetBranchID())
	verifyBranchIDInMap(t, ids, data.Members[8].GetBranchID())
	verifyBranchIDInMap(t, ids, data.Members[12].GetBranchID())
	verifyBranchIDInMap(t, ids, data.Members[13].GetBranchID())
}

// test partial updates, allow keeping keys to reduce thrashing

func TestCleanInvalidFromBranch(t *testing.T) {
	reportMem = false
	reportTimeTracking = false

	dataTable := []dataSeed{
		{1, Uint32Max, "ᶂ", ""}, // keep
		{2, Uint32Max, "y", ""}, // keep
		{3, Uint32Max, "", ""},
		{4, 1, "ᶂn", ""}, // keep
		{5, 1, "ᶂa", ""}, // keep
		{6, 1, "ff", ""},
	}

	data := verifyHierarchy(t, dataTable)
	verifyBranchID(t, "a", data.Members[1].GetBranchID())
	verifyBranchID(t, "y", data.Members[2].GetBranchID())
	verifyBranchID(t, "b", data.Members[3].GetBranchID())
	ids := make(map[string]bool)
	ids["aa"] = true
	ids["ab"] = true
	ids["ac"] = true

	verifyBranchIDInMap(t, ids, data.Members[4].GetBranchID())
	verifyBranchIDInMap(t, ids, data.Members[5].GetBranchID())
	verifyBranchIDInMap(t, ids, data.Members[6].GetBranchID())
}

// // test partial trees, allow partial selection to enable branch updates
// // without having the entire tree onhand

// func TestPartialBranch(t *testing.T) {
// 	reportMem = false
// 	reportTimeTracking = false

// 	dataTable := []dataSeed{
// 		{1, 100, "x", "1234"},     // keep
// 		{2, 100, "12345", "1234"}, // keep
// 		{3, 100, "123455", "1234"},
// 		{4, 100, "12346", "1234"},
// 		{5, 100, "12346", "1234"},
// 		{6, 1, "ff", ""},
// 		{7, 4, "xnv", ""}, // keep
// 		{8, 4, "", ""},
// 		{9, 6, "ffa", ""},
// 		{10, 6, "ffb", ""},
// 		{11, 10, "ffba", ""},
// 		{12, 4, "xnvyyz", ""}, // lose
// 		{13, 4, "xnv", ""},    // lose
// 		{14, 13, "", ""},
// 		{15, 14, "", ""},
// 	}

// 	data := verifyHierarchy(t, dataTable)
// 	verifyBranchID(t, "12345", data.Members[2].GetBranchID())
// 	verifyBranchIDHasParentBranchID(t, data.Members[1])
// 	verifyBranchIDHasParentBranchID(t, data.Members[2])
// 	verifyBranchIDHasParentBranchID(t, data.Members[3])
// 	verifyBranchIDHasParentBranchID(t, data.Members[4])
// 	verifyBranchIDHasParentBranchID(t, data.Members[5])
// }

func verifyBranchID(t *testing.T, branchID string, actualBranchID string) {
	// Additional validations for kept keys
	// log.Printf("Testing '%v' and assigned '%v'", branchID, actualBranchID)
	if actualBranchID != branchID {
		t.Errorf("Expected to have branch id '%v', instead held '%v'", branchID, actualBranchID)
	}
}

func assertBoolean(t *testing.T, exptected bool, actual bool) {
	// Additional validations for kept keys
	// log.Printf("Testing '%v' and assigned '%v'", branchID, actualBranchID)
	if exptected != actual {
		t.Errorf("Expected '%v' to be '%v'", exptected, actual)
	}
}

func verifyBranchIDInMap(t *testing.T, ids map[string]bool, actualBranchID string) {
	// Additional validations for kept keys
	// log.Printf("Testing '%v' and assigned '%v'", branchID, actualBranchID)
	if _, exists := ids[actualBranchID]; !exists {
		t.Errorf("Expected '%v' to be found in %v", actualBranchID, ids)
	}
}

// func verifyBranchIDHasParentBranchID(t *testing.T, r RecordI) {
// 	if !strings.HasPrefix(r.GetBranchID(), r.GetParentBranchID()) {
// 		t.Errorf("Expected '%v' to start with '%v'", r.GetBranchID(), r.GetParentBranchID())
// 	}
// }

func verifyHierarchy(t *testing.T, dataTable []dataSeed) Group {

	data := Group{}
	data.Members = make(map[uint32]Record)
	data.SetChars(chars)
	branchKeys := make(map[string]bool)

	for _, tt := range dataTable {
		data.Members[tt.ID] = &record{id: tt.ID, parentID: tt.parentID, branchID: tt.branchID, parentBranchID: tt.parentBranchID, parentBranchDepth: 5}
	}
	data.CalculateHierarchy()

	for _, tt := range dataTable {
		r := data.Members[tt.ID]
		if _, ok := branchKeys[r.GetBranchID()]; ok {
			t.Errorf("Key already used '%v'", r.GetBranchID())
		} else {
			branchKeys[r.GetBranchID()] = true
		}

		// TODO: might be more than one character if there are more parents than characters
		if tt.parentID == Uint32Max && len([]rune(r.GetBranchID())) != 1 {
			t.Errorf("Expected '%v' to be a single character", r.GetBranchID())
		}
		p, haveParent := data.Members[tt.parentID]
		if tt.parentID != Uint32Max && haveParent &&
			!strings.HasPrefix(r.GetBranchID(), p.GetBranchID()) {
			t.Errorf("Expected '%v'(%v) to contain '%v'(%v)", p.GetBranchID(), p.GetID(), r.GetBranchID()[:len(r.GetBranchID())-1], r.GetID())
		}

		// Check partial diff marking, if BranchID didn't change, it shouldn't be marked for update
		if tt.branchID == r.GetBranchID() {
			if r.(*record).GetIsChanged() {
				t.Errorf("Expected '%v'(%v) not to be marked to change", p.GetBranchID(), p.GetID())
			}
		} else {
			if !r.(*record).GetIsChanged() {
				t.Errorf("Expected '%v'(%v) to be marked to change, as it is different from '%v'", r.GetBranchID(), r.GetID(), tt.branchID)
			}
		}
	}
	return data

}
