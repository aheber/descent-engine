package engine

import (
	"fmt"
	"runtime"
	"strings"
	"time"
)

type void struct{}

var (
	reportMem          = true // Enable/disable memory usage reporting
	reportTimeTracking = true
	//// edgeTrack          = 0
	emptyVal void
	// Uint32Max is the maximum value for a Uint32 variable
	Uint32Max = uint32(4294967295)
)

// Group of members and their related eligible chain characters
type Group struct {
	Members map[uint32]Record
	chars   []string
	charMap map[rune]void
}

// SetChars is used to assign the available character for building the Branch ID
func (group *Group) SetChars(chars []string) {
	group.chars = chars
	group.charMap = make(map[rune]void)
	for _, s := range chars {
		group.charMap[[]rune(s)[0]] = emptyVal
	}
}

// CalculateHierarchy calculates a tree hierarchy given a list of records, optional list of characters to use to build lineage chain
func (group *Group) CalculateHierarchy() {
	// Used to report the memory size of the struct for optimization purposes
	// a := Record{}
	// fmt.Println(unsafe.Sizeof(a))
	defer PrintMemUsage()
	// Setup to track runtime
	defer TimeTrack(time.Now(), "Total runtime")
	PrintMemUsage()
	// Keep track of list of parent nodes as our entry points to start or processing
	var parents []uint32
	// var loseRecords = make(map[uint32]RecordI)

	// Clear any pre-existing child data
	for _, v := range group.Members {
		if len(v.GetChildren()) > 0 {
			v.SetChildren([]uint32{})
			group.Members[v.GetID()] = v
		}
	}

	startLinkParents := time.Now()
	// Iterate over tree
	for _, v := range group.Members {
		// List struct in map
		// Capture records without a parent to be the root of our engine
		if v.GetParentID() == Uint32Max {
			parents = append(parents, v.GetID())
			continue
		} else {
			if p, ok := group.Members[v.GetParentID()]; ok {
				p.SetChildren(append(p.GetChildren(), v.GetID()))
				group.Members[v.GetParentID()] = p
			} else {
				// TODO: decide how/when to handle this
				// handle missing parents
				// r, ok := loseRecords[v.getParentID()]
				// if !ok {
				// 	r = RecordI{
				// 		ID:          v.getParentID(),
				// 		BranchID:    v.getParentBranchID(),
				// 		BranchDepth: v.ParentBranchDepth,
				// 	}
				// }
				// r.children = append(r.children, v.ID)
				// loseRecords[r.ID] = r
			}
		}
	}
	TimeTrack(startLinkParents, "Linking Parents")
	PrintMemUsage()

	// Start calculating ids with a blank starting value
	startCalcLineageChain := time.Now()
	group.calculateLineageChain("", parents, 1)
	// TODO: decide what to do with this
	// // If we got records with lose parents (partials) then loop through and recalc the children
	// for _, p := range loseRecords {
	// 	group.calculateLineageChain(p.getBranchID(), p.getChildren(), int(p.getBranchDepth()-1))
	// }
	PrintMemUsage()
	TimeTrack(startCalcLineageChain, "Calculate Lineage Chain")
}

func (group *Group) calculateLineageChain(parentChain string, children []uint32, depth int) {
	// Determine how many characters wide are needed for this sibling group
	widthNeeded := 1
	childCount := len(children)
	for childCount > len(group.chars) {
		widthNeeded++
		childCount = childCount / len(group.chars)
	}

	for _, cID := range children {
		c := group.Members[cID]
		childBranchID := group.Members[cID].GetBranchID()

		// evalute all of the chars in the current Branch ID, if any of them are not in the current char map, burn the Branch ID
		if len([]rune(childBranchID)) > 0 {
			for _, r := range []rune(childBranchID) {
				if _, has := group.charMap[r]; !has {
					c.SetBranchID("")
					group.Members[cID] = c
					break
				}
			}
		}
	}

	// Allow children holding existing compatible IDs to hold them
	// Verify ID extends parent and meets width criteria
	// Build list of reserved characters for this sibling group so we don't reassign
	reservedChars := make(map[string]void)
	usedChars := make(map[string]void)
	for _, cID := range children {
		childBranchID := group.Members[cID].GetBranchID()
		if len([]rune(childBranchID)) > 0 &&
			strings.HasPrefix(childBranchID, parentChain) {
			childBranchUniqueParts := strings.Replace(childBranchID, parentChain, "", 1)
			if len([]rune(childBranchUniqueParts)) == widthNeeded {
				reservedChars[childBranchUniqueParts] = emptyVal
			}
		}
	}

	// Spin up an integer list of that width so we can track position
	charPosList := make([]int, widthNeeded)
	// Loop through each child and append a value from the Char array as the value for that
	for _, cID := range children {
		c := group.Members[cID]
		// Add nested set tracking
		//// edgeTrack++

		// If use child has already claimed an ID
		if len(c.GetBranchID()) > 0 {
			if strings.HasPrefix(c.GetBranchID(), parentChain) {
				childBranchUniqueParts := strings.Replace(c.GetBranchID(), parentChain, "", 1)
				// That ID was already found to be valid against the parent
				if _, exists := reservedChars[childBranchUniqueParts]; exists {
					// Nobody has claimed that ID yet
					if _, used := usedChars[childBranchUniqueParts]; !used {
						// Mark is as claimed
						usedChars[childBranchUniqueParts] = emptyVal
					} else {
						c.SetBranchID("")
					}
				} else {
					c.SetBranchID("")
				}
			} else {
				c.SetBranchID("")
			}
		}
		// init string with value of parent
		if len(c.GetBranchID()) == 0 {
			// WHILE branch ID is not already reserved

			iterationCount := 0
			for {
				iterationCount++
				c.SetBranchID(parentChain)
				for i := len(charPosList) - 1; i >= 0; i-- {
					c.SetBranchID(c.GetBranchID() + group.chars[charPosList[i]])
				}

				charPosList = increaseCharPos(charPosList, group.chars)
				if _, exists := reservedChars[strings.Replace(c.GetBranchID(), parentChain, "", 1)]; exists {
					// fmt.Printf("Child claimed '%v' but was already used, iteration loop is %v\n", c.BranchID, iterationCount)
				} else {
					break
				}
			}
		}

		c.SetBranchDepth(uint8(depth))
		group.Members[cID] = c
		if len(c.GetChildren()) > 0 {
			// Use recurssion to start processing this record's children
			group.calculateLineageChain(c.GetBranchID(), c.GetChildren(), depth+1)
		}
	}
}

func increaseCharPos(charPosList []int, chars []string) []int {
	charPosList[0]++

	// Increase base position and possible roll up the chain
	for i := 0; i < len(charPosList); i++ {
		// rollup the integer buffers
		if charPosList[i] >= len(chars) {
			charPosList[i+1]++
			charPosList[i] = 0
		}
	}
	return charPosList
}

// TimeTrack is used for reporting on duration between an intial time stamp and now
func TimeTrack(start time.Time, name string) {
	elapsed := time.Since(start)
	if reportTimeTracking {
		fmt.Printf("%s took %s\n", name, elapsed)
	}
}

// PrintMemUsage outputs the current, total and OS memory being used. As well as the number
// of garage collection cycles completed.
func PrintMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	if reportMem {
		fmt.Printf("Alloc = %v MiB", bToMb(m.Alloc))
		fmt.Printf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
		fmt.Printf("\tSys = %v MiB", bToMb(m.Sys))
		fmt.Printf("\tNumGC = %v\n", m.NumGC)
	}
}
func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}
