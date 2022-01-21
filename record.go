package main

import "git.doterra.net/salesforce/hierarchy-calculation-engine/engine"

type parentMode = string

const (
	parent1 parentMode = "parent1"
	parent2 parentMode = "parent2"
)

type record struct {
	id                 uint32
	children           []uint32
	isChanged          bool
	parentMode         parentMode
	sfID               string
	parent1SFID        string
	parent2SFID        string
	parent1BranchID    string
	parent2BranchID    string
	parent1BranchDepth uint8
	parent2BranchDepth uint8
	idLookupTable      *map[string]uint32
}

func (r *record) ChangeParentMode(parentMode string) {
	r.parentMode = parentMode
	r.children = []uint32{}
}
func (r *record) GetID() uint32 {
	return r.id
}
func (r *record) GetParentID() uint32 {
	parent := ""
	switch r.parentMode {
	case parent1:
		parent = r.parent1SFID
	case parent2:
		parent = r.parent2SFID
	}
	parentID := engine.Uint32Max
	if parentIDVal, has := (*r.idLookupTable)[parent]; has {
		parentID = parentIDVal
	}
	return parentID
}
func (r *record) GetChildren() []uint32 {
	return r.children
}
func (r *record) SetChildren(children []uint32) {
	r.children = children
}
func (r *record) GetBranchID() string {
	switch r.parentMode {
	case parent1:
		return r.parent1BranchID
	case parent2:
		return r.parent2BranchID
	}
	return ""
}
func (r *record) SetBranchID(branchID string) {
	r.isChanged = true
	switch r.parentMode {
	case parent1:
		r.parent1BranchID = branchID
	case parent2:
		r.parent2BranchID = branchID
	}
}
func (r *record) SetBranchDepth(branchDepth uint8) {
	switch r.parentMode {
	case parent1:
		r.parent1BranchDepth = branchDepth
	case parent2:
		r.parent2BranchDepth = branchDepth
	}
}
func (r *record) GetIsChanged() bool {
	return r.isChanged
}

// Fields used to export data for SFDC Bulk API, used by package
func (r record) Fields() map[string]interface{} {
	return map[string]interface{}{
		"Id":                        r.sfID,
		"Parent_1_Lineage_Chain__c": r.parent1BranchID,
		"Parent_1_Lineage_Depth__c": r.parent1BranchDepth,
		"Parent_2_Lineage_Chain__c": r.parent2BranchID,
		"Parent_2_Lineage_Depth__c": r.parent2BranchDepth,
	}
}

// InsertNull used to submit values in nullable mode, used by package
func (r record) InsertNull() bool {
	return true
}
