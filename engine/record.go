package engine

// Record interface to enable surfacing data to the engine
type Record interface {
	GetID() uint32
	GetParentID() uint32
	GetChildren() []uint32
	SetChildren([]uint32)
	GetBranchID() string
	SetBranchID(string)
	SetBranchDepth(uint8)
}

type record struct {
	id                uint32
	parentID          uint32
	children          []uint32
	branchDepth       uint8
	isChanged         bool
	branchID          string
	parentBranchID    string
	parentBranchDepth uint8
}

func (r *record) GetID() uint32 {
	return r.id
}
func (r *record) GetParentID() uint32 {
	return r.parentID
}
func (r *record) GetChildren() []uint32 {
	return r.children
}
func (r *record) SetChildren(children []uint32) {
	r.children = children
}
func (r *record) GetBranchID() string {
	return r.branchID
}
func (r *record) SetBranchID(branchID string) {
	r.isChanged = true
	r.branchID = branchID
}
func (r *record) GetParentBranchID() string {
	return r.parentBranchID
}
func (r *record) SetBranchDepth(branchDepth uint8) {
	r.branchDepth = branchDepth
}
func (r *record) GetIsChanged() bool {
	return r.isChanged
}
