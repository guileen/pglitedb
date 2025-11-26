package sql

type StatementType int

const (
	SelectStatement StatementType = iota
	InsertStatement
	UpdateStatement
	DeleteStatement
	BeginStatement
	CommitStatement
	RollbackStatement
	CreateTableStatement
	DropTableStatement
	CreateIndexStatement
	DropIndexStatement
	UnknownStatement
)

type ParsedQuery struct {
	Type             StatementType
	Statement        interface{}
	Query            string
	ReturningColumns []string
}

type Statement interface {
	StatementNode()
}

type ParameterPlaceholder struct {
	Index int
}

type Parser interface {
	Parse(query string) (*ParsedQuery, error)
	
	ParseWithParams(query string, paramCount int) (*ParsedQuery, error)
	
	Validate(query string) error
	
	GetStatementType(stmt interface{}) StatementType
	
	SupportsParameterPlaceholders() bool
}