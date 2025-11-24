package types

// QueryBuilder builds database queries
type QueryBuilder interface {
	Select(columns ...string) QueryBuilder
	Where(condition string, args ...interface{}) QueryBuilder
	WhereIn(column string, values []interface{}) QueryBuilder
	WhereNull(column string) QueryBuilder
	WhereNotNull(column string) QueryBuilder
	OrderBy(column string, direction ...string) QueryBuilder
	GroupBy(columns ...string) QueryBuilder
	Having(condition string, args ...interface{}) QueryBuilder
	Limit(limit int) QueryBuilder
	Offset(offset int) QueryBuilder
	Join(table, condition string) QueryBuilder
	LeftJoin(table, condition string) QueryBuilder
	RightJoin(table, condition string) QueryBuilder

	Build() (string, []interface{}, error)
	Count() (int64, error)
	First() (*Record, error)
	Get() ([]*Record, error)
	Create(data map[string]interface{}) (*Record, error)
	CreateBatch(records []map[string]interface{}) ([]*Record, error)
	Update(data map[string]interface{}) error
	Delete() error
}