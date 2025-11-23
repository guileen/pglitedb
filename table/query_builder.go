package table

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// Alteration represents a table alteration
type Alteration struct {
	Type    string            `json:"type"` // "add_column", "drop_column", "modify_column", "add_index", "drop_index"
	Column  *ColumnDefinition `json:"column,omitempty"`
	Index   *IndexDefinition  `json:"index,omitempty"`
	OldName string            `json:"old_name,omitempty"`
	NewName string            `json:"new_name,omitempty"`
}

// queryBuilder implements QueryBuilder interface
type queryBuilder struct {
	manager *Manager
	table   string
	query   string
	args    []interface{}
	err     error
}

// NewQueryBuilder creates a new query builder
func NewQueryBuilder(manager *Manager, table string) QueryBuilder {
	return &queryBuilder{
		manager: manager,
		table:   table,
	}
}

func (qb *queryBuilder) Select(columns ...string) QueryBuilder {
	if len(columns) == 0 {
		qb.query = "SELECT *"
	} else {
		qb.query = "SELECT " + strings.Join(columns, ", ")
	}
	qb.query += " FROM " + qb.table
	return qb
}

func (qb *queryBuilder) Where(condition string, args ...interface{}) QueryBuilder {
	if qb.query == "" {
		qb.query = "SELECT * FROM " + qb.table
	}

	if strings.Contains(qb.query, "WHERE") {
		qb.query += " AND " + condition
	} else {
		qb.query += " WHERE " + condition
	}
	qb.args = append(qb.args, args...)
	return qb
}

func (qb *queryBuilder) WhereIn(column string, values []interface{}) QueryBuilder {
	if len(values) == 0 {
		qb.err = fmt.Errorf("WhereIn requires at least one value")
		return qb
	}

	placeholders := make([]string, len(values))
	for i := range values {
		placeholders[i] = "?"
	}

	condition := fmt.Sprintf("%s IN (%s)", column, strings.Join(placeholders, ", "))
	return qb.Where(condition, values...)
}

func (qb *queryBuilder) WhereNull(column string) QueryBuilder {
	return qb.Where(fmt.Sprintf("%s IS NULL", column))
}

func (qb *queryBuilder) WhereNotNull(column string) QueryBuilder {
	return qb.Where(fmt.Sprintf("%s IS NOT NULL", column))
}

func (qb *queryBuilder) OrderBy(column string, direction ...string) QueryBuilder {
	dir := "ASC"
	if len(direction) > 0 {
		dir = strings.ToUpper(direction[0])
	}

	if qb.query == "" {
		qb.query = "SELECT * FROM " + qb.table
	}

	if strings.Contains(qb.query, "ORDER BY") {
		qb.query += ", " + column + " " + dir
	} else {
		qb.query += " ORDER BY " + column + " " + dir
	}
	return qb
}

func (qb *queryBuilder) GroupBy(columns ...string) QueryBuilder {
	if qb.query == "" {
		qb.query = "SELECT * FROM " + qb.table
	}

	qb.query += " GROUP BY " + strings.Join(columns, ", ")
	return qb
}

func (qb *queryBuilder) Having(condition string, args ...interface{}) QueryBuilder {
	if qb.query == "" {
		qb.err = fmt.Errorf("Having requires a preceding GroupBy")
		return qb
	}

	qb.query += " HAVING " + condition
	qb.args = append(qb.args, args...)
	return qb
}

func (qb *queryBuilder) Limit(limit int) QueryBuilder {
	if qb.query == "" {
		qb.query = "SELECT * FROM " + qb.table
	}

	qb.query += " LIMIT ?"
	qb.args = append(qb.args, limit)
	return qb
}

func (qb *queryBuilder) Offset(offset int) QueryBuilder {
	if qb.query == "" {
		qb.query = "SELECT * FROM " + qb.table
	}

	qb.query += " OFFSET ?"
	qb.args = append(qb.args, offset)
	return qb
}

func (qb *queryBuilder) Join(table, condition string) QueryBuilder {
	if qb.query == "" {
		qb.query = "SELECT * FROM " + qb.table
	}

	qb.query += " JOIN " + table + " ON " + condition
	return qb
}

func (qb *queryBuilder) LeftJoin(table, condition string) QueryBuilder {
	if qb.query == "" {
		qb.query = "SELECT * FROM " + qb.table
	}

	qb.query += " LEFT JOIN " + table + " ON " + condition
	return qb
}

func (qb *queryBuilder) RightJoin(table, condition string) QueryBuilder {
	if qb.query == "" {
		qb.query = "SELECT * FROM " + qb.table
	}

	qb.query += " RIGHT JOIN " + table + " ON " + condition
	return qb
}

func (qb *queryBuilder) Build() (string, []interface{}, error) {
	if qb.err != nil {
		return "", nil, qb.err
	}
	if qb.query == "" {
		qb.query = "SELECT * FROM " + qb.table
	}
	return qb.query, qb.args, nil
}

func (qb *queryBuilder) Count() (int64, error) {
	// Build count query
	countQuery := qb.buildCountQuery()

	var count int64
	err := qb.manager.db.QueryRow(countQuery, qb.args...).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count records: %w", err)
	}

	return count, nil
}

func (qb *queryBuilder) First() (*Record, error) {
	if qb.query == "" {
		qb.query = "SELECT * FROM " + qb.table
	}
	qb.query += " LIMIT 1"

	ctx := context.Background()
	return qb.executeQuery(ctx)
}

func (qb *queryBuilder) Get() ([]*Record, error) {
	if qb.query == "" {
		qb.query = "SELECT * FROM " + qb.table
	}

	ctx := context.Background()
	return qb.executeQueryMany(ctx)
}

func (qb *queryBuilder) Create(data map[string]interface{}) (*Record, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("no data provided for insert")
	}

	columns := make([]string, 0, len(data))
	placeholders := make([]string, 0, len(data))
	values := make([]interface{}, 0, len(data))

	for column, value := range data {
		columns = append(columns, column)
		placeholders = append(placeholders, "?")
		values = append(values, value)
	}

	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		qb.table,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
	)

	ctx := context.Background()
	result, err := qb.manager.db.ExecContext(ctx, query, values...)
	if err != nil {
		return nil, fmt.Errorf("failed to insert record: %w", err)
	}

	id, err := result.LastInsertId()
	if err != nil {
		return nil, fmt.Errorf("failed to get insert ID: %w", err)
	}

	// Return the created record
	return qb.Where("id = ?", id).First()
}

func (qb *queryBuilder) CreateBatch(records []map[string]interface{}) ([]*Record, error) {
	if len(records) == 0 {
		return nil, fmt.Errorf("no records provided for batch insert")
	}

	// For simplicity, insert one by one
	// In production, use batch INSERT
	var results []*Record
	for _, record := range records {
		created, err := qb.Create(record)
		if err != nil {
			return nil, fmt.Errorf("failed to insert record in batch: %w", err)
		}
		results = append(results, created)
	}

	return results, nil
}

func (qb *queryBuilder) Update(data map[string]interface{}) error {
	if len(data) == 0 {
		return fmt.Errorf("no data provided for update")
	}

	setClauses := make([]string, 0, len(data))
	values := make([]interface{}, 0, len(data))

	for column, value := range data {
		setClauses = append(setClauses, fmt.Sprintf("%s = ?", column))
		values = append(values, value)
	}

	query := fmt.Sprintf(
		"UPDATE %s SET %s",
		qb.table,
		strings.Join(setClauses, ", "),
	)

	// Append WHERE clause arguments
	values = append(values, qb.args...)

	ctx := context.Background()
	_, err := qb.manager.db.ExecContext(ctx, query, values...)
	if err != nil {
		return fmt.Errorf("failed to update records: %w", err)
	}

	return nil
}

func (qb *queryBuilder) Delete() error {
	if qb.query == "" {
		qb.query = "DELETE FROM " + qb.table
	}

	ctx := context.Background()
	_, err := qb.manager.db.ExecContext(ctx, qb.query, qb.args...)
	if err != nil {
		return fmt.Errorf("failed to delete records: %w", err)
	}

	return nil
}

// Private helper methods

func (qb *queryBuilder) executeQuery(ctx context.Context) (*Record, error) {
	rows, err := qb.manager.db.QueryContext(ctx, qb.query, qb.args...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		return nil, ErrRecordNotFound
	}

	record, err := qb.scanRecord(rows)
	if err != nil {
		return nil, fmt.Errorf("failed to scan record: %w", err)
	}

	return record, nil
}

func (qb *queryBuilder) executeQueryMany(ctx context.Context) ([]*Record, error) {
	rows, err := qb.manager.db.QueryContext(ctx, qb.query, qb.args...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	var records []*Record
	for rows.Next() {
		record, err := qb.scanRecord(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan record: %w", err)
		}
		records = append(records, record)
	}

	return records, nil
}

func (qb *queryBuilder) scanRecord(rows *sql.Rows) (*Record, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	// Create slice to hold values
	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range columns {
		valuePtrs[i] = &values[i]
	}

	// Scan the row
	if err := rows.Scan(valuePtrs...); err != nil {
		return nil, err
	}

	// Convert to record format
	data := make(map[string]*Value)
	for i, column := range columns {
		data[column] = &Value{
			Data: values[i],
			Type: ColumnTypeString, // Default type, would be determined from schema
		}
	}

	return &Record{
		Table: qb.table,
		Data:  data,
	}, nil
}

func (qb *queryBuilder) buildCountQuery() string {
	if qb.query == "" {
		return "SELECT COUNT(*) FROM " + qb.table
	}

	// Replace SELECT clause with COUNT(*)
	query := qb.query

	// Find SELECT clause and replace with COUNT(*)
	selectIndex := strings.Index(strings.ToUpper(query), "SELECT")
	if selectIndex != -1 {
		fromIndex := strings.Index(strings.ToUpper(query), "FROM")
		if fromIndex != -1 {
			query = "SELECT COUNT(*)" + query[fromIndex:]
		}
	}

	return query
}
