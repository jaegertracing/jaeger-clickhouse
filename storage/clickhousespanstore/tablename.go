package clickhousespanstore

type TableName string

func (tableName TableName) ToLocal() TableName {
	return tableName + "_local"
}
