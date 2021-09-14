/*
@Time : 2021/9/14 10:33 上午
@Author : 21
@File : gdb_driver_clickhouse
@Software: GoLand
*/
package gdb

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/gogf/gf/text/gstr"
	"github.com/gogf/gf/util/gconv"
	"strings"
)

// DriverMysql is the driver for mysql database.
type DriverClickHouse struct {
	*Core
}

// New creates and returns a database object for clickhouse.
// It implements the interface of gdb.Driver for extra database driver installation.
func (d *DriverClickHouse) New(core *Core, node *ConfigNode) (DB, error) {
	return &DriverClickHouse{
		Core: core,
	}, nil
}

func (d *DriverClickHouse) FilteredLink() string {
	return ""
}


// Open creates and returns a underlying sql.DB object for clickhouse.
// Note that it converts time.Time argument to local timezone in default.
func (d *DriverClickHouse) Open(config *ConfigNode) (*sql.DB, error) {
	source := ""
	if config.Pass != "" {
		source = fmt.Sprintf(
			"tcp://%s:%s?database=%s&password=%s&charset=%s&debug=%s",
			config.Host, config.Port, config.Name, config.Pass, config.Charset, gconv.String(config.Debug),
		)
	} else {
		source = fmt.Sprintf(
			"tcp://%s:%s?database=%s&charset=%s&debug=%s",
			config.Host, config.Port, config.Name, config.Charset, gconv.String(config.Debug),
		)
	}
	//glog.Infof("Open: %s %s", source, clickhouse.DefaultDatabase)
	if db, err := sql.Open("clickhouse", source); err == nil {
		d.SetSchema(config.Name)
		return db, nil
	} else {
		return nil, err
	}
}

// Tables retrieves and returns the tables of current schema.
// It's mainly used in cli tool chain for automatically generating the models.
func (d *DriverClickHouse) Tables(ctx context.Context, schema ...string) (tables []string, err error) {
	var result Result
	link, err := d.SlaveLink(schema...)
	if err != nil {
		return nil, err
	}
	result, err = d.DoGetAll(ctx, link, `SHOW TABLES`)
	if err != nil {
		return
	}
	for _, m := range result {
		for _, v := range m {
			tables = append(tables, v.String())
		}
	}
	return
}

// TableFields retrieves and returns the fields information of specified table of current
// schema.
//
// The parameter `link` is optional, if given nil it automatically retrieves a raw sql connection
// as its link to proceed necessary sql query.
//
// Note that it returns a map containing the field name and its corresponding fields.
// As a map is unsorted, the TableField struct has a "Index" field marks its sequence in
// the fields.
//
// It's using cache feature to enhance the performance, which is never expired util the
// process restarts.
func (d *DriverClickHouse) TableFields(ctx context.Context, table string, schema ...string) (fields map[string]*TableField, err error) {
	link, err := d.SlaveLink(schema...)
	if err != nil {
		return nil, err
	}
	getColumnsSql := fmt.Sprintf("select * from `system`.columns c where database = '%s' and `table` = '%s'", d.GetSchema(), table)
	result, err := d.DoGetAll(ctx, link, getColumnsSql)
	if err != nil {
		return nil, err
	}
	fields = make(map[string]*TableField)
	for _, m := range result {
		fields[m["name"].String()] = &TableField{
			Index:   m["position"].Int(),
			Name:    m["name"].String(),
			Type:    m["type"].String(),
			Comment: m["comment"].String(),
		}
	}
	return fields, nil
}

func (d *DriverClickHouse) DoInsert(ctx context.Context, link Link, table string, list List, option DoInsertOption) (result sql.Result, err error) {
	var (
		keys           []string      // Field names.
		values         []string      // Value holder string array, like: (?,?,?)
		params         []interface{} // Values that will be committed to underlying database driver.
		onDuplicateStr string        // onDuplicateStr is used in "ON DUPLICATE KEY UPDATE" statement.
	)
	// Handle the field names and place holders.
	for k, _ := range list[0] {
		keys = append(keys, k)
	}
	// Prepare the batch result pointer.
	var (
		charL, charR = d.GetChars()
		batchResult  = new(SqlResult)
		keysStr      = charL + strings.Join(keys, charR+","+charL) + charR
		operation    = GetInsertOperationByOption(option.InsertOption)
	)
	var (
		listLength  = len(list)
		valueHolder = make([]string, 0)
	)
	for i := 0; i < listLength; i++ {
		values = values[:0]
		// Note that the map type is unordered,
		// so it should use slice+key to retrieve the value.
		for _, k := range keys {
			if s, ok := list[i][k].(Raw); ok {
				values = append(values, gconv.String(s))
			} else {
				values = append(values, "?")
				params = append(params, list[i][k])
			}
		}
		valueHolder = append(valueHolder, "("+gstr.Join(values, ",")+")")
		// Batch package checks: It meets the batch number or it is the last element.
		if len(valueHolder) == option.BatchCount || (i == listLength-1 && len(valueHolder) > 0) {
			r, err := d.DoExec(ctx, link, fmt.Sprintf(
				"%s INTO %s(%s) VALUES%s %s",
				operation, d.QuotePrefixTableName(table), keysStr,
				gstr.Join(valueHolder, ","),
				onDuplicateStr,
			), params...)
			if err != nil {
				return r, err
			}
			params = params[:0]
			valueHolder = valueHolder[:0]
		}
	}
	return batchResult, nil
}