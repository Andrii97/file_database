package main

import (
	"testing"
	"strings"
)

func getTab(tables *Cache, name string) *Table {
	for i := range *tables {
		if (*tables)[i].name == name {
			return (*tables)[i]
		}
	}
	table := DecodeJSON(name)

	if table != nil {
		*tables = append(*tables, table)
	}
	return table
}

func set(tables *Cache, query_split []string) string {
	if len(query_split) >= 4 {
		table := getTab(tables, query_split[0])
		if (table == nil) {
			table = NewTable(query_split[0], make(map[string]string))
		} 
		
		table.data[query_split[2]] = strings.Join(query_split[3: ], " ")
		return "OK"
	} else {
		return "Error"
	}
}

func get(tables *Cache, query_split []string) string {
	if len(query_split) == 3  {
		table := getTable(tables, query_split[0])
		if (table == nil) {
			return "table not exists"
		} else {
			value, ok := table.data[query_split[2]]
			if ok {
				return string(value)
			} else {
				return "value not exists"
			}
		}
	} else {
		return "Error"
	}
}

func del(tables *Cache, query_split []string) string {
	if len(query_split) == 3  {
		table := getTable(tables, query_split[0])
		if (table == nil) {
			return "table not exists"
		} else {
			_, ok := table.data[query_split[2]]
			if ok {
				delete(table.data, query_split[2])
				return "key deleted"
			} else {
				return "key not exists"
			}
		}
	} else {
		return "Error"
	}
}


func TestSet(t *testing.T) {
	var tables Cache
	query := "name1 set 11 28" 
	query_split := strings.Fields(query)
	if set(&tables, query_split) == "OK"{
		t.Log("Test: Set operation to existing file/nStatus: passed successful.")
	} else {
		t.Error("FAILED: Set operation in existing file.")
	}

	query1 := "name1012 set user boris" 
	query_split1 := strings.Fields(query1)
	if set(&tables, query_split1) == "OK"{
		t.Log("Test: Set operation with creating of new file/nStatus: passed successful.")
	} else {
		t.Error("FAILED: Set operation with creating of new file.")
	}
	
}


func TestGet(t *testing.T) {
	var tables Cache
	query := "name1 get user" 
	query_split := strings.Fields(query)
	if  get(&tables, query_split) == "12"{
		t.Log("Test: Get operation from existing file/nStatus: passed successful.")
	} else {
		t.Error("FAILED: Get operation from existing file with existing key.")
	}

	query1 := "name1012 get user" 
	query_split1 := strings.Fields(query1)
	if get(&tables, query_split1) == "table not exists"{
		t.Log("Test: Get operation from unknown table /nStatus: passed successful.")
	} else {
		t.Error("FAILED: Get operation from unknown table.")
	}

	query2 := "name1 get login"
	query_split2 := strings.Fields(query2)
	if get(&tables, query_split2) == "value not exists"{
		t.Log("Test: Get operation with unknown value/nStatus: passed successful.")
	} else {
		t.Error("FAILED: Get operation with unknown value.")
	}
}


func TestDel(t *testing.T) {
	var tables Cache
	query := "name1 del 1" 
	query_split := strings.Fields(query)
	if del(&tables, query_split) == "key deleted"{
		t.Log("Test: Delete operation in existing file/nStatus: passed successful.")
	} else {
		t.Error("FAILED: Delete operation in existing file.")
	}

	query1 := "name1012 del user" 
	query_split1 := strings.Fields(query1)
	if del(&tables, query_split1) == "table not exists"{
		t.Log("Test: Delete operation in unknown table/nStatus: passed successful.")
	} else {
		t.Error("FAILED: Delete operation in unknown file.")
	}

	query2 := "name1 del login" 
	query_split2 := strings.Fields(query2)
	if del(&tables, query_split2) == "key not exists"{
		t.Log("Test: Delete operation for unknown key/nStatus: passed successful.")
	} else {
		t.Error("FAILED: Delete operation for unknown key.")
	}
}
