import * as t from 'io-ts'
import * as tdc from 'io-ts-derive-class'

import { registerTable, from, ISNULL, val, ROW_NUMBER, COUNT, AVG, MAX, MIN, SUM } from './index';
import * as ut from './untyped_ast'

import { toQuery } from './mssql'
import { DBSchema, ColumnSchema, TableSchema } from 'dbschema-inator';

const tableCache: { [key: string]: TableSchema } = {};

export function getFindTable(tableName: string) {
    const cached = tableCache[tableName];
    if(cached !== undefined){
        return cached;
    }

    const table = dbschema.tables.find(t => `${t.name.schema}.${t.name.name}` === tableName);
    if(table === undefined){
        throw new Error(`Could not find table ${tableName} in schema!`);
    }

    tableCache[tableName] = table;
    return table;
}

export function toUnsafeQuery(expr: ut.SelectStatementExpr){
    return toQuery(expr, {
        allowSqlInjection: true
    })
}

export function toSafeQuery(expr: ut.SelectStatementExpr){
    return toQuery(expr, {
        allowSqlInjection: false,
        getTableSchema: getFindTable
    })
}

const PersonType = t.type({
    ID: t.Integer,
    FirstName: t.string,
    LastName: t.string,
})

export class Person extends tdc.DeriveClass(PersonType) {}

registerTable(Person, 'dbo.Person');

const AddressType = t.type({
    ID: t.Integer,
    PersonID: t.Integer,
    StreetAddress1: t.string,
    StreetAddress2: t.union([t.string, t.null])
})

export class Address extends tdc.DeriveClass(AddressType) {}

registerTable(Address, 'dbo.Address');

const dbschema: DBSchema = {
    name: 'sqlquery-inator',
    tables: [
        { 
            name: { db_name: 'sqlquery-inator', schema: 'dbo', name: 'Person'},
            columns: [
                { 
                    name: 'ID',
                    is_only_primary_key: true,
                    is_part_of_primary_key: true,
                    is_identity: true,
                    db_default: null,
                    max_length: null,
                    db_type: 'int',
                    is_nullable: false,
                    is_only_member_of_unique_constraint: false,
                    is_part_of_unique_constraint: false,
                },
                { 
                    name: 'FirstName',
                    db_type: 'nvarchar',
                    max_length: 100,
                    is_nullable: false,
                    is_only_primary_key: false,
                    is_part_of_primary_key: false,
                    is_identity: false,
                    db_default: null,
                    is_only_member_of_unique_constraint: false,
                    is_part_of_unique_constraint: false,
                },
                { 
                    name: 'LastName',
                    db_type: 'nvarchar',
                    max_length: 100,
                    is_nullable: false,
                    is_only_primary_key: false,
                    is_part_of_primary_key: false,
                    is_identity: false,
                    db_default: null,
                    is_only_member_of_unique_constraint: false,
                    is_part_of_unique_constraint: false,
                }
            ],
            many_to_ones: [],
            one_to_manys: [{
                child_table: { db_name: 'sqlquery-inator', schema: 'dbo', name: 'Address'}, 
                constraint_name: { db_name: 'sqlquery-inator', schema: 'dbo', name: 'FK_Address_ToPerson'}, 
                column_map: [{ column: 'ID', child_column: 'PersonID' }]
            }],
            one_to_ones: [],
            primary_keys: ['ID'],
            type: 'BASE TABLE',
            unique_constraints: []
        },
        { 
            name: { db_name: 'sqlquery-inator', schema: 'dbo', name: 'Address'},
            columns: [
                { 
                    name: 'ID',
                    is_only_primary_key: true,
                    is_part_of_primary_key: true,
                    is_identity: true,
                    db_default: null,
                    max_length: null,
                    db_type: 'int',
                    is_nullable: false,
                    is_only_member_of_unique_constraint: false,
                    is_part_of_unique_constraint: false,
                },
                { 
                    name: 'PersonID',
                    is_only_primary_key: false,
                    is_part_of_primary_key: false,
                    is_identity: false,
                    db_default: null,
                    max_length: null,
                    db_type: 'int',
                    is_nullable: false,
                    is_only_member_of_unique_constraint: false,
                    is_part_of_unique_constraint: false,
                },
                { 
                    name: 'StreetAddress1',
                    db_type: 'nvarchar',
                    max_length: 100,
                    is_nullable: false,
                    is_only_primary_key: false,
                    is_part_of_primary_key: false,
                    is_identity: false,
                    db_default: null,
                    is_only_member_of_unique_constraint: false,
                    is_part_of_unique_constraint: false,
                },
                { 
                    name: 'StreetAddress2',
                    db_type: 'nvarchar',
                    max_length: 100,
                    is_nullable: true,
                    is_only_primary_key: false,
                    is_part_of_primary_key: false,
                    is_identity: false,
                    db_default: null,
                    is_only_member_of_unique_constraint: false,
                    is_part_of_unique_constraint: false,
                }
            ],
            many_to_ones: [{ 
                parent_table: { db_name: 'sqlquery-inator', schema: 'dbo', name: 'Person'}, 
                constraint_name: { db_name: 'sqlquery-inator', schema: 'dbo', name: 'FK_Address_ToPerson'}, 
                column_map: [{ column: 'PersonID', parent_column: 'ID' }]
            }],
            one_to_manys: [],
            one_to_ones: [],
            primary_keys: ['ID'],
            type: 'BASE TABLE',
            unique_constraints: []
        },
    ]
}