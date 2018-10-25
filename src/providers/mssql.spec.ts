import { expect } from 'chai';
import 'mocha';

import * as t from 'io-ts'
import * as tdc from 'io-ts-derive-class'

import { registerTable, from, ISNULL, val } from '../index';
import * as query from '../sqlquery';

import { toQuery } from './mssql'
import { DBSchema, ColumnSchema } from 'dbschema-inator';

const PersonType = t.type({
    ID: t.Integer,
    FirstName: t.string,
    LastName: t.string,
})

class Person extends tdc.DeriveClass(PersonType) {}

registerTable(Person, 'TestDB.dbo.Person');

const AddressType = t.type({
    ID: t.Integer,
    PersonID: t.Integer,
    StreetAddress1: t.string,
    StreetAddress2: t.union([t.string, t.null])
})

class Address extends tdc.DeriveClass(AddressType) {}

registerTable(Address, 'TestDB.dbo.Address');

const testDBSchema: DBSchema = {
    name: 'TestDB',
    tables: [
        { 
            name: { db_name: 'TestDB', schema: 'dbo', name: 'Person'},
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
                child_table: { db_name: 'TestDB', schema: 'dbo', name: 'Address'}, 
                constraint_name: { db_name: 'TestDB', schema: 'dbo', name: 'FK_Address_ToPerson'}, 
                column_map: [{ column: 'ID', child_column: 'PersonID' }]
            }],
            one_to_ones: [],
            primary_keys: ['ID'],
            type: 'BASE TABLE',
            unique_constraints: []
        },
        { 
            name: { db_name: 'TestDB', schema: 'dbo', name: 'Address'},
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
                parent_table: { db_name: 'TestDB', schema: 'dbo', name: 'Person'}, 
                constraint_name: { db_name: 'TestDB', schema: 'dbo', name: 'FK_Address_ToPerson'}, 
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

describe('mssql query tests', () => {
    it('Can generate Select * query', async () => {
        const query = from(Person, 'p').selectAll()
        const result = toQuery(testDBSchema, query.expr);

        expect(result.sql).eq(`select p.[ID], p.[FirstName], p.[LastName] from [TestDB].[dbo].[Person] as p`)
    })

    it('Can select individual columns from table', async () => {
        const query = from(Person, 'p').select(p => { return { ID: p.ID }});
        const result = toQuery(testDBSchema, query.expr);

        expect(result.sql).eq(`select p.[ID] from [TestDB].[dbo].[Person] as p`)
    })

    it('Can call select with scalar function', async () => {
        const isNullExpr = ISNULL(val(null), val(''));
        const query = from(Person, 'p').select(p => { return { test: isNullExpr } });
        const result = toQuery(testDBSchema, query.expr);

        expect(result.sql).eq(`select (ISNULL(null, '')) as 'test' from [TestDB].[dbo].[Person] as p`)
    })

    it('Can select columns and additional calculated fields', async () => {
        const isNullExpr = ISNULL(val(null), val(''));
        const query = from(Person, 'p').select(p => { return { ID: p.ID, FirstName: p.FirstName, blah: isNullExpr }})
        const result = toQuery(testDBSchema, query.expr);

        expect(result.sql).eq(`select p.[ID], p.[FirstName], (ISNULL(null, '')) as 'blah' from [TestDB].[dbo].[Person] as p`)
    })

    it('Can select all columns using spread syntax', async () => {
        const isNullExpr = ISNULL(val(null), val(''));
        const query = from(Person, 'p').select(p => { return { ...p, blah: isNullExpr }})
        const result = toQuery(testDBSchema, query.expr);

        expect(result.sql).eq(`select p.[ID], p.[FirstName], p.[LastName], (ISNULL(null, '')) as 'blah' from [TestDB].[dbo].[Person] as p`)
    })

    it('Can filter using where clause', async () => {
        const query = from(Person, 'p').selectAll().where(p => p.FirstName.equals('Heinz').and(p.LastName.equals('Doofenschmirtz')))
        const result = toQuery(testDBSchema, query.expr);

        expect(result.sql).eq(`select p.[ID], p.[FirstName], p.[LastName] from [TestDB].[dbo].[Person] as p where p.[FirstName] = @v AND p.[LastName] = @v0`)
    })

    it('Multiple where clauses result in AND predicates', async () => {
        const query = from(Person, 'p').selectAll().where(p => p.FirstName.equals('Heinz')).where(p => p.LastName.equals('Doofenschmirtz'))
        const result = toQuery(testDBSchema, query.expr);

        expect(result.sql).eq(`select p.[ID], p.[FirstName], p.[LastName] from [TestDB].[dbo].[Person] as p where p.[FirstName] = @v AND p.[LastName] = @v0`)
    })

    it('Multiple selects results in inner query', async () => {
        const query = from(Person, 'p').selectAll().select(p => { return { ID: p.ID, FirstName: p.FirstName}}, 'p2')

        const result = toQuery(testDBSchema, query.expr);
        expect(result.sql).eq(`select p2.[ID], p2.[FirstName] from (select p.[ID], p.[FirstName], p.[LastName] from [TestDB].[dbo].[Person] as p) as p2`)
    })

    it('Join with on predicate generates correct query', async () => {
        const query = from(Person, 'p')
                        .join(Address, 'a').on(r => r.p.ID.equals(r.a.PersonID))
                        .innerJoin(Person, 'p2').on(r => r.p2.ID.equals(r.p.ID))
                        .select(r => { 
                            return { ...r.p, ...r.a }
                        })
                        

        const result = toQuery(testDBSchema, query.expr);

        expect(result.sql).eq(`select a.[ID], p.[FirstName], p.[LastName], a.[PersonID], a.[StreetAddress1], a.[StreetAddress2] from [TestDB].[dbo].[Person] as p join [TestDB].[dbo].[Address] as a on p.[ID] = a.[PersonID] join [TestDB].[dbo].[Person] as p2 on p2.[ID] = p.[ID]`)
    });

    it('Select can use spread operator and delete to return desired results', async () => {
        const query = from(Person, 'p')
                        .join(Address, 'a').on(r => r.p.ID.equals(r.a.PersonID))
                        .join(Person, 'p2').on(r => r.p2.ID.equals(r.p.ID))
                        .select(r => { 
                            const a: any = { ...r.a };
                            a.AddressID = a.ID;
                            delete a.ID
                            return { ...r.p, ...a }
                        })
                        

        const result = toQuery(testDBSchema, query.expr);

        expect(result.sql).eq(`select p.[ID], p.[FirstName], p.[LastName], a.[PersonID], a.[StreetAddress1], a.[StreetAddress2], (a.[ID]) as 'AddressID' from [TestDB].[dbo].[Person] as p join [TestDB].[dbo].[Address] as a on p.[ID] = a.[PersonID] join [TestDB].[dbo].[Person] as p2 on p2.[ID] = p.[ID]`);
    });

    it('Selecting a value results in a parameter being injected', async () => {
        const query = from(Person, 'p').select(p => { return { ID: val(1) }})

        const result = toQuery(testDBSchema, query.expr);
        
        expect(result.sql).eq(`select (@v) as 'ID' from [TestDB].[dbo].[Person] as p`)
    });

    it('left outer join returns nullable columns', async () => {
        const query = from(Person, 'p')
                        .leftOuterJoin(Address, 'a').on(r => r.p.ID.equals(r.a.PersonID))
                        .leftOuterJoin(Person, 'p2').on(r => r.a.PersonID.equals(r.p2.ID))
                        .select(r => { 
                            return { ...r.p, StreetAddress1: r.a.StreetAddress1, SecondFirstName: r.p2.FirstName }
                        });

        const result = toQuery(testDBSchema, query.expr);

        expect(result.sql).eq(`select p.[ID], p.[FirstName], p.[LastName], a.[StreetAddress1], (p2.[FirstName]) as 'SecondFirstName' from [TestDB].[dbo].[Person] as p left outer join [TestDB].[dbo].[Address] as a on p.[ID] = a.[PersonID] left outer join [TestDB].[dbo].[Person] as p2 on a.[PersonID] = p2.[ID]`)
    });

    it('right outer join returns nullable columns', async () => {
        const query = from(Person, 'p')
                        .rightOuterJoin(Address, 'a').on(r => r.p.ID.equals(r.a.PersonID))
                        .rightOuterJoin(Person, 'p2').on(r => r.a.PersonID.equals(r.p2.ID))
                        .select(r => { 
                            return { ...r.p2, StreetAddress1: r.a.StreetAddress1, FirstName2: r.p.FirstName }
                        });

        const result = toQuery(testDBSchema, query.expr);

        expect(result.sql).eq(`select p2.[ID], p2.[FirstName], p2.[LastName], a.[StreetAddress1], (p.[FirstName]) as 'FirstName2' from [TestDB].[dbo].[Person] as p right outer join [TestDB].[dbo].[Address] as a on p.[ID] = a.[PersonID] right outer join [TestDB].[dbo].[Person] as p2 on a.[PersonID] = p2.[ID]`)
    });

    it('subquery is allowed in join', async () => {
        const subquery = from(Person, 'p').selectAll().where(p => p.FirstName.equals('Heinz'))

        const query = from(Person, 'p')
                        .leftOuterJoin(Address, 'a').on(r => r.p.ID.equals(r.a.PersonID))
                        .leftOuterJoin(subquery, 'p2').on(r => r.a.PersonID.equals(r.p2.ID))
                        .select(r => { 
                            return { ...r.p, StreetAddress1: r.a.StreetAddress1, SecondFirstName: r.p2.FirstName }
                        });

        const result = toQuery(testDBSchema, query.expr);

        expect(result.sql).eq(`select p.[ID], p.[FirstName], p.[LastName], a.[StreetAddress1], (p2.[FirstName]) as 'SecondFirstName' from [TestDB].[dbo].[Person] as p left outer join [TestDB].[dbo].[Address] as a on p.[ID] = a.[PersonID] left outer join (select p.[ID], p.[FirstName], p.[LastName] from [TestDB].[dbo].[Person] as p where p.[FirstName] = @v) as p2 on a.[PersonID] = p2.[ID]`);
    });
});

