import { expect, use } from 'chai';
import 'mocha';

import * as t from 'io-ts'
import * as tdc from 'io-ts-derive-class'

import { registerTable, from, ISNULL, val, ROW_NUMBER } from '../index';
import * as query from '../sqlquery';

import { toQuery } from './mssql'
import { DBSchema, ColumnSchema } from 'dbschema-inator';
import { join } from 'path';

const PersonType = t.type({
    ID: t.Integer,
    FirstName: t.string,
    LastName: t.string,
})

class Person extends tdc.DeriveClass(PersonType) {}

registerTable(Person, 'sqlquery-inator.dbo.Person');

const AddressType = t.type({
    ID: t.Integer,
    PersonID: t.Integer,
    StreetAddress1: t.string,
    StreetAddress2: t.union([t.string, t.null])
})

class Address extends tdc.DeriveClass(AddressType) {}

registerTable(Address, 'sqlquery-inator.dbo.Address');

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

function compare(actual: string, expected: string){
    actual = actual.trim()
    expected = expected.trim()
    return expect(actual).eq(expected);
}

describe('mssql query tests', () => {
    it('Can generate Select * query', async () => {
        const query = from(Person, 'p').selectAll()
        const result = toQuery(dbschema, query.expr);

        compare(result.sql, 
`select
    p.[ID],
    p.[FirstName],
    p.[LastName]
from [sqlquery-inator].[dbo].[Person] as p`)
    })

    it('Can select individual columns from table', async () => {
        const query = from(Person, 'p').select(p => { return { ID: p.ID }});
        const result = toQuery(dbschema, query.expr);

        compare(result.sql, 
`select
    p.[ID]
from [sqlquery-inator].[dbo].[Person] as p`)
    })

    it('Can call select with scalar function', async () => {
        const isNullExpr = ISNULL(val(null), val(''));
        const query = from(Person, 'p').select(p => { return { test: isNullExpr } });
        const result = toQuery(dbschema, query.expr);

        compare(result.sql, 
`select
    (ISNULL(null, '')) as 'test'
from [sqlquery-inator].[dbo].[Person] as p`)
    })

    it('Can select columns and additional calculated fields', async () => {
        const isNullExpr = ISNULL(val(null), val(''));
        const query = from(Person, 'p').select(p => { return { ID: p.ID, FirstName: p.FirstName, blah: isNullExpr }})
        const result = toQuery(dbschema, query.expr);

        compare(result.sql, 
`select
    p.[ID],
    p.[FirstName],
    (ISNULL(null, '')) as 'blah'
from [sqlquery-inator].[dbo].[Person] as p`)
    })

    it('Can select all columns using spread syntax', async () => {
        const isNullExpr = ISNULL(val(null), val(''));
        const query = from(Person, 'p').select(p => { return { ...p, blah: isNullExpr }})
        const result = toQuery(dbschema, query.expr);

        compare(result.sql, 
`select
    p.[ID],
    p.[FirstName],
    p.[LastName],
    (ISNULL(null, '')) as 'blah'
from [sqlquery-inator].[dbo].[Person] as p`)
    })

    it('Can filter using where clause', async () => {
        const query = from(Person, 'p').selectAll().where(p => p.FirstName.equals('Heinz').and(p.LastName.equals('Doofenschmirtz')))
        const result = toQuery(dbschema, query.expr);

        compare(result.sql, 
`select
    p.[ID],
    p.[FirstName],
    p.[LastName]
from [sqlquery-inator].[dbo].[Person] as p
where p.[FirstName] = @v AND p.[LastName] = @v0`)
    })

    it('Multiple where clauses result in AND predicates', async () => {
        const query = from(Person, 'p').selectAll().where(p => p.FirstName.equals('Heinz')).where(p => p.LastName.equals('Doofenschmirtz'))
        const result = toQuery(dbschema, query.expr);

        compare(result.sql, 
`select
    p.[ID],
    p.[FirstName],
    p.[LastName]
from [sqlquery-inator].[dbo].[Person] as p
where p.[FirstName] = @v AND p.[LastName] = @v0`)
    })

    it('Multiple selects results in inner query', async () => {
        const query = from(Person, 'p').selectAll().select(p => { return { ID: p.ID, FirstName: p.FirstName}}, 'p2')

        const result = toQuery(dbschema, query.expr);
        compare(result.sql,
`select
    p2.[ID],
    p2.[FirstName]
from (
    select
        p.[ID],
        p.[FirstName],
        p.[LastName]
    from [sqlquery-inator].[dbo].[Person] as p
) as p2`)
    })

    it('Join with on predicate generates correct query', async () => {
        const query = from(Person, 'p')
                        .join(Address, 'a').on(r => r.p.ID.equals(r.a.PersonID))
                        .innerJoin(Person, 'p2').on(r => r.p2.ID.equals(r.p.ID))
                        .select(r => { 
                            return { ...r.p, ...r.a }
                        })
                        

        const result = toQuery(dbschema, query.expr);

        compare(result.sql,
`select
    a.[ID],
    p.[FirstName],
    p.[LastName],
    a.[PersonID],
    a.[StreetAddress1],
    a.[StreetAddress2]
from [sqlquery-inator].[dbo].[Person] as p
join [sqlquery-inator].[dbo].[Address] as a on p.[ID] = a.[PersonID]
join [sqlquery-inator].[dbo].[Person] as p2 on p2.[ID] = p.[ID]`)
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
                        

        const result = toQuery(dbschema, query.expr);

        compare(result.sql, 
`select
    p.[ID],
    p.[FirstName],
    p.[LastName],
    a.[PersonID],
    a.[StreetAddress1],
    a.[StreetAddress2],
    (a.[ID]) as 'AddressID'
from [sqlquery-inator].[dbo].[Person] as p
join [sqlquery-inator].[dbo].[Address] as a on p.[ID] = a.[PersonID]
join [sqlquery-inator].[dbo].[Person] as p2 on p2.[ID] = p.[ID]`)
    });

    it('Selecting a value results in a parameter being injected', async () => {
        const query = from(Person, 'p').select(p => { return { ID: val(1) }})

        const result = toQuery(dbschema, query.expr);
        
        compare(result.sql,
`select
    (@v) as 'ID'
from [sqlquery-inator].[dbo].[Person] as p`)
    });

    it('left outer join returns nullable columns', async () => {
        const query = from(Person, 'p')
                        .leftOuterJoin(Address, 'a').on(r => r.p.ID.equals(r.a.PersonID))
                        .leftOuterJoin(Person, 'p2').on(r => r.a.PersonID.equals(r.p2.ID))
                        .select(r => { 
                            return { ...r.p, StreetAddress1: r.a.StreetAddress1, SecondFirstName: r.p2.FirstName }
                        });

        const result = toQuery(dbschema, query.expr);

        compare(result.sql, 
`select
    p.[ID],
    p.[FirstName],
    p.[LastName],
    a.[StreetAddress1],
    (p2.[FirstName]) as 'SecondFirstName'
from [sqlquery-inator].[dbo].[Person] as p
left outer join [sqlquery-inator].[dbo].[Address] as a on p.[ID] = a.[PersonID]
left outer join [sqlquery-inator].[dbo].[Person] as p2 on a.[PersonID] = p2.[ID]`)
    });

    it('right outer join returns nullable columns', async () => {
        const query = from(Person, 'p')
                        .rightOuterJoin(Address, 'a').on(r => r.p.ID.equals(r.a.PersonID))
                        .rightOuterJoin(Person, 'p2').on(r => r.a.PersonID.equals(r.p2.ID))
                        .select(r => { 
                            return { ...r.p2, StreetAddress1: r.a.StreetAddress1, FirstName2: r.p.FirstName }
                        });

        const result = toQuery(dbschema, query.expr);

        compare(result.sql, 
`select
    p2.[ID],
    p2.[FirstName],
    p2.[LastName],
    a.[StreetAddress1],
    (p.[FirstName]) as 'FirstName2'
from [sqlquery-inator].[dbo].[Person] as p
right outer join [sqlquery-inator].[dbo].[Address] as a on p.[ID] = a.[PersonID]
right outer join [sqlquery-inator].[dbo].[Person] as p2 on a.[PersonID] = p2.[ID]`)
    });

    it('subquery is allowed in join', async () => {
        const subquery = from(Person, 'p').selectAll().where(p => p.FirstName.equals('Heinz'))

        const query = from(Person, 'p')
                        .leftOuterJoin(Address, 'a').on(r => r.p.ID.equals(r.a.PersonID))
                        .leftOuterJoin(subquery, 'p2').on(r => r.a.PersonID.equals(r.p2.ID))
                        .select(r => { 
                            return { ...r.p, StreetAddress1: r.a.StreetAddress1, SecondFirstName: r.p2.FirstName }
                        });

        const result = toQuery(dbschema, query.expr);

        compare(result.sql, 
`select
    p.[ID],
    p.[FirstName],
    p.[LastName],
    a.[StreetAddress1],
    (p2.[FirstName]) as 'SecondFirstName'
from [sqlquery-inator].[dbo].[Person] as p
left outer join [sqlquery-inator].[dbo].[Address] as a on p.[ID] = a.[PersonID]
left outer join (
    select
        p.[ID],
        p.[FirstName],
        p.[LastName]
    from [sqlquery-inator].[dbo].[Person] as p
    where p.[FirstName] = @v
) as p2 on a.[PersonID] = p2.[ID]`)
    });

    it('Can order by ID', async () => {
        const query = from(Person, 'p').selectAll().orderBy(p => p.ID);

        const result = toQuery(dbschema, query.expr);

        compare(result.sql, 
`select
    p.[ID],
    p.[FirstName],
    p.[LastName]
from [sqlquery-inator].[dbo].[Person] as p
order by p.[ID] ASC`)
    });

    it('Can order by ID then by FirstName desc', async () => {
        const query = from(Person, 'p').selectAll().orderBy(p => p.ID).thenByDesc(p => p.FirstName);

        const result = toQuery(dbschema, query.expr);

        compare(result.sql, 
`select
    p.[ID],
    p.[FirstName],
    p.[LastName]
from [sqlquery-inator].[dbo].[Person] as p
order by p.[ID] ASC, p.[FirstName] DESC`)
    })

    it('Can limit the result set', async () => {
        const query = from(Person, 'p').selectAll().take(5);

        const result = toQuery(dbschema, query.expr);

        compare(result.sql, 
`select top (5)
    p.[ID],
    p.[FirstName],
    p.[LastName]
from [sqlquery-inator].[dbo].[Person] as p`);
    })

    it('subquery within subquery formats correctly', async () => {
        const subquery = from(Person, 'p').selectAll().where(p => p.FirstName.equals('Heinz'))
        const subquery2 = subquery.select(p => { return { ID: p.ID, FirstName: p.FirstName } })

        const query = from(Person, 'p')
                        .leftOuterJoin(Address, 'a').on(r => r.p.ID.equals(r.a.PersonID))
                        .leftOuterJoin(subquery2, 'p2').on(r => r.a.PersonID.equals(r.p2.ID))
                        .select(r => { 
                            return { ...r.p, StreetAddress1: r.a.StreetAddress1, SecondFirstName: r.p2.FirstName }
                        });

        const result = toQuery(dbschema, query.expr);

        compare(result.sql, 
`select
    p.[ID],
    p.[FirstName],
    p.[LastName],
    a.[StreetAddress1],
    (p2.[FirstName]) as 'SecondFirstName'
from [sqlquery-inator].[dbo].[Person] as p
left outer join [sqlquery-inator].[dbo].[Address] as a on p.[ID] = a.[PersonID]
left outer join (
    select
        [ID],
        [FirstName]
    from (
        select
            p.[ID],
            p.[FirstName],
            p.[LastName]
        from [sqlquery-inator].[dbo].[Person] as p
        where p.[FirstName] = @v
    ) as ta1
) as p2 on a.[PersonID] = p2.[ID]`)
    });

    it('Can use ROW_NUMBER with orderby', async () => {
        const query = from(Person, 'p').select(p => { 
            return {
                ...p,
                RowNumber: ROW_NUMBER([p.ID.asc])
            }
        })

        const result = toQuery(dbschema, query.expr);
        compare(result.sql, 
`select
    p.[ID],
    p.[FirstName],
    p.[LastName],
    (ROW_NUMBER() OVER (ORDER BY p.[ID] ASC)) as 'RowNumber'
from [sqlquery-inator].[dbo].[Person] as p`)

    });

    it('Can skip records with ROW_NUMBER', async () => {
        const query = from(Person, 'p').select(p => { 
            return {
                ...p,
                RowNumber: ROW_NUMBER([p.ID.asc])
            }
        }).where(p => p.RowNumber.greaterThan(10))

        const result = toQuery(dbschema, query.expr);

        compare(result.sql, 
`select
    [ID],
    [FirstName],
    [LastName],
    [RowNumber]
from (
    select
        p.[ID],
        p.[FirstName],
        p.[LastName],
        (ROW_NUMBER() OVER (ORDER BY p.[ID] ASC)) as 'RowNumber'
    from [sqlquery-inator].[dbo].[Person] as p
) as ta1
where [RowNumber] > @v`)
    })

    it('Can use from on subquery', async () => {
        const subquery = from(Person, 'p').selectAll();

        const query = from(subquery, 'p2').selectAll();

        const result = toQuery(dbschema, query.expr);

        compare(result.sql,
`select
    p2.[ID],
    p2.[FirstName],
    p2.[LastName]
from (
    select
        p.[ID],
        p.[FirstName],
        p.[LastName]
    from [sqlquery-inator].[dbo].[Person] as p
) as p2`)
    })

    it('Can join from subquery', async () => {
        const subquery = from(Person, 'p').selectAll();

        const query = from(subquery, 'p2')
                        .join(Address, 'a').on(r => r.p2.ID.equals(r.a.PersonID))
                        .select(r => { return { ...r.p2}})

        const result = toQuery(dbschema, query.expr);

        compare(result.sql,
`select
    p2.[ID],
    p2.[FirstName],
    p2.[LastName]
from (
    select
        p.[ID],
        p.[FirstName],
        p.[LastName]
    from [sqlquery-inator].[dbo].[Person] as p
) as p2
join [sqlquery-inator].[dbo].[Address] as a on p2.[ID] = a.[PersonID]
`)
    })

    it('Can join multiple subqueries', async () => {
        const subquery = from(Person, 'p').selectAll();
        const subquery2 = from(Person, 'p2').selectAll();

        const query = from(subquery, 'p2')
                        .join(subquery2, 'p3').on(r => r.p2.ID.equals(r.p3.ID))
                        .select(r => { return { ...r.p2}})

        const result = toQuery(dbschema, query.expr);

        compare(result.sql,
`select
    p2.[ID],
    p2.[FirstName],
    p2.[LastName]
from (
    select
        p.[ID],
        p.[FirstName],
        p.[LastName]
    from [sqlquery-inator].[dbo].[Person] as p
) as p2
join (
    select
        p2.[ID],
        p2.[FirstName],
        p2.[LastName]
    from [sqlquery-inator].[dbo].[Person] as p2
) as p3 on p2.[ID] = p3.[ID]
`)
    })

    it('Can join against query with ROW_NUMBER and have aliases match up', async () => {
        const rownumberQuery = from(Person, 'p').select(p => { 
            return {
                ...p,
                RowNumber: ROW_NUMBER([p.ID.asc])
            }
        }).where(p => p.RowNumber.greaterThan(10))
        

        const query = from(rownumberQuery, 'p2')
                        .join(Address, 'a').on(r => r.p2.ID.equals(r.a.PersonID))
                        .select(r => { return { ...r.p2 } })

        const result = toQuery(dbschema, query.expr);
        
        compare(result.sql, 
`select
    p2.[ID],
    p2.[FirstName],
    p2.[LastName],
    p2.[RowNumber]
from (
    select
        [ID],
        [FirstName],
        [LastName],
        [RowNumber]
    from (
        select
            p.[ID],
            p.[FirstName],
            p.[LastName],
            (ROW_NUMBER() OVER (ORDER BY p.[ID] ASC)) as 'RowNumber'
        from [sqlquery-inator].[dbo].[Person] as p
    ) as ta1
    where [RowNumber] > @v
) as p2
join [sqlquery-inator].[dbo].[Address] as a on p2.[ID] = a.[PersonID]
    `)
    })

    it('Can select distinct', async () => {
        const query = from(Person, 'p')
                        .select(p => { return { FirstName: p.FirstName } })
                        .distinct();

        const result = toQuery(dbschema, query.expr);

        compare(result.sql,
`select distinct
    p.[FirstName]
from [sqlquery-inator].[dbo].[Person] as p
`)
    })

    it('Can select distinct', async () => {
        const query = from(Person, 'p')
                        .select(p => { return { FirstName: p.FirstName } })
                        .distinct()
                        .take(100);

        const result = toQuery(dbschema, query.expr);

        compare(result.sql,
`select distinct top (100)
    p.[FirstName]
from [sqlquery-inator].[dbo].[Person] as p
`)
    })
});

