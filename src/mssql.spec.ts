import { expect, use } from 'chai';
import 'mocha';

import * as t from 'io-ts'
import * as tdc from 'io-ts-derive-class'

import { registerTable, from, ISNULL, val, ROW_NUMBER, PATINDEX, COUNT, AVG, MAX, MIN, SUM, UNION } from './index';
import * as ut from './untyped_ast'

import { toQuery } from './mssql'
import { DBSchema, ColumnSchema, TableSchema } from 'dbschema-inator';
import { toUnsafeQuery, toSafeQuery, Address, Person } from './testschema.spec'

function compare(actual: string, expected: string){
    actual = actual.trim()
    expected = expected.trim()
    return expect(actual).eq(expected);
}

describe('mssql query tests', () => {
    it('Can generate Select * query', async () => {
        const query = from(Person, 'p').selectAll()
        const unsafe = toUnsafeQuery(query.expr);
        const safe = toSafeQuery(query.expr);

        compare(unsafe.sql, 
`select
    p.[ID],
    p.[FirstName],
    p.[LastName]
from [dbo].[Person] as p`)

        compare(safe.sql, 
`select
    ta2.[ID],
    ta2.[FirstName],
    ta2.[LastName]
from [dbo].[Person] as ta2`)
    })

    it('Can select individual columns from table', async () => {
        const query = from(Person, 'p').select(p => { return { ID: p.ID }});
        const unsafe = toUnsafeQuery(query.expr);
        const safe = toSafeQuery(query.expr);

        compare(unsafe.sql, 
`select
    p.[ID]
from [dbo].[Person] as p`)

        compare(safe.sql, 
`select
    ta2.[ID]
from [dbo].[Person] as ta2`)
    })

    it('Can call select with scalar function', async () => {
        const isNullExpr = ISNULL(val(null), val(''));
        const query = from(Person, 'p').select(p => { return { test: isNullExpr } });
        const unsafe = toUnsafeQuery(query.expr);
        const safe = toSafeQuery(query.expr);

        compare(unsafe.sql, 
`select
    (ISNULL(null, '')) as 'test'
from [dbo].[Person] as p`)

        compare(safe.sql, 
`select
    (ISNULL(null, '')) as 'ca1'
from [dbo].[Person] as ta2`)
    })

    it('Can select columns and additional calculated fields', async () => {
        const isNullExpr = ISNULL(val(null), val(''));
        const query = from(Person, 'p').select(p => { return { ID: p.ID, FirstName: p.FirstName, blah: isNullExpr }})
        const unsafe = toUnsafeQuery(query.expr);
        const safe = toSafeQuery(query.expr);

        compare(unsafe.sql, 
`select
    p.[ID],
    p.[FirstName],
    (ISNULL(null, '')) as 'blah'
from [dbo].[Person] as p`)

        compare(safe.sql, 
`select
    ta2.[ID],
    ta2.[FirstName],
    (ISNULL(null, '')) as 'ca1'
from [dbo].[Person] as ta2`)
    })

    it('Can select all columns using spread syntax', async () => {
        const isNullExpr = ISNULL(val(null), val(''));
        const query = from(Person, 'p').select(p => { return { ...p, blah: isNullExpr }})
        const unsafe = toUnsafeQuery(query.expr);
        const safe = toSafeQuery(query.expr);

        compare(unsafe.sql, 
`select
    p.[ID],
    p.[FirstName],
    p.[LastName],
    (ISNULL(null, '')) as 'blah'
from [dbo].[Person] as p`)

        compare(safe.sql, 
`select
    ta2.[ID],
    ta2.[FirstName],
    ta2.[LastName],
    (ISNULL(null, '')) as 'ca1'
from [dbo].[Person] as ta2`)
    })

    it('Can filter using where clause', async () => {
        const query = from(Person, 'p').selectAll().where(p => p.FirstName.equals('Heinz').and(p.LastName.equals('Doofenschmirtz')))
        const unsafe = toUnsafeQuery(query.expr);
        const safe = toSafeQuery(query.expr);

        compare(unsafe.sql, 
`select
    p.[ID],
    p.[FirstName],
    p.[LastName]
from [dbo].[Person] as p
where (p.[FirstName] = @v AND p.[LastName] = @v1)`)

        compare(safe.sql, 
`select
    ta2.[ID],
    ta2.[FirstName],
    ta2.[LastName]
from [dbo].[Person] as ta2
where (ta2.[FirstName] = @v AND ta2.[LastName] = @v1)`)
    })

    it('Multiple where clauses result in AND predicates', async () => {
        const query = from(Person, 'p').selectAll().where(p => p.FirstName.equals('Heinz')).where(p => p.LastName.equals('Doofenschmirtz'))
        const unsafe = toUnsafeQuery(query.expr);
        const safe = toSafeQuery(query.expr);

        compare(unsafe.sql, 
`select
    p.[ID],
    p.[FirstName],
    p.[LastName]
from [dbo].[Person] as p
where (p.[FirstName] = @v AND p.[LastName] = @v1)`)

        compare(safe.sql, 
`select
    ta2.[ID],
    ta2.[FirstName],
    ta2.[LastName]
from [dbo].[Person] as ta2
where (ta2.[FirstName] = @v AND ta2.[LastName] = @v1)`)
    })

    it('Multiple selects results in inner query', async () => {
        const query = from(Person, 'p').selectAll().select(p => { return { ID: p.ID, FirstName: p.FirstName}}, 'p2')
        const unsafe = toUnsafeQuery(query.expr);
        const safe = toSafeQuery(query.expr);

        compare(unsafe.sql,
`select
    p2.[ID],
    p2.[FirstName]
from (
    select
        p.[ID],
        p.[FirstName],
        p.[LastName]
    from [dbo].[Person] as p
) as p2`)

        compare(safe.sql,
`select
    ta1.[ID],
    ta1.[FirstName]
from (
    select
        ta2.[ID],
        ta2.[FirstName],
        ta2.[LastName]
    from [dbo].[Person] as ta2
) as ta1`)
    })

    it('Join with on predicate generates correct query', async () => {
        const query = from(Person, 'p')
                        .join(Address, 'a').on(r => r.p.ID.equals(r.a.PersonID))
                        .innerJoin(Person, 'p2').on(r => r.p2.ID.equals(r.p.ID))
                        .select(r => { 
                            return { ...r.p, ...r.a }
                        })
                        

        const unsafe = toUnsafeQuery(query.expr);
        const safe = toSafeQuery(query.expr);

        compare(unsafe.sql,
`select
    a.[ID],
    p.[FirstName],
    p.[LastName],
    a.[PersonID],
    a.[StreetAddress1],
    a.[StreetAddress2]
from [dbo].[Person] as p
join [dbo].[Address] as a on p.[ID] = a.[PersonID]
join [dbo].[Person] as p2 on p2.[ID] = p.[ID]`)

        compare(safe.sql,
`select
    ta3.[ID],
    ta2.[FirstName],
    ta2.[LastName],
    ta3.[PersonID],
    ta3.[StreetAddress1],
    ta3.[StreetAddress2]
from [dbo].[Person] as ta2
join [dbo].[Address] as ta3 on ta2.[ID] = ta3.[PersonID]
join [dbo].[Person] as ta4 on ta4.[ID] = ta2.[ID]`)
    });

    it('Join with where clause', async () => {
        const query = from(Person, 'p')
                        .join(Person, 'p2').on(r => r.p.ID.equals(r.p2.ID))
                        .select(r => ({ ...r.p}))
                        .where(r => r.FirstName.equals('Heinz'))

        const unsafe = toUnsafeQuery(query.expr);
        const safe = toSafeQuery(query.expr);
        
        compare(unsafe.sql,
`select
    p.[ID],
    p.[FirstName],
    p.[LastName]
from [dbo].[Person] as p
join [dbo].[Person] as p2 on p.[ID] = p2.[ID]
where p.[FirstName] = @v`)

        compare(safe.sql,
`select
    ta2.[ID],
    ta2.[FirstName],
    ta2.[LastName]
from [dbo].[Person] as ta2
join [dbo].[Person] as ta3 on ta2.[ID] = ta3.[ID]
where ta2.[FirstName] = @v`)
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
                        

        const unsafe = toUnsafeQuery(query.expr);
        const safe = toSafeQuery(query.expr);

        compare(unsafe.sql, 
`select
    p.[ID],
    p.[FirstName],
    p.[LastName],
    a.[PersonID],
    a.[StreetAddress1],
    a.[StreetAddress2],
    (a.[ID]) as 'AddressID'
from [dbo].[Person] as p
join [dbo].[Address] as a on p.[ID] = a.[PersonID]
join [dbo].[Person] as p2 on p2.[ID] = p.[ID]`)

        compare(safe.sql, 
`select
    ta2.[ID],
    ta2.[FirstName],
    ta2.[LastName],
    ta3.[PersonID],
    ta3.[StreetAddress1],
    ta3.[StreetAddress2],
    (ta3.[ID]) as 'ca1'
from [dbo].[Person] as ta2
join [dbo].[Address] as ta3 on ta2.[ID] = ta3.[PersonID]
join [dbo].[Person] as ta4 on ta4.[ID] = ta2.[ID]
`)
    });

    it('Selecting a value results in a parameter being injected', async () => {
        const query = from(Person, 'p').select(p => { return { ID: val(1) }})

        const unsafe = toUnsafeQuery(query.expr);
        const safe = toSafeQuery(query.expr);
        
        compare(unsafe.sql,
`select
    (@v) as 'ID'
from [dbo].[Person] as p`)

        compare(safe.sql,
`select
    (@v) as 'ca1'
from [dbo].[Person] as ta2`)
    });

    it('left outer join returns nullable columns', async () => {
        const query = from(Person, 'p')
                        .leftOuterJoin(Address, 'a').on(r => r.p.ID.equals(r.a.PersonID))
                        .leftOuterJoin(Person, 'p2').on(r => r.a.PersonID.equals(r.p2.ID))
                        .select(r => { 
                            return { ...r.p, StreetAddress1: r.a.StreetAddress1, SecondFirstName: r.p2.FirstName }
                        });

        const unsafe = toUnsafeQuery(query.expr);
        const safe = toSafeQuery(query.expr);

        compare(unsafe.sql, 
`select
    p.[ID],
    p.[FirstName],
    p.[LastName],
    a.[StreetAddress1],
    (p2.[FirstName]) as 'SecondFirstName'
from [dbo].[Person] as p
left outer join [dbo].[Address] as a on p.[ID] = a.[PersonID]
left outer join [dbo].[Person] as p2 on a.[PersonID] = p2.[ID]`)

        compare(safe.sql, 
`select
    ta2.[ID],
    ta2.[FirstName],
    ta2.[LastName],
    ta3.[StreetAddress1],
    (ta4.[FirstName]) as 'ca1'
from [dbo].[Person] as ta2
left outer join [dbo].[Address] as ta3 on ta2.[ID] = ta3.[PersonID]
left outer join [dbo].[Person] as ta4 on ta3.[PersonID] = ta4.[ID]`)
    });

    it('right outer join returns nullable columns', async () => {
        const query = from(Person, 'p')
                        .rightOuterJoin(Address, 'a').on(r => r.p.ID.equals(r.a.PersonID))
                        .rightOuterJoin(Person, 'p2').on(r => r.a.PersonID.equals(r.p2.ID))
                        .select(r => { 
                            return { ...r.p2, StreetAddress1: r.a.StreetAddress1, FirstName2: r.p.FirstName }
                        });

        const unsafe = toUnsafeQuery(query.expr);
        const safe = toSafeQuery(query.expr);

        compare(unsafe.sql, 
`select
    p2.[ID],
    p2.[FirstName],
    p2.[LastName],
    a.[StreetAddress1],
    (p.[FirstName]) as 'FirstName2'
from [dbo].[Person] as p
right outer join [dbo].[Address] as a on p.[ID] = a.[PersonID]
right outer join [dbo].[Person] as p2 on a.[PersonID] = p2.[ID]`)

        compare(safe.sql, 
`select
    ta4.[ID],
    ta4.[FirstName],
    ta4.[LastName],
    ta3.[StreetAddress1],
    (ta2.[FirstName]) as 'ca1'
from [dbo].[Person] as ta2
right outer join [dbo].[Address] as ta3 on ta2.[ID] = ta3.[PersonID]
right outer join [dbo].[Person] as ta4 on ta3.[PersonID] = ta4.[ID]`)
    });

    it('subquery is allowed in join', async () => {
        const subquery = from(Person, 'p').selectAll().where(p => p.FirstName.equals('Heinz'))

        const query = from(Person, 'p')
                        .leftOuterJoin(Address, 'a').on(r => r.p.ID.equals(r.a.PersonID))
                        .leftOuterJoin(subquery, 'p2').on(r => r.a.PersonID.equals(r.p2.ID))
                        .select(r => { 
                            return { ...r.p, StreetAddress1: r.a.StreetAddress1, SecondFirstName: r.p2.FirstName }
                        });

        const unsafe = toUnsafeQuery(query.expr);
        const safe = toSafeQuery(query.expr);

        compare(unsafe.sql, 
`select
    p.[ID],
    p.[FirstName],
    p.[LastName],
    a.[StreetAddress1],
    (p2.[FirstName]) as 'SecondFirstName'
from [dbo].[Person] as p
left outer join [dbo].[Address] as a on p.[ID] = a.[PersonID]
left outer join (
    select
        p.[ID],
        p.[FirstName],
        p.[LastName]
    from [dbo].[Person] as p
    where p.[FirstName] = @v
) as p2 on a.[PersonID] = p2.[ID]`)

        compare(safe.sql, 
`select
    ta2.[ID],
    ta2.[FirstName],
    ta2.[LastName],
    ta3.[StreetAddress1],
    (ta4.[FirstName]) as 'ca1'
from [dbo].[Person] as ta2
left outer join [dbo].[Address] as ta3 on ta2.[ID] = ta3.[PersonID]
left outer join (
    select
        ta2.[ID],
        ta2.[FirstName],
        ta2.[LastName]
    from [dbo].[Person] as ta2
    where ta2.[FirstName] = @v
) as ta4 on ta3.[PersonID] = ta4.[ID]`)
    });

    it('Can order by ID', async () => {
        const query = from(Person, 'p').selectAll().orderBy(p => p.ID);

        const unsafe = toUnsafeQuery(query.expr);
        const safe = toSafeQuery(query.expr);

        compare(unsafe.sql, 
`select
    p.[ID],
    p.[FirstName],
    p.[LastName]
from [dbo].[Person] as p
order by p.[ID] ASC`)

        compare(unsafe.sql, 
`select
    p.[ID],
    p.[FirstName],
    p.[LastName]
from [dbo].[Person] as p
order by p.[ID] ASC`)
    });

    it('Can order by ID then by FirstName desc', async () => {
        const query = from(Person, 'p').selectAll().orderBy(p => p.ID).thenByDesc(p => p.FirstName);

        const unsafe = toUnsafeQuery(query.expr);
        const safe = toSafeQuery(query.expr);

        compare(unsafe.sql, 
`select
    p.[ID],
    p.[FirstName],
    p.[LastName]
from [dbo].[Person] as p
order by p.[ID] ASC, p.[FirstName] DESC`)

        compare(safe.sql, 
`select
    ta2.[ID],
    ta2.[FirstName],
    ta2.[LastName]
from [dbo].[Person] as ta2
order by ta2.[ID] ASC, ta2.[FirstName] DESC`)
    })

    it('Can limit the result set', async () => {
        const query = from(Person, 'p').selectAll().take(5);

        const unsafe = toUnsafeQuery(query.expr);
        const safe = toSafeQuery(query.expr);

        compare(unsafe.sql, 
`select top (5)
    p.[ID],
    p.[FirstName],
    p.[LastName]
from [dbo].[Person] as p`)

        compare(safe.sql, 
`select top (5)
    ta2.[ID],
    ta2.[FirstName],
    ta2.[LastName]
from [dbo].[Person] as ta2`)
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

        const unsafe = toUnsafeQuery(query.expr);
        const safe = toSafeQuery(query.expr);

        compare(unsafe.sql, 
`select
    p.[ID],
    p.[FirstName],
    p.[LastName],
    a.[StreetAddress1],
    (p2.[FirstName]) as 'SecondFirstName'
from [dbo].[Person] as p
left outer join [dbo].[Address] as a on p.[ID] = a.[PersonID]
left outer join (
    select
        [ID],
        [FirstName]
    from (
        select
            p.[ID],
            p.[FirstName],
            p.[LastName]
        from [dbo].[Person] as p
        where p.[FirstName] = @v
    ) as ta1
) as p2 on a.[PersonID] = p2.[ID]`)

        compare(safe.sql, 
`select
    ta2.[ID],
    ta2.[FirstName],
    ta2.[LastName],
    ta3.[StreetAddress1],
    (ta4.[FirstName]) as 'ca1'
from [dbo].[Person] as ta2
left outer join [dbo].[Address] as ta3 on ta2.[ID] = ta3.[PersonID]
left outer join (
    select
        [ID],
        [FirstName]
    from (
        select
            ta2.[ID],
            ta2.[FirstName],
            ta2.[LastName]
        from [dbo].[Person] as ta2
        where ta2.[FirstName] = @v
    ) as ta1
) as ta4 on ta3.[PersonID] = ta4.[ID]`)
    });

    it('Can use ROW_NUMBER with orderby', async () => {
        const query = from(Person, 'p').select(p => { 
            return {
                ...p,
                RowNumber: ROW_NUMBER([p.ID.asc])
            }
        })

        const unsafe = toUnsafeQuery(query.expr);
        const safe = toSafeQuery(query.expr);

        compare(unsafe.sql, 
`select
    p.[ID],
    p.[FirstName],
    p.[LastName],
    (ROW_NUMBER() OVER (ORDER BY p.[ID] ASC)) as 'RowNumber'
from [dbo].[Person] as p`)

        compare(safe.sql, 
`select
    ta2.[ID],
    ta2.[FirstName],
    ta2.[LastName],
    (ROW_NUMBER() OVER (ORDER BY ta2.[ID] ASC)) as 'ca1'
from [dbo].[Person] as ta2`)

    });

    it('Can skip records with ROW_NUMBER', async () => {
        const query = from(Person, 'p').select(p => { 
            return {
                ...p,
                RowNumber: ROW_NUMBER([p.ID.asc])
            }
        }).where(p => p.RowNumber.greaterThan(10))

        const unsafe = toUnsafeQuery(query.expr);
        const safe = toSafeQuery(query.expr);

        compare(unsafe.sql, 
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
    from [dbo].[Person] as p
) as ta1
where [RowNumber] > @v`)

        compare(safe.sql, 
`select
    [ID],
    [FirstName],
    [LastName],
    [ca1]
from (
    select
        ta2.[ID],
        ta2.[FirstName],
        ta2.[LastName],
        (ROW_NUMBER() OVER (ORDER BY ta2.[ID] ASC)) as 'ca1'
    from [dbo].[Person] as ta2
) as ta1
where [ca1] > @v
`)


    })

    it('Can page records with ROW_NUMBER and take', async () => {
        const query = from(Person, 'p').select(p => { 
            return {
                ...p,
                RowNumber: ROW_NUMBER([p.ID.asc])
            }
        }).where(p => p.RowNumber.greaterThan(10).and(p.RowNumber.lessThan(21)))
        .take(10)
        .select(r => {
            const row = { ...r };
            delete row.RowNumber
            return row;
        })

        const unsafe = toUnsafeQuery(query.expr);
        const safe = toSafeQuery(query.expr);

        compare(unsafe.sql, 
`select
    [ID],
    [FirstName],
    [LastName]
from (
    select top (10)
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
        from [dbo].[Person] as p
    ) as ta1
    where ([RowNumber] > @v AND [RowNumber] < @v1)
) as ta1`)

        compare(safe.sql, 
`select
    [ID],
    [FirstName],
    [LastName]
from (
    select top (10)
        [ID],
        [FirstName],
        [LastName],
        [ca1]
    from (
        select
            ta2.[ID],
            ta2.[FirstName],
            ta2.[LastName],
            (ROW_NUMBER() OVER (ORDER BY ta2.[ID] ASC)) as 'ca1'
        from [dbo].[Person] as ta2
    ) as ta1
    where ([ca1] > @v AND [ca1] < @v1)
) as ta1`)
    })

    it('Can use orderby pagination', async () => {
        const query = from(Person, 'p').selectAll().orderByDesc(r => r.ID).page(2, 10);

        const unsafe = toUnsafeQuery(query.expr);
        const safe = toSafeQuery(query.expr);

        expect(unsafe.parameters['v'].value).eq(10);
        expect(unsafe.parameters['v1'].value).eq(20);

        compare(unsafe.sql, 
`select
    [ID],
    [FirstName],
    [LastName]
from (
    select
        (ROW_NUMBER() OVER (ORDER BY p.[ID] DESC)) as '_RowNumber',
        p.[ID],
        p.[FirstName],
        p.[LastName]
    from [dbo].[Person] as p
) as ta1
where ([_RowNumber] > @v AND [_RowNumber] <= @v1)`)

        compare(safe.sql, 
`select
    [ID],
    [FirstName],
    [LastName]
from (
    select
        (ROW_NUMBER() OVER (ORDER BY ta2.[ID] DESC)) as 'ca1',
        ta2.[ID],
        ta2.[FirstName],
        ta2.[LastName]
    from [dbo].[Person] as ta2
) as ta1
where ([ca1] > @v AND [ca1] <= @v1)`)
    })

    it('Can orderby against join', async () => {
        const query = from(Person, 'p')
                        .join(Person, 'p2').on(r => r.p.ID.equals(r.p2.ID))
                        .orderByDesc(r => r.p2.ID)
                        .select(r => ({ ...r.p}))

        const unsafe = toUnsafeQuery(query.expr);
        const safe = toSafeQuery(query.expr);

        compare(unsafe.sql, 
`select
    p.[ID],
    p.[FirstName],
    p.[LastName]
from [dbo].[Person] as p
join [dbo].[Person] as p2 on p.[ID] = p2.[ID]
order by p2.[ID] DESC`)

        compare(safe.sql, 
`select
    ta2.[ID],
    ta2.[FirstName],
    ta2.[LastName]
from [dbo].[Person] as ta2
join [dbo].[Person] as ta3 on ta2.[ID] = ta3.[ID]
order by ta3.[ID] DESC`)
    })

    it('JoinOrderBy as subquery drops orderby', async () => {
        const subquery = from(Person, 'p')
                        .join(Person, 'p2').on(r => r.p.ID.equals(r.p2.ID))
                        .orderByDesc(r => r.p2.ID)
                        .select(r => ({ ...r.p}))

        const query = from(Person, 'p')
                        .join(subquery, 's').on(r => r.p.ID.equals(r.s.ID))
                        .select(r => ({ ...r.s}))

        const unsafe = toUnsafeQuery(query.expr);
        const safe = toSafeQuery(query.expr);

        compare(unsafe.sql, 
`select
    s.[ID],
    s.[FirstName],
    s.[LastName]
from [dbo].[Person] as p
join (
    select
        p.[ID],
        p.[FirstName],
        p.[LastName]
    from [dbo].[Person] as p
    join [dbo].[Person] as p2 on p.[ID] = p2.[ID]
) as s on p.[ID] = s.[ID]`)

        compare(safe.sql, 
`select
    ta3.[ID],
    ta3.[FirstName],
    ta3.[LastName]
from [dbo].[Person] as ta2
join (
    select
        ta2.[ID],
        ta2.[FirstName],
        ta2.[LastName]
    from [dbo].[Person] as ta2
    join [dbo].[Person] as ta3 on ta2.[ID] = ta3.[ID]
) as ta3 on ta2.[ID] = ta3.[ID]`)
    })

    it('Can use from on subquery', async () => {
        const subquery = from(Person, 'p').selectAll();

        const query = from(subquery, 'p2').selectAll();

        const unsafe = toUnsafeQuery(query.expr);
        const safe = toSafeQuery(query.expr);

        compare(unsafe.sql,
`select
    p2.[ID],
    p2.[FirstName],
    p2.[LastName]
from (
    select
        p.[ID],
        p.[FirstName],
        p.[LastName]
    from [dbo].[Person] as p
) as p2`)

        compare(safe.sql,
`select
    ta2.[ID],
    ta2.[FirstName],
    ta2.[LastName]
from (
    select
        ta2.[ID],
        ta2.[FirstName],
        ta2.[LastName]
    from [dbo].[Person] as ta2
) as ta2`)
    })

    it('Can join from subquery', async () => {
        const subquery = from(Person, 'p').selectAll();

        const query = from(subquery, 'p2')
                        .join(Address, 'a').on(r => r.p2.ID.equals(r.a.PersonID))
                        .select(r => { return { ...r.p2}})

        const unsafe = toUnsafeQuery(query.expr);
        const safe = toSafeQuery(query.expr);

        compare(unsafe.sql,
`select
    p2.[ID],
    p2.[FirstName],
    p2.[LastName]
from (
    select
        p.[ID],
        p.[FirstName],
        p.[LastName]
    from [dbo].[Person] as p
) as p2
join [dbo].[Address] as a on p2.[ID] = a.[PersonID]`)

        compare(safe.sql,
`select
    ta2.[ID],
    ta2.[FirstName],
    ta2.[LastName]
from (
    select
        ta2.[ID],
        ta2.[FirstName],
        ta2.[LastName]
    from [dbo].[Person] as ta2
) as ta2
join [dbo].[Address] as ta3 on ta2.[ID] = ta3.[PersonID]`)
    })

    it('Can join multiple subqueries', async () => {
        const subquery = from(Person, 'p').selectAll();
        const subquery2 = from(Person, 'p2').selectAll();

        const query = from(subquery, 'p2')
                        .join(subquery2, 'p3').on(r => r.p2.ID.equals(r.p3.ID))
                        .select(r => { return { ...r.p2}})

        const unsafe = toUnsafeQuery(query.expr);
        const safe = toSafeQuery(query.expr);

        compare(unsafe.sql,
`select
    p2.[ID],
    p2.[FirstName],
    p2.[LastName]
from (
    select
        p.[ID],
        p.[FirstName],
        p.[LastName]
    from [dbo].[Person] as p
) as p2
join (
    select
        p2.[ID],
        p2.[FirstName],
        p2.[LastName]
    from [dbo].[Person] as p2
) as p3 on p2.[ID] = p3.[ID]`)
        
        compare(safe.sql,
`select
    ta2.[ID],
    ta2.[FirstName],
    ta2.[LastName]
from (
    select
        ta2.[ID],
        ta2.[FirstName],
        ta2.[LastName]
    from [dbo].[Person] as ta2
) as ta2
join (
    select
        ta2.[ID],
        ta2.[FirstName],
        ta2.[LastName]
    from [dbo].[Person] as ta2
) as ta3 on ta2.[ID] = ta3.[ID]`)
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

        const unsafe = toUnsafeQuery(query.expr);
        const safe = toSafeQuery(query.expr);
        
        compare(unsafe.sql, 
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
        from [dbo].[Person] as p
    ) as ta1
    where [RowNumber] > @v
) as p2
join [dbo].[Address] as a on p2.[ID] = a.[PersonID]`)

        compare(safe.sql, 
`select
    ta2.[ID],
    ta2.[FirstName],
    ta2.[LastName],
    ta2.[ca1]
from (
    select
        [ID],
        [FirstName],
        [LastName],
        [ca1]
    from (
        select
            ta2.[ID],
            ta2.[FirstName],
            ta2.[LastName],
            (ROW_NUMBER() OVER (ORDER BY ta2.[ID] ASC)) as 'ca1'
        from [dbo].[Person] as ta2
    ) as ta1
    where [ca1] > @v
) as ta2
join [dbo].[Address] as ta3 on ta2.[ID] = ta3.[PersonID]`)
    })

    it('Can select distinct', async () => {
        const query = from(Person, 'p')
                        .select(p => { return { FirstName: p.FirstName } })
                        .distinct();

        const unsafe = toUnsafeQuery(query.expr);
        const safe = toSafeQuery(query.expr);

        compare(unsafe.sql,
`select distinct
    p.[FirstName]
from [dbo].[Person] as p`)

        compare(safe.sql,
`select distinct
    ta2.[FirstName]
from [dbo].[Person] as ta2`)
    })

    it('Can select distinct with take', async () => {
        const query = from(Person, 'p')
                        .select(p => { return { FirstName: p.FirstName } })
                        .distinct()
                        .take(100);

        const unsafe = toUnsafeQuery(query.expr);
        const safe = toSafeQuery(query.expr);

        compare(unsafe.sql,
`select distinct top (100)
    p.[FirstName]
from [dbo].[Person] as p`)

        compare(safe.sql,
`select distinct top (100)
    ta2.[FirstName]
from [dbo].[Person] as ta2`)
    })

    it('Can GroupBy FirstName and count', async () => {
        const query = from(Person, 'p')
                        .selectAll()
                        .groupBy(r => { return { FirstName: r.FirstName } })
                        .select(r => { return { FirstName: r.FirstName, count: COUNT() }})
                        

        const unsafe = toUnsafeQuery(query.expr);
        const safe = toSafeQuery(query.expr);
        
        compare(unsafe.sql,
`select
    p.[FirstName],
    (COUNT(*)) as 'count'
from [dbo].[Person] as p
group by p.[FirstName]`)

        compare(safe.sql,
`select
    ta2.[FirstName],
    (COUNT(*)) as 'ca1'
from [dbo].[Person] as ta2
group by ta2.[FirstName]`)
    })

    it('Can GroupBy on join', async () => {
        const query = from(Person, 'p')
                        .join(Address, 'a').on(r => r.p.ID.equals(r.a.PersonID))
                        .groupBy(r => { return { FirstName: r.p.FirstName } })
                        .select(r => { return { FirstName: r.FirstName, count: COUNT() }})
                        

        const unsafe = toUnsafeQuery(query.expr);
        const safe = toSafeQuery(query.expr);

        compare(unsafe.sql,
`select
    p.[FirstName],
    (COUNT(*)) as 'count'
from [dbo].[Person] as p
join [dbo].[Address] as a on p.[ID] = a.[PersonID]
group by p.[FirstName]`)

        compare(safe.sql,
`select
    ta2.[FirstName],
    (COUNT(*)) as 'ca1'
from [dbo].[Person] as ta2
join [dbo].[Address] as ta3 on ta2.[ID] = ta3.[PersonID]
group by ta2.[FirstName]`)
    })

    it('Can select count(*)', async () => {
        const query = from(Person, 'p')
                        .selectAll()
                        .where(p => p.FirstName.equals('Heinz'))
                        .count()
                        

        const unsafe = toUnsafeQuery(query.expr);
        const safe = toSafeQuery(query.expr);
        
        compare(unsafe.sql,
`select
    (COUNT(*)) as 'count'
from (
    select
        p.[ID],
        p.[FirstName],
        p.[LastName]
    from [dbo].[Person] as p
    where p.[FirstName] = @v
) as ta1`)

        compare(safe.sql,
`select
    (COUNT(*)) as 'ca1'
from (
    select
        ta2.[ID],
        ta2.[FirstName],
        ta2.[LastName]
    from [dbo].[Person] as ta2
    where ta2.[FirstName] = @v
) as ta1`)
    })

    it('Can select distinct column count', async () => {
        const query = from(Person, 'p')
                        .selectAll()
                        .count(p => p.FirstName)
                        

        const unsafe = toUnsafeQuery(query.expr);
        const safe = toSafeQuery(query.expr);

        compare(unsafe.sql,
`select
    (COUNT(DISTINCT [FirstName])) as 'count'
from (
    select
        p.[ID],
        p.[FirstName],
        p.[LastName]
    from [dbo].[Person] as p
) as ta1`)

        compare(safe.sql,
`select
    (COUNT(DISTINCT [FirstName])) as 'ca1'
from (
    select
        ta2.[ID],
        ta2.[FirstName],
        ta2.[LastName]
    from [dbo].[Person] as ta2
) as ta1`)
    })

    it('Can use AVG', async () => {
        const query = from(Person, 'p')
                        .select(p => { return { avg: AVG(p.ID) }});
                        

        const unsafe = toUnsafeQuery(query.expr);
        const safe = toSafeQuery(query.expr);

        compare(unsafe.sql,
`select
    (AVG(p.[ID])) as 'avg'
from [dbo].[Person] as p`)

    compare(safe.sql,
`select
    (AVG(ta2.[ID])) as 'ca1'
from [dbo].[Person] as ta2`)
    })

    it('Can use SUM', async () => {
        const query = from(Person, 'p')
                        .select(p => { return { sum: SUM(p.ID) }});
                        

        const unsafe = toUnsafeQuery(query.expr);
        const safe = toSafeQuery(query.expr);

        compare(unsafe.sql,
`select
    (SUM(p.[ID])) as 'sum'
from [dbo].[Person] as p`)

        compare(safe.sql,
`select
    (SUM(ta2.[ID])) as 'ca1'
from [dbo].[Person] as ta2`)
    })

    it('Can use MAX', async () => {
        const query = from(Person, 'p')
                        .select(p => ({ max: MAX(p.ID) }));
                        

        const unsafe = toUnsafeQuery(query.expr);
        const safe = toSafeQuery(query.expr);

        compare(unsafe.sql,
`select
    (MAX(p.[ID])) as 'max'
from [dbo].[Person] as p`)

        compare(safe.sql,
`select
    (MAX(ta2.[ID])) as 'ca1'
from [dbo].[Person] as ta2`)
    })

    it('Can use MIN', async () => {
        const query = from(Person, 'p')
                        .select(p => ({ min: MIN(p.ID) }));
                        

        const unsafe = toUnsafeQuery(query.expr);
        const safe = toSafeQuery(query.expr);

        compare(unsafe.sql,
`select
    (MIN(p.[ID])) as 'min'
from [dbo].[Person] as p`)

        compare(safe.sql,
`select
    (MIN(ta2.[ID])) as 'ca1'
from [dbo].[Person] as ta2`)
    })

    it('Can use where clause on join', async () => {
        const query = from(Person, 'p')
                        .join(Person, 'p2').on(r => r.p.ID.equals(r.p2.ID))
                        .where(r => r.p.FirstName.equals('Heinz'))
                        .select(r => ({ ...r.p}))

        const unsafe = toUnsafeQuery(query.expr);
        const safe = toSafeQuery(query.expr);
        
        compare(unsafe.sql,
`select
    p.[ID],
    p.[FirstName],
    p.[LastName]
from [dbo].[Person] as p
join [dbo].[Person] as p2 on p.[ID] = p2.[ID]
where p.[FirstName] = @v`)

        compare(safe.sql,
`select
    ta2.[ID],
    ta2.[FirstName],
    ta2.[LastName]
from [dbo].[Person] as ta2
join [dbo].[Person] as ta3 on ta2.[ID] = ta3.[ID]
where ta2.[FirstName] = @v`)
    })

    it('Can use where clause in between joins', async () => {
        const query = from(Person, 'p')
                        .join(Person, 'p2').on(r => r.p.ID.equals(r.p2.ID))
                        .where(r => r.p.FirstName.equals('Heinz'))
                        .join(Person, 'p3').on(r => r.p2.ID.equals(r.p.ID))
                        .where(r => r.p3.LastName.equals('Doofenschmirtz'))
                        .select(r => ({ ...r.p}))

        const unsafe = toUnsafeQuery(query.expr);
        const safe = toSafeQuery(query.expr);
        
        compare(unsafe.sql,
`select
    p.[ID],
    p.[FirstName],
    p.[LastName]
from [dbo].[Person] as p
join [dbo].[Person] as p2 on p.[ID] = p2.[ID]
join [dbo].[Person] as p3 on p2.[ID] = p.[ID]
where (p.[FirstName] = @v AND p3.[LastName] = @v1)`)

        compare(safe.sql,
`select
    ta2.[ID],
    ta2.[FirstName],
    ta2.[LastName]
from [dbo].[Person] as ta2
join [dbo].[Person] as ta3 on ta2.[ID] = ta3.[ID]
join [dbo].[Person] as ta4 on ta3.[ID] = ta2.[ID]
where (ta2.[FirstName] = @v AND ta4.[LastName] = @v1)`)
    })

    it('Can write complex query and get expected results', async () => {
        const subquery = from(Person, 'p')
                        .selectAll()
                        .where(p => p.FirstName.equals('Heinz'))

        const subquery2 = from(Address, 'a')
                            .selectAll()
                            .where(a => a.StreetAddress1.equals('Evil Incorporated'))
        
        const query = from(Person, 'p')
                        .join(subquery, 'p2').on(r => r.p.ID.equals(r.p2.ID))
                        .join(subquery2, 'a2').on(r => r.a2.PersonID.equals(r.p.ID))
                        .join(Address, 'a').on(r => r.a2.ID.equals(r.a.ID))
                        .where(r => r.p.FirstName.equals('Heinz'))
                        .select(r => ({ ...r.p2}))
                        
                        

        const unsafe = toUnsafeQuery(query.expr);
        const safe = toSafeQuery(query.expr);

        compare(unsafe.sql,
`select
    p2.[ID],
    p2.[FirstName],
    p2.[LastName]
from [dbo].[Person] as p
join (
    select
        p.[ID],
        p.[FirstName],
        p.[LastName]
    from [dbo].[Person] as p
    where p.[FirstName] = @v
) as p2 on p.[ID] = p2.[ID]
join (
    select
        a.[ID],
        a.[PersonID],
        a.[StreetAddress1],
        a.[StreetAddress2]
    from [dbo].[Address] as a
    where a.[StreetAddress1] = @v1
) as a2 on a2.[PersonID] = p.[ID]
join [dbo].[Address] as a on a2.[ID] = a.[ID]
where p.[FirstName] = @v2`)

        compare(safe.sql,
`select
    ta3.[ID],
    ta3.[ca1],
    ta3.[ca2]
from [dbo].[Person] as ta2
join (
    select
        ta2.[ID],
        ta2.[FirstName],
        ta2.[LastName]
    from [dbo].[Person] as ta2
    where ta2.[FirstName] = @v
) as ta3 on ta2.[ID] = ta3.[ID]
join (
    select
        ta2.[ID],
        ta2.[PersonID],
        ta2.[StreetAddress1],
        ta2.[StreetAddress2]
    from [dbo].[Address] as ta2
    where ta2.[StreetAddress1] = @v1
) as ta4 on ta4.[PersonID] = ta2.[ID]
join [dbo].[Address] as ta5 on ta4.[ID] = ta5.[ID]
where ta2.[FirstName] = @v2`)
    })

    it('Can use OR in where clause', async () => {
        const query = from(Person, 'p').selectAll().where(p => p.FirstName.equals('Heinz').or(p.LastName.equals('Doofenschmirtz')))

        const unsafe = toUnsafeQuery(query.expr);
        const safe = toSafeQuery(query.expr);

        compare(unsafe.sql, 
`select
    p.[ID],
    p.[FirstName],
    p.[LastName]
from [dbo].[Person] as p
where (p.[FirstName] = @v OR p.[LastName] = @v1)`)

        compare(safe.sql, 
`select
    ta2.[ID],
    ta2.[FirstName],
    ta2.[LastName]
from [dbo].[Person] as ta2
where (ta2.[FirstName] = @v OR ta2.[LastName] = @v1)`)
    });

    it('Can use multiple AND | OR in where clause', async () => {
        const query = from(Person, 'p').selectAll().where(p => 
            p.FirstName.equals('Heinz')
            .and(
                p.LastName.equals('Doofenschmirtz')
            )
            .or(
                p.LastName.equals('Incorporated')
                .and(p.FirstName.equals('Evil'))
            )
            .or(p.ID.equals(1))
        )

        const unsafe = toUnsafeQuery(query.expr);
        const safe = toSafeQuery(query.expr);

        compare(unsafe.sql, 
`select
    p.[ID],
    p.[FirstName],
    p.[LastName]
from [dbo].[Person] as p
where (((p.[FirstName] = @v AND p.[LastName] = @v1) OR (p.[LastName] = @v2 AND p.[FirstName] = @v3)) OR p.[ID] = @v4)`)

        compare(safe.sql, 
`select
    ta2.[ID],
    ta2.[FirstName],
    ta2.[LastName]
from [dbo].[Person] as ta2
where (((ta2.[FirstName] = @v AND ta2.[LastName] = @v1) OR (ta2.[LastName] = @v2 AND ta2.[FirstName] = @v3)) OR ta2.[ID] = @v4)`)
    });

    it('Can use like predicate', async () => {
        const query = from(Person, 'p').selectAll().where(p => 
            p.FirstName.like('%Heinz%')
        )

        const unsafe = toUnsafeQuery(query.expr);
        const safe = toSafeQuery(query.expr);

        compare(unsafe.sql, 
`select
    p.[ID],
    p.[FirstName],
    p.[LastName]
from [dbo].[Person] as p
where p.[FirstName] like @v`)

        compare(safe.sql, 
`select
    ta2.[ID],
    ta2.[FirstName],
    ta2.[LastName]
from [dbo].[Person] as ta2
where ta2.[FirstName] like @v`)
    });

    it('Can use PATINDEX with ROW_NUMBER', async () => {
        const query = from(Person, 'p').select(p => { 
            return {
                ...p,
                RowNumber: ROW_NUMBER([PATINDEX('%Test%', p.FirstName).desc])
            }
        })

        const unsafe = toUnsafeQuery(query.expr);
        const safe = toSafeQuery(query.expr);

        compare(unsafe.sql, 
`select
    p.[ID],
    p.[FirstName],
    p.[LastName],
    (ROW_NUMBER() OVER (ORDER BY PATINDEX(@v, p.[FirstName]) DESC)) as 'RowNumber'
from [dbo].[Person] as p`)

        compare(safe.sql, 
`select
    ta2.[ID],
    ta2.[FirstName],
    ta2.[LastName],
    (ROW_NUMBER() OVER (ORDER BY PATINDEX(@v, ta2.[FirstName]) DESC)) as 'ca1'
from [dbo].[Person] as ta2`)
    });

    it('Can use column addition', async () => {
        const query = from(Person, 'p').select(p => { 
            return {
                ...p,
                FirstAndLast: p.FirstName.add(' ').add(p.LastName)
            }
        })

        const unsafe = toUnsafeQuery(query.expr);
        const safe = toSafeQuery(query.expr);

        compare(unsafe.sql, 
`select
    p.[ID],
    p.[FirstName],
    p.[LastName],
    (p.[FirstName] + @v + p.[LastName]) as 'FirstAndLast'
from [dbo].[Person] as p`)

        compare(safe.sql, 
`select
    ta2.[ID],
    ta2.[FirstName],
    ta2.[LastName],
    (ta2.[FirstName] + @v + ta2.[LastName]) as 'ca1'
from [dbo].[Person] as ta2`)
    });

    it('Can union multiple queries', async () => {
        const query1 = from(Person, 'p').where(p => p.FirstName.equals('Heinz')).select(p => { 
            return {
                ID: p.ID
            }
        })

        const query2 = from(Person, 'p').where(p => p.LastName.equals('Doofenschmirtz')).select(p => { 
            return {
                ID: p.ID
            }
        })

        const query3 = from(Person, 'p').where(p => p.ID.equals(1)).select(p => { 
            return {
                ID: p.ID
            }
        })

        const query = UNION(query1, query2).union(query3).select()

        const unsafe = toUnsafeQuery(query.expr);
        const safe = toSafeQuery(query.expr);

        compare(unsafe.sql, 
`select
    [ID]
from (
        select
            [ID]
        from (
            select
                p.[ID],
                p.[FirstName],
                p.[LastName]
            from [dbo].[Person] as p
            where p.[FirstName] = @v
        ) as ta1
    union
        select
            [ID]
        from (
            select
                p.[ID],
                p.[FirstName],
                p.[LastName]
            from [dbo].[Person] as p
            where p.[LastName] = @v1
        ) as ta1
    union
        select
            [ID]
        from (
            select
                p.[ID],
                p.[FirstName],
                p.[LastName]
            from [dbo].[Person] as p
            where p.[ID] = @v2
        ) as ta1
) as ta1`)

        compare(safe.sql, 
`select
    [ID]
from (
        select
            [ID]
        from (
            select
                ta2.[ID],
                ta2.[FirstName],
                ta2.[LastName]
            from [dbo].[Person] as ta2
            where ta2.[FirstName] = @v
        ) as ta1
    union
        select
            [ID]
        from (
            select
                ta2.[ID],
                ta2.[FirstName],
                ta2.[LastName]
            from [dbo].[Person] as ta2
            where ta2.[LastName] = @v1
        ) as ta1
    union
        select
            [ID]
        from (
            select
                ta2.[ID],
                ta2.[FirstName],
                ta2.[LastName]
            from [dbo].[Person] as ta2
            where ta2.[ID] = @v2
        ) as ta1
) as ta1`)
    });

    it('Can use IN clause against array', async () => {
        const query = from(Person, 'p').where(p => p.ID.in([1, 2, 3, 4]))

        const unsafe = toUnsafeQuery(query.expr);
        const safe = toSafeQuery(query.expr);

        compare(unsafe.sql, 
`select
    p.[ID],
    p.[FirstName],
    p.[LastName]
from [dbo].[Person] as p
where p.[ID] in (
    (
        @v,
        @v1,
        @v2,
        @v3
    )
)`)

        compare(safe.sql, 
`select
    ta2.[ID],
    ta2.[FirstName],
    ta2.[LastName]
from [dbo].[Person] as ta2
where ta2.[ID] in (
    (
        @v,
        @v1,
        @v2,
        @v3
    )
)
`)
    });

    it('Can use IN clause against subquery', async () => {
        const subquery = from(Person, 'p').where(p => p.FirstName.equals('Heinz')).select(p => ({ ID: p.ID }))
        const query = from(Person, 'p').where(p => p.ID.in(subquery))

        const unsafe = toUnsafeQuery(query.expr);
        const safe = toSafeQuery(query.expr);

        compare(unsafe.sql, 
`select
    p.[ID],
    p.[FirstName],
    p.[LastName]
from [dbo].[Person] as p
where p.[ID] in (
    select
        [ID]
    from (
        select
            p.[ID],
            p.[FirstName],
            p.[LastName]
        from [dbo].[Person] as p
        where p.[FirstName] = @v
    ) as ta1
)`)

        compare(safe.sql, 
`select
    ta2.[ID],
    ta2.[FirstName],
    ta2.[LastName]
from [dbo].[Person] as ta2
where ta2.[ID] in (
    select
        [ID]
    from (
        select
            ta2.[ID],
            ta2.[FirstName],
            ta2.[LastName]
        from [dbo].[Person] as ta2
        where ta2.[FirstName] = @v
    ) as ta1
)`)
    });

    it('Can use generic search query', async () => {
        const query = from(Person, 'p').selectAll().search('Heinz Doofenschmirtz', r => r, r => ({ ID: r.ID }), 1, 20)

        const unsafe = toUnsafeQuery(query.expr);
        const safe = toSafeQuery(query.expr);

        compare(unsafe.sql, 
`select
    [ID],
    [FirstName],
    [LastName],
    [_RowNumber]
from (
    select
        (ROW_NUMBER() OVER (ORDER BY fq.[_RowNumber] ASC)) as '_RowNumber',
        fq.[ID],
        fq.[FirstName],
        fq.[LastName],
        fq.[_RowNumber]
    from (
        select
            m.[ID],
            m.[FirstName],
            m.[LastName],
            (ROW_NUMBER() OVER (ORDER BY PATINDEX(@v4, [FirstName] + @v5 + [LastName]) DESC)) as '_RowNumber'
        from (
            select
                p.[ID],
                p.[FirstName],
                p.[LastName]
            from [dbo].[Person] as p
        ) as m
        join (
            select
                p.[ID],
                p.[FirstName],
                p.[LastName]
            from (
                    select
                        p.[ID],
                        p.[FirstName],
                        p.[LastName]
                    from [dbo].[Person] as p
                    where p.[FirstName] like @v
                union
                    select
                        p.[ID],
                        p.[FirstName],
                        p.[LastName]
                    from [dbo].[Person] as p
                    where p.[LastName] like @v1
                union
                    select
                        p.[ID],
                        p.[FirstName],
                        p.[LastName]
                    from [dbo].[Person] as p
                    where p.[FirstName] like @v2
                union
                    select
                        p.[ID],
                        p.[FirstName],
                        p.[LastName]
                    from [dbo].[Person] as p
                    where p.[LastName] like @v3
            ) as ta1
        ) as m2 on m.[ID] = m2.[ID]
    ) as fq
) as ta1
where ([_RowNumber] > @v6 AND [_RowNumber] <= @v7)`)

        compare(safe.sql, 
`select
    [ID],
    [FirstName],
    [LastName],
    [ca1]
from (
    select
        (ROW_NUMBER() OVER (ORDER BY ta2.[ca1] ASC)) as 'ca1',
        ta2.[ID],
        ta2.[FirstName],
        ta2.[LastName],
        ta2.[ca1]
    from (
        select
            ta2.[ID],
            ta2.[FirstName],
            ta2.[LastName],
            (ROW_NUMBER() OVER (ORDER BY PATINDEX(@v4, [FirstName] + @v5 + [LastName]) DESC)) as 'ca1'
        from (
            select
                ta2.[ID],
                ta2.[FirstName],
                ta2.[LastName]
            from [dbo].[Person] as ta2
        ) as ta2
        join (
            select
                ta2.[ID],
                ta2.[FirstName],
                ta2.[LastName]
            from (
                    select
                        ta2.[ID],
                        ta2.[FirstName],
                        ta2.[LastName]
                    from [dbo].[Person] as ta2
                    where ta2.[FirstName] like @v
                union
                    select
                        ta2.[ID],
                        ta2.[FirstName],
                        ta2.[LastName]
                    from [dbo].[Person] as ta2
                    where ta2.[LastName] like @v1
                union
                    select
                        ta2.[ID],
                        ta2.[FirstName],
                        ta2.[LastName]
                    from [dbo].[Person] as ta2
                    where ta2.[FirstName] like @v2
                union
                    select
                        ta2.[ID],
                        ta2.[FirstName],
                        ta2.[LastName]
                    from [dbo].[Person] as ta2
                    where ta2.[LastName] like @v3
            ) as ta1
        ) as ta3 on ta2.[ID] = ta3.[ID]
    ) as ta2
) as ta1
where ([ca1] > @v6 AND [ca1] <= @v7)`)
    });
});

