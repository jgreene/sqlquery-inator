import { expect, use } from 'chai';
import 'mocha';
import * as ut from './untyped_ast'
import * as sql from './mssql'
import { getFindTable, toUnsafeQuery, toSafeQuery, Address, Person } from './testschema.spec'
import { registerTable, from, ISNULL, val, ROW_NUMBER, COUNT, AVG, MAX, MIN, SUM } from './index';
import { hydrateResults } from './sqlquery'

const INJECTION_CONSTANT = 'SQLINJECTIONSTRING'

const testedTags: string[] = []
function addTag(tag: string) {
    if(testedTags.indexOf(tag) === -1){
        testedTags.push(tag);
    }
}

// Expressions that only receive other expressions and no string aren't injectable.  We add those tags here asserting that they are safe.
addTag('RowNumberExpr')
addTag('GroupByExpr')
addTag('WhereExpr')
addTag('AndExpr')
addTag('OrExpr')
addTag('PredicateExpr')
addTag('ProjectionExpr')
addTag('StarExpr')
addTag('NullExpr')
addTag('EmptyStringExpr')
addTag('UnionExpr')


function GetUntestedTags() {
    const untestedTags = ut.allTags.filter(t => testedTags.some(tt => t === tt) === false)
    return untestedTags
}

function test(expr: ut.Expr) {
    var safeInjectionOccurred = true
    try
    {
        const safeCtx = sql.createContext({ allowSqlInjection: false, getTableSchema: getFindTable })
        const safeResult = sql.toSql(expr, safeCtx)
        safeInjectionOccurred = safeResult.indexOf(INJECTION_CONSTANT) !== -1
    }
    catch(error) {
        safeInjectionOccurred = false
    }
    
    expect(safeInjectionOccurred).eq(false)

    // const unsafeCtx = sql.createContext({ allowSqlInjection: true })
    // const unsafeResult = sql.toSql(expr, unsafeCtx)
    // const unsafeInjectionOccurred = unsafeResult.indexOf(INJECTION_CONSTANT) !== -1
    // expect(unsafeInjectionOccurred).eq(true)

    addTag(expr._tag)
}


describe('sql injection tests', () => {

    it('FromExpr is not injectable', async () => {
        test(new ut.FromExpr(INJECTION_CONSTANT, 'p'))
        test(new ut.FromExpr('dbo.Person', INJECTION_CONSTANT))
    })

    it('FromSelectExpr is not injectable', async () => {
        const query = from(Person, 'p').selectAll()
        test(new ut.FromSelectExpr(query.expr, INJECTION_CONSTANT))
    })

    it('TableReferenceExpr is not injectable', async () => {
        test(new ut.TableReferenceExpr(INJECTION_CONSTANT))
    })

    it('JoinExpr is not injectable', async () => {
        const joinQuery = from(Person, 'p')
                            .join(Person, 'p2').on(r => r.p.ID.equals(r.p2.ID))
        
        const joinExpr = joinQuery.expr;
        test(new ut.JoinExpr({...joinExpr, alias: INJECTION_CONSTANT}))
        test(new ut.JoinExpr({...joinExpr, joinType: (INJECTION_CONSTANT as any)}))
    })

    it('ColumnExpr is not injectable', async () => {
        test(new ut.ColumnExpr(INJECTION_CONSTANT, 'FirstName'))
        test(new ut.ColumnExpr('dbo.Person', INJECTION_CONSTANT))
        test(new ut.ColumnExpr('dbo.Person', 'p', INJECTION_CONSTANT))
    })

    it('FieldExpr is not injectable', async () => {
        test(new ut.FieldExpr(INJECTION_CONSTANT, INJECTION_CONSTANT))
    })

    it('AsExpr is not injectable', async () => {
        test(new ut.AsExpr(new ut.FieldExpr('Test', 't'), INJECTION_CONSTANT))
    })

    it('SelectStatementExpr is not injectable', async () => {
        const query = from(Person, INJECTION_CONSTANT).selectAll()

        test(query.expr)
    })

    it('OrderByExpr is not injectable', async () => {
        const query = from(Person, INJECTION_CONSTANT).selectAll().orderBy(p => p.ID)
        const orderByExpr = query.expr.orderBy!;

        test(new ut.OrderByExpr(orderByExpr.field, INJECTION_CONSTANT as any, orderByExpr.parent))
    })

    it('TakeExpr is not injectable', async () => {
        test(new ut.TakeExpr(INJECTION_CONSTANT as any))
    })

    it('ScalarFunctionExpr is not injectable', async () => {
        test(new ut.ScalarFunctionExpr(INJECTION_CONSTANT, []))
    })

    it('AggregateFunctionExpr is not injectable', async () => {
        test(new ut.AggregateFunctionExpr(INJECTION_CONSTANT, false, new ut.StarExpr()))
    })

    it('ValueExpr is not injectable', async () => {
        test(new ut.ValueExpr(INJECTION_CONSTANT))
    })

    it('OperatorExpr is not injectable', async () => {
        test(new ut.OperatorExpr(new ut.FieldExpr('Test', 't'), INJECTION_CONSTANT, new ut.FieldExpr('Test2', 't')))
    })

    it('All tags are tested for sql injection vulnerabilities', async () => {
        const untested = GetUntestedTags()
        if(untested.length > 0){
            console.log(`There are ${untested.length} untested tags`)
            console.log(untested)
        }
        
        expect(untested.length).eq(0)
    })
})




