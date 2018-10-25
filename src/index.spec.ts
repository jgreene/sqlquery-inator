import { expect } from 'chai';
import 'mocha';

import * as t from 'io-ts'
import * as tdc from 'io-ts-derive-class'

import { registerTable, from } from './index';
import * as ut from './untyped_ast';

const PersonType = t.type({
    ID: t.Integer,
    FirstName: t.string,
    LastName: t.string,
})

class Person extends tdc.DeriveClass(PersonType) {}

registerTable<Person>(Person, 'TestDB.dbo.Person');

describe('SQL query AST', () => {

    it('From Expression has table name', async () => {
        const expr = from(Person, 'p').expr

        expect(ut.isFromExpr(expr)).eq(true)

        if(ut.isFromExpr(expr)){
            expect(expr.tableName).eq('TestDB.dbo.Person')
        }
    })

    it('Can Select all', async () => {
        const selectExpr = from(Person, 'p').selectAll().expr;
        
        expect(ut.isSelectStatementExpr(selectExpr)).eq(true);

        if(ut.isSelectStatementExpr(selectExpr)){
            
        }
    });

    it('isValidComparison being passed an invalid comparison operator results in false', async () => {
        const res = ut.isValidPredicateOperator("invalid")

        expect(res).eq(false)
    })

    it('isValidComparison being passed a valid comparison operator results in true', async () => {
        const res = ut.isValidPredicateOperator(ut.PredicateOperator.equals)

        expect(res).eq(true)
    })

});