import { expect, use } from 'chai';
import 'mocha';

import { toUnsafeQuery, toSafeQuery, Address, Person } from './testschema.spec'
import { registerTable, from, ISNULL, val, ROW_NUMBER, COUNT, AVG, MAX, MIN, SUM } from './index';
import { hydrateResults } from './sqlquery'


describe('sqlquery tests', () => {

    it('Can hydrate result set from sql query aliases', async () => {
        const query = from(Person, 'p').select(p => ({ Test: p.FirstName }))
        const safe = toSafeQuery(query.expr)
        const originalResults = [{ ca1: 'Heinz' }]
        const hydrated = hydrateResults(safe, originalResults)
        
        expect(hydrated.length).eq(1)
        const result = hydrated[0]
        expect(result).has.property('Test')
        expect(result.Test).eq('Heinz')
        expect(result).does.not.have.property('ca1')
    })
})
