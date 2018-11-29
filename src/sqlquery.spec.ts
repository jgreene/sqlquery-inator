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

    it('Hydrating results does not change column order', async () => {
        const query = from(Person, 'p').select(p => ({ FirstName: p.FirstName, Test: p.FirstName, LastName: p.LastName }))
        const safe = toSafeQuery(query.expr)
        const originalResults = [{ 'FirstName': 'Heinz', ca1: 'Heinz', 'LastName': 'Doofenschmirtz' }]
        const hydrated = hydrateResults(safe, originalResults)
        
        expect(hydrated.length).eq(1)
        const result = hydrated[0]
        expect(result).has.property('Test')
        expect(result.Test).eq('Heinz')
        expect(result).does.not.have.property('ca1')
        const keys = Object.keys(result);
        expect(keys.length).eq(3)
        expect(keys[1]).eq('Test')
    })
})
