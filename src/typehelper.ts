import * as ut from './untyped_ast'
import * as t from 'io-ts'
import * as tdc from 'io-ts-derive-class'
import * as moment from 'moment'

export function GetTypeFromValue<T extends ut.ColumnType>(value: T): t.Type<any> {
    if(typeof value === 'string'){
        return t.string
    }

    if(typeof value === 'number'){
        return t.number
    }

    if(typeof value === 'boolean'){
        return t.boolean
    }

    if(moment.isMoment(value)){
        return tdc.DateTime
    }

    if(value === null){
        return t.null
    }

    throw new Error('Could not get type for value: ' + JSON.stringify(value, null, 2));
}

export function getTypesFromTag(type: t.Type<any>): Array<'string' | 'number' | 'null' | 'undefined'> {
    const tag = (type as any)['_tag'];

    if (tag === "UnionType") {
        const u = type as t.UnionType<any>;

        let res = u.types.map((innerType: t.Type<any>) => getTypesFromTag(innerType));
        return [].concat.apply([], res);
    }

    if (tag === "LiteralType") {
        const lit = type as t.LiteralType<any>;
        if (typeof lit.value === 'string') {
            return ['string']
        }

        if (typeof lit.value === 'number') {
            return ['number']
        }
    }

    if (tag === "KeyofType") {
        return ['string']
    }

    if (tag === "NumberType") {
        return ['number'];
    }

    if (tag === 'StringType') {
        return ['string'];
    }

    if (tag === 'NullType') {
        return ['null'];
    }

    if (tag === 'UndefinedType') {
        return ['undefined'];
    }

    return [] as Array<'string' | 'number' | 'null' | 'undefined'>;
};

