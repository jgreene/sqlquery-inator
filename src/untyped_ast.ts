import * as moment from 'moment'

const validFunctionNames = [
    'ISNULL',
    'CONCAT',
    'COUNT',
    'MAX',
    'MIN',
    'SUM',
    'AVG',
    'PATINDEX',
]

const operators = [
    '+',
    '-',
    '/',
    '*',
    '%'
]

export function registerFunction(name: string) {
    validFunctionNames.push(name);
}

export function isValidFunction(name: string): boolean {
    return validFunctionNames.indexOf(name) !== -1;
}

export function isValidOperator(name: string): boolean {
    return operators.indexOf(name) !== -1;
}

export type ColumnType = boolean | number | string | moment.Moment | null

export const allTags: string[] = []
function addTag(tag: string) {
    if(allTags.indexOf(tag) === -1){
        allTags.push(tag);
    }
}

function is<T extends Expr>(tag: string) {
    addTag(tag);
    return function(input: Expr | undefined): input is T {
        return !!input && (input as any)['_tag'] === tag
    }
}

export abstract class Expr {
    readonly _tag: string = '';
}

export class FromExpr extends Expr {
    readonly _tag = 'FromExpr'
    constructor(public tableName: string, public alias: string) {
        super()
    }
}

export const isFromExpr = is<FromExpr>('FromExpr')

export class FromSelectExpr extends Expr {
    readonly _tag = 'FromSelectExpr'
    constructor(public expr: SelectStatementExpr, public alias: string) {
        super()
    }
}

export const isFromSelectExpr = is<FromSelectExpr>('FromSelectExpr')

export class RowNumberExpr extends Expr {
    readonly _tag = 'RowNumberExpr'
    constructor(public orderBy: OrderByExpr) {
        super()
    }
}

export const isRowNumberExpr = is<RowNumberExpr>('RowNumberExpr')

export class GroupByExpr extends Expr {
    readonly _tag = 'GroupByExpr'
    constructor(public projection: ProjectionExpr) {
        super()
    }
}

export const isGroupByExpr = is<GroupByExpr>('GroupByExpr')

export type JoinType = 'inner' | 'leftOuter' | 'rightOuter'

export const JoinType = {
    inner: 'inner' as JoinType,
    leftOuter: 'leftOuter' as JoinType,
    rightOuter: 'rightOuter' as JoinType
}

export function isValidJoinType(input: any): input is JoinType {
    return (JoinType as any)[input] !== undefined;
}

export class TableReferenceExpr extends Expr {
    readonly _tag = 'TableReferenceExpr'
    constructor(
        public tableName: string
    ) {
        super()
    }
}

export const isTableReferenceExpr = is<TableReferenceExpr>('TableReferenceExpr')

type JoinOptions = {
    parent: SelectStatementExpr | JoinExpr | FromExpr | FromSelectExpr
    joinType: JoinType
    joinSource: Expr
    alias: string
    on: Expr
    where?: WhereExpr
    orderBy?: OrderByExpr
    groupBy?: GroupByExpr
    _tag?: string
}

export class JoinExpr extends Expr {
    readonly _tag = 'JoinExpr'
    public parent: SelectStatementExpr | JoinExpr | FromExpr | FromSelectExpr
    public joinType: JoinType
    public joinSource: Expr
    public alias: string
    public on: Expr
    public where?: WhereExpr
    public orderBy?: OrderByExpr
    public groupBy?: GroupByExpr

    constructor(
        options: JoinOptions
    ) {
        super()
        this.parent = options.parent
        this.joinType = options.joinType
        this.joinSource = options.joinSource
        this.alias = options.alias
        this.on = options.on
        Object.assign(this, options);
    }
}

export const isJoinExpr = is<JoinExpr>('JoinExpr')

export class WhereExpr extends Expr {
    readonly _tag = 'WhereExpr'
    constructor(public clause: OrExpr | AndExpr | PredicateExpr) {
        super()
    }
}

export const isWhereExpr = is<WhereExpr>('WhereExpr')

export class AndExpr extends Expr {
    readonly _tag = 'AndExpr'
    constructor(public left: Expr, public right: Expr) {
        super()
    }
}

export const isAndExpr = is<AndExpr>('AndExpr')

export class OrExpr extends Expr {
    readonly _tag = 'OrExpr'
    constructor(public left: Expr, public right: Expr) {
        super()
    }
}

export const isOrExpr = is<OrExpr>('OrExpr')

export type PredicateOperator = | "equals" 
    | "notEquals" 
    | "greaterThan" 
    | "lessThan" 
    | "greaterThanOrEquals" 
    | "lessThanOrEquals" 
    | "isNull" 
    | "isNotNull"
    | "like"
    | "in"

export const PredicateOperator = {
    equals: "equals" as PredicateOperator,
    notEquals: "notEquals"  as PredicateOperator,
    greaterThan: "greaterThan" as PredicateOperator,
    lessThan: "lessThan"  as PredicateOperator,
    greaterThanOrEquals: "greaterThanOrEquals" as PredicateOperator,
    lessThanOrEquals: "lessThanOrEquals"  as PredicateOperator,
    isNull: "isNull" as PredicateOperator,
    isNotNull: "isNotNull" as PredicateOperator,
    like: "like" as PredicateOperator,
    in: "in" as PredicateOperator
}

export function isValidPredicateOperator(input: any): input is PredicateOperator {
    const res = (PredicateOperator as any)[input]
    return res !== undefined;
}

export class PredicateExpr extends Expr {
    readonly _tag = 'PredicateExpr'
    constructor(public left: Expr, public operator: PredicateOperator, public right?: Expr | undefined) {
        super()
    }
}

export const isPredicateExpr = is<PredicateExpr>('PredicateExpr')

export class ColumnExpr extends Expr {
    readonly _tag = 'ColumnExpr'
    constructor(public tableName: string, public columnName: string, public alias?: string | undefined) {
        super()
    }
}

export const isColumnExpr = is<ColumnExpr>('ColumnExpr')

export class FieldExpr extends Expr {
    readonly _tag = 'FieldExpr'
    constructor(public name: string, public alias?: string | undefined) {
        super()
    }
}

export const isFieldExpr = is<FieldExpr>('FieldExpr')

export class AsExpr extends Expr {
    readonly _tag = 'AsExpr'
    constructor(public left: Expr, public alias: string) {
        super()
    }
}

export const isAsExpr = is<AsExpr>('AsExpr')

export class ProjectionExpr extends Expr {
    readonly _tag = 'ProjectionExpr'

    constructor(public projections: Array<Expr>) {
        super()
    }
}

export const isProjectionExpr = is<ProjectionExpr>('ProjectionExpr')

export class StarExpr extends Expr {
    readonly _tag = 'StarExpr'

    constructor() {
        super()
    }
}

export const isStarExpr = is<StarExpr>('StarExpr')

type SelectOptions = {
    projection: ProjectionExpr
    from?: SelectStatementExpr | JoinExpr | FromExpr | FromSelectExpr | UnionExpr | undefined, 
    where?: Expr | undefined, 
    alias?: string | undefined,
    orderBy?: OrderByExpr | undefined,
    take?: TakeExpr | undefined,
    distinct?: boolean | undefined,
    groupBy?: GroupByExpr | undefined,
    _tag?: string
}

export class SelectStatementExpr extends Expr {
    readonly _tag = 'SelectStatementExpr'

    public readonly projection: ProjectionExpr
    public readonly from?: SelectStatementExpr | JoinExpr | FromExpr | FromSelectExpr | UnionExpr | undefined
    public readonly where?: WhereExpr | undefined
    public readonly alias?: string | undefined
    public readonly orderBy?: OrderByExpr | undefined
    public readonly take?: TakeExpr | undefined
    public readonly distinct?: boolean | undefined
    public readonly groupBy?: GroupByExpr | undefined

    constructor(
        options: SelectOptions
        
    ) {
        super()
        this.projection = options.projection;
        Object.assign(this, options);
    }
}

export const isSelectStatementExpr = is<SelectStatementExpr>('SelectStatementExpr')

export class OrderByExpr extends Expr {
    readonly _tag = 'OrderByExpr'

    constructor(public field: Expr, public direction: 'ASC' | 'DESC', public parent?: OrderByExpr | undefined) {
        super()
    }
}

export const isOrderByExpr = is<OrderByExpr>('OrderByExpr')

export class TakeExpr extends Expr {
    readonly _tag = 'TakeExpr'

    constructor(public take: number) {
        super()
    }
}

export const isTakeExpr = is<TakeExpr>('TakeExpr')

export class ScalarFunctionExpr extends Expr {
    readonly _tag = 'ScalarFunctionExpr'

    constructor(public name: string, public args: Expr[]) {
        super()
    }
}

export const isScalarFunctionExpr = is<ScalarFunctionExpr>('ScalarFunctionExpr')

export class OperatorExpr extends Expr {
    readonly _tag = 'OperatorExpr'

    constructor(public left: Expr, public name: string, public right: Expr) {
        super()
    }
}

export const isOperatorExpr = is<OperatorExpr>('OperatorExpr')

export class AggregateFunctionExpr extends Expr {
    readonly _tag = 'AggregateFunctionExpr'

    constructor(public name: string, public distinct: boolean, public arg: Expr) {
        super()
    }
}

export const isAggregateFunctionExpr = is<AggregateFunctionExpr>('AggregateFunctionExpr')

export class ValueExpr extends Expr {
    readonly _tag = 'ValueExpr'

    constructor(public value: ColumnType) {
        super()
    }
}

export const isValueExpr = is<ValueExpr>('ValueExpr')

export class NullExpr extends Expr {
    readonly _tag = 'NullExpr'

    constructor() {
        super()
    }
}

export const isNullExpr = is<NullExpr>('NullExpr')

export class EmptyStringExpr extends Expr {
    readonly _tag = 'EmptyStringExpr'

    constructor() {
        super()
    }
}

export const isEmptyStringExpr = is<EmptyStringExpr>('EmptyStringExpr')

export class UnionExpr extends Expr {
    readonly _tag = 'UnionExpr'

    constructor(
        public select1: SelectStatementExpr | UnionExpr, 
        public select2: SelectStatementExpr, 
        public all: boolean) {
        super()
    }
}

export const isUnionExpr = is<UnionExpr>('UnionExpr')

export class ArrayExpr extends Expr {
    readonly _tag = 'ArrayExpr'

    constructor(
        public expressions: Array<Expr>) {
        super()
    }
}

export const isArrayExpr = is<ArrayExpr>('ArrayExpr')

// export class ExistsExpr extends Expr {
//     readonly _tag = 'ExistsExpr'

//     constructor(
//         public select: SelectStatementExpr) {
//         super()
//     }
// }

// export const isExistsExpr = is<ExistsExpr>('ExistsExpr')
