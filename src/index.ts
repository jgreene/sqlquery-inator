import * as ut from './untyped_ast'
import * as t from 'io-ts'
import * as tdc from 'io-ts-derive-class'
import { GetTypeFromValue, getTypesFromTag } from './typehelper'

export type ColumnType = ut.ColumnType

type Constructor<T> = new (...args: any[]) => T
type Table<T> = Constructor<tdc.ITyped<any, T, any, t.mixed>>

export type Row<T> = {
    [P in keyof T]: T[P] extends ColumnExpr<infer U> ? T[P] : T[P] extends ColumnType ? ColumnExpr<T[P]> : never
}

export type ToOuterRow<T> = {
    [P in keyof T]: T[P] extends ColumnType ? ColumnExpr<T[P] | null> : T[P] extends ColumnExpr<infer U> ? ColumnExpr<U | null> : never
}

export type AggregateRow<T> = {
    [P in keyof T]: T[P] extends AggregateColumnExpr<infer U> ? T[P] : never
}

const tableNameMap: any = {}

export function registerTable<T>(ctor: Constructor<T>, tableName: string) {
    tableNameMap[ctor as any] = tableName;
}

export function registerFunction(name: string) {
    ut.registerFunction(name);
}

export function getTableFromType(ctor: Table<any>): string {
    const name = tableNameMap[ctor as any];
    if(name === undefined){
        throw new Error('Could not find table for type: ' + ctor.name);
    }
    return name;
}

function createComparisonOperator(operator: ut.PredicateOperator) {
    return function<T, C1 extends ColumnType, C2 extends ColumnType>(c1: C1 | ColumnExpr<C1>, c2: C2 | ColumnExpr<C2>): PredicateExpr<T> {
        const left = c1 instanceof ColumnExpr ? c1.expr : val(c1).expr;
        const right = c2 instanceof ColumnExpr ? c2.expr : val(c2).expr;

        const pred = new ut.PredicateExpr(left, operator, right)
        
        return new PredicateExpr<T>(pred);
    }
}

const operators = {
    equals: createComparisonOperator(ut.PredicateOperator.equals),
    notEquals: createComparisonOperator(ut.PredicateOperator.notEquals),
    greaterThan: createComparisonOperator(ut.PredicateOperator.greaterThan),
    lessThan: createComparisonOperator(ut.PredicateOperator.lessThan),
    greaterThanOrEquals: createComparisonOperator(ut.PredicateOperator.greaterThanOrEquals),
    lessThanOrEquals: createComparisonOperator(ut.PredicateOperator.lessThanOrEquals),
    like: createComparisonOperator(ut.PredicateOperator.like),
}

abstract class TypedExpr<T> {
    constructor(public expr: ut.Expr) {}
}

export class ColumnExpr<C extends ColumnType> extends TypedExpr<C> {
    constructor(public type: t.Type<any>, public expr: ut.Expr) {
        super(expr)
    }

    equals<T, C2 extends ColumnType>(c2: C2 | ColumnExpr<C2>): PredicateExpr<T> {
        return operators.equals<T, C, C2>(this, c2);
    }

    notEquals<T, C2 extends ColumnType>(c2: C2 | ColumnExpr<C2>): PredicateExpr<T> {
        return operators.notEquals<T, C, C2>(this, c2);
    }

    greaterThan<T, C2 extends ColumnType>(c2: C2 | ColumnExpr<C2>): PredicateExpr<T> {
        return operators.greaterThan<T, C, C2>(this, c2);
    }

    lessThan<T, C2 extends ColumnType>(c2: C2 | ColumnExpr<C2>): PredicateExpr<T> {
        return operators.lessThan<T, C, C2>(this, c2);
    }

    greaterThanOrEquals<T, C2 extends ColumnType>(c2: C2 | ColumnExpr<C2>): PredicateExpr<T> {
        return operators.greaterThanOrEquals<T, C, C2>(this, c2);
    }

    lessThanOrEquals<T, C2 extends ColumnType>(c2: C2 | ColumnExpr<C2>): PredicateExpr<T> {
        return operators.lessThanOrEquals<T, C, C2>(this, c2);
    }

    like<T, C2 extends ColumnType>(c2: C2 | ColumnExpr<C2>): PredicateExpr<T> {
        return operators.like<T, C, C2>(this, c2);
    }

    as<TName extends string>(name: TName): AsExpr<C, TName> {
        return as<C, TName>(this, name)
    }

    private createOperator<C2 extends ColumnType>(operator: string) {
        if(!ut.isValidOperator(operator)){
            throw new Error('Unsupported operator: ' + operator);
        }

        const self = this;
        return function(c2: C2 | ColumnExpr<C2>): ColumnExpr<C> {
            const column = c2 instanceof ColumnExpr ? c2 : val(c2);
            const rightExpr = column.expr
            var op = new ut.OperatorExpr(self.expr, operator, rightExpr);
            return  new ColumnExpr<C>(column.type, op);
        }
    }

    add = this.createOperator<string | number | null>('+')
    subtract = this.createOperator<number>('-')
    divide = this.createOperator<number>('/')
    multiply = this.createOperator<number>('*')
    modulo = this.createOperator<number>('%')

    get asc(): OrderColumnExpr<C> {
        return new OrderColumnExpr<C>(new ut.OrderByExpr(this.expr, 'ASC'))
    }

    get desc(): OrderColumnExpr<C> {
        return new OrderColumnExpr<C>(new ut.OrderByExpr(this.expr, 'DESC'))
    }
}

export class AggregateColumnExpr<C extends ColumnType> extends ColumnExpr<C> {
    /* forcing the type checker to not assume AggregateColumnExpr and ColumnExpr are equivalent */
    public isAggregateColumn: boolean = true
    constructor(type: t.Type<any>, expr: ut.Expr) {
        super(type, expr)
    }
}

function toAggregateColumn<C extends ColumnType>(column: ColumnExpr<C>): AggregateColumnExpr<C> {
    return new AggregateColumnExpr<C>(column.type, column.expr);
}

export class OrderColumnExpr<C extends ColumnType> extends TypedExpr<C> {
    constructor(public expr: ut.OrderByExpr) {
        super(expr)
    }
}

export class WindowFunctionColumnExpr<C extends ColumnType> extends ColumnExpr<C> {
    constructor(public type: t.Type<any>, expr: ut.Expr){
        super(type, expr)
    }
}

export class AsExpr<T extends ColumnType, name extends string> extends ColumnExpr<T> {
    constructor(public column: ColumnExpr<T>, public name: name) {
        super(column.type, new ut.AsExpr(column.expr, name))
    }
}

export function as<C extends ColumnType, name extends string>(expr: ColumnExpr<C>, alias: name): AsExpr<C, name> {
    return new AsExpr<C, name>(expr, alias);
}

export class ValueExpr<T extends ColumnType> extends ColumnExpr<T> {
    constructor(public type: t.Type<any>, public expr: ut.Expr) {
        super(type, expr)
    }
}



export function val<T extends ColumnType>(value: T): ValueExpr<T> {
    const type = GetTypeFromValue(value);
    const expr = value === '' ? new ut.EmptyStringExpr() : value === null ? new ut.NullExpr : new ut.ValueExpr(value)

    return new ValueExpr(type, expr);
}

export function ISNULL<C extends ColumnType>(expr: TypedExpr<C | null>, value: ValueExpr<Exclude<C, null>>): ColumnExpr<Exclude<C, null>> {
    return new ColumnExpr(value.type, new ut.ScalarFunctionExpr('ISNULL', [expr.expr, value.expr]))
}

export function COUNT<C extends ColumnType>(column?: ColumnExpr<C> | undefined): AggregateColumnExpr<number> {
    if(column === undefined){
        return new AggregateColumnExpr(t.number, new ut.AggregateFunctionExpr('COUNT', false, new ut.StarExpr));
    }
    
    return new AggregateColumnExpr(t.number, new ut.AggregateFunctionExpr('COUNT', true, column.expr));
};

function createAggregateFunction<C extends ColumnType>(name: string){
    return function(column: ColumnExpr<C>, distinct: boolean = false): AggregateColumnExpr<number> {
        return new AggregateColumnExpr(t.number, new ut.AggregateFunctionExpr(name, distinct, column.expr));
    };
}

export const AVG = createAggregateFunction<number | null>('AVG')
export const SUM = createAggregateFunction<number | null>('SUM')
export const MAX = createAggregateFunction<number | null>('MAX')
export const MIN = createAggregateFunction<number | null>('MIN')

function getOrderByExpr<T>(ordering: OrderColumnExpr<any>[]): ut.OrderByExpr {
    var expr: ut.OrderByExpr | undefined = undefined;

    ordering.forEach(c => {
        expr = new ut.OrderByExpr(c.expr.field, c.expr.direction, expr);
    })

    if(expr === undefined){
        throw new Error('Could not get order by expression from ' + JSON.stringify(ordering));
    }

    return expr;
}

function INTERNAL_ROW_NUMBER<T>(ordering: ut.OrderByExpr): WindowFunctionColumnExpr<number> {
    return new WindowFunctionColumnExpr<number>(t.number, new ut.RowNumberExpr(ordering));
}

export function ROW_NUMBER<T>(ordering: OrderColumnExpr<any>[]): WindowFunctionColumnExpr<number> {
    const expr = getOrderByExpr(ordering);

    return INTERNAL_ROW_NUMBER(expr);
}

export function PATINDEX<T extends string>(value: T | ColumnExpr<T>, columnExpr: ColumnExpr<T>): ColumnExpr<T> {
    const column = value instanceof ColumnExpr ? value : val(value);
    return new ColumnExpr(t.number, new ut.ScalarFunctionExpr('PATINDEX', [column.expr, columnExpr.expr]))
}

function GetColumnProjectionsFromTable<T>(table: Table<T>, alias?: string | undefined): Row<T> {
    const type = tdc.getType(table);
    if(type === null)
    {
        throw new Error('Could not get column projections for ' + JSON.stringify(table, null, 2));
    }
    const tableName = getTableFromType(table);
    if(tableName === null){
        throw new Error(`Could not get tableName from type: ${JSON.stringify(table, null, 2)}`);
    }
    const projection: any = {};
    Object.keys(type.props).forEach(key => {
        const propType = type.props[key];
        projection[key] = new ColumnExpr<any>(propType, new ut.ColumnExpr(tableName, key, alias));
    })

    return projection;
}

function GetColumnProjectionsFromRow<T>(row: Row<T>, alias?: string | undefined): Row<T> {
    const projection: any = {}
    for(let key in row){
        const field = row[key];
        projection[key] = new ColumnExpr<any>(field.type, new ut.FieldExpr(key, alias));
    }

    return projection;
}

function GetProjectionExprFromRow<T>(row: Row<T> | AggregateRow<T>): ut.ProjectionExpr {
    let keys = Object.keys(row);
    if(keys.length === 0){
        throw new Error('Cannot select 0 columns!');
    }

    const res: ut.Expr[] = [];

    for(let key in row){
        const prop = row[key];
        if(ut.isColumnExpr(prop.expr) && key === prop.expr.columnName){
            res.push(prop.expr);
        }
        else
        {
            const asExpr = as(prop as any, key);
            res.push(asExpr.expr);
        }
    }

    return new ut.ProjectionExpr(res)
}

export class PredicateExpr<T> extends TypedExpr<T> {
    constructor(public expr: ut.PredicateExpr | ut.AndExpr | ut.OrExpr) {
        super(expr)
    }

    and<R>(right: PredicateExpr<R>): PredicateExpr<R> {
        const a = new ut.AndExpr(this.expr, right.expr);

        return new PredicateExpr<R>(a);
    }

    or<R>(right: PredicateExpr<R>): PredicateExpr<R> {
        const a = new ut.OrExpr(this.expr, right.expr);

        return new PredicateExpr<R>(a);
    }
}

function isWindowedFunctionCall(column: ColumnExpr<ColumnType>): boolean {
    if(column instanceof WindowFunctionColumnExpr){
        return true;
    }

    if(column instanceof AsExpr){
        return isWindowedFunctionCall(column.column);
    }

    return false;
}

function hasWindowedFunction<T>(row: Row<T>){
    for(let key in row){
        const column = row[key];
        const isWindowed = isWindowedFunctionCall(column);
        if(isWindowed)
        {
            return true;
        }
    }

    return false;
}

export class SelectExpr<T> extends TypedExpr<T> {
    constructor(public row: Row<T>, public expr: ut.SelectStatementExpr) {
        super(expr)
    }

    where(func: (t: Row<T>) => PredicateExpr<T>): SelectExpr<T> {
        if(!ut.isSelectStatementExpr(this.expr)){
            throw new Error('Invalid select statement detected!');
        }

        const isWindowed = hasWindowedFunction(this.row);
        if(!isWindowed){
            const pred = func(this.row);

            const where = ut.isWhereExpr(this.expr.where) ? 
                            new ut.WhereExpr(new ut.AndExpr(this.expr.where.clause, pred.expr))
                            : new ut.WhereExpr(pred.expr);

            let expr = new ut.SelectStatementExpr({
                ...this.expr,
                where: where,
            })
            return new SelectExpr<T>(this.row, expr);
        }

        
        const innerExpr = new ut.SelectStatementExpr(
                            {
                                ...this.expr,
                                where: undefined,
                                take: undefined,
                                distinct: undefined
                            });

        const newRow = GetColumnProjectionsFromRow(this.row, this.expr.alias);
        const pred = func(newRow);
        const where = new ut.WhereExpr(pred.expr);
        const newProjection = GetProjectionExprFromRow(newRow);

        const expr = new ut.SelectStatementExpr(
                        {
                            ...this.expr,
                            projection: newProjection,
                            from: innerExpr,
                            where: where,
                            orderBy: undefined,
                        });

        return new SelectExpr<T>(newRow, expr);
    }

    select<Projection>(func: (t: Row<T>) => Row<Projection>, alias?: string | undefined): 
        SelectExpr<Projection>
    {
        
        const newRow = GetColumnProjectionsFromRow(this.row);
        const row = func(newRow);

        const projection = GetProjectionExprFromRow(row);

        const select = new ut.SelectStatementExpr({ projection: projection, from: this.expr, alias: alias });

        return new SelectExpr<Projection>(row, select);
    }

    orderBy(func: (t: Row<T>) => ColumnExpr<ColumnType>, direction?: 'ASC' | 'DESC'): OrderByExpr<T> {
        
        const field = func(this.row);
        const orderBy = new ut.OrderByExpr(field.expr, direction || 'ASC')
        const select = new ut.SelectStatementExpr({
            ...this.expr,
            orderBy: orderBy
        });

        return new OrderByExpr<T>(this.row, select);
    }

    orderByDesc(func: (t: Row<T>) => ColumnExpr<ColumnType>): OrderByExpr<T> {
        return this.orderBy(func, 'DESC');
    }

    take(num: number): SelectExpr<T> {
        const select = new ut.SelectStatementExpr(
                            { ...this.expr, take: new ut.TakeExpr(num) }
                        );
        return new SelectExpr<T>(this.row, select);
    }

    distinct(): SelectExpr<T> {
        const select = new ut.SelectStatementExpr(
            {
                ...this.expr,
                distinct: true
            });


        return new SelectExpr<T>(this.row, select);
    }

    groupBy<G>(func: (t: Row<T>) => Row<G>): GroupByExpr<G> {
        const row = func(this.row);
        const projection = GetProjectionExprFromRow(row);

        const select = new ut.SelectStatementExpr(
            {
                ...this.expr,
                groupBy: new ut.GroupByExpr(projection)
            });

        return new GroupByExpr<G>(row, select)
    }

    count(func?: undefined | ((t: Row<T>) => ColumnExpr<ColumnType>)): SelectExpr<{ count: number}> {
        if(func === undefined) {
            return this.select(r => { return { count: COUNT()}});
        }

        const newRow = GetColumnProjectionsFromRow(this.row);

        const column = func(newRow);
        return this.select(r => { return { count: COUNT(column)}});
    }

    search<ColumnsToSearch>(searchText: string, func: (t: Row<T>) => Row<ColumnsToSearch>, searchWildcard: string = '*'): SelectExpr<T> {
        const newRow = GetColumnProjectionsFromRow(this.row);
        const row = func(newRow);
        
        const num = parseInt(searchText, 10) || parseFloat(searchText);
        const isNum = isNaN(num) === false;

        if(isNum){
            const numberRow = GetColumnsOfType(row, 'number')
            
            const query = this.where((r: any) => {
                var pred: any = undefined;
                for(let n in numberRow){
                    const column = numberRow[n];
                    if(pred === undefined){
                        pred = column.equals(num)
                    }
                    else {
                        pred.or(column.equals(num))
                    }
                }

                return pred;
            })

            return query;
        }

        var union: UnionExpr<any> | undefined = undefined

        
        
        throw ''
    }
}

export function GetColumnsOfType<T>(row: Row<T>, acceptedType: 'string' | 'number'): Row<T> {
    var result: any = {}
    for(let k in row){
        const column = row[k]
        const types = getTypesFromTag(column.type)
        if(types.indexOf(acceptedType) !== -1){
            result[k] = column
        }
    }
    return result;
}

function toAggregateRow<T>(row: Row<T>): AggregateRow<T> {
    var res: any = {};
    for(let key in row){
        let column = row[key];
        res[key] = toAggregateColumn(column);
    }

    return res;
}

export class OrderByExpr<T> extends SelectExpr<T> {
    constructor(row: Row<T>, expr: ut.SelectStatementExpr) {
        super(row, expr)
    }

    thenBy(func: (t: Row<T>) => ColumnExpr<ColumnType>, direction?: 'ASC' | 'DESC'): OrderByExpr<T> {
        const field = func(this.row);
        const orderBy = new ut.OrderByExpr(field.expr, direction || 'ASC', this.expr.orderBy)
        const select = new ut.SelectStatementExpr({
            ...this.expr,
            orderBy: orderBy
        });

        return new OrderByExpr<T>(this.row, select);
    }

    thenByDesc(func: (t: Row<T>) => ColumnExpr<ColumnType>): OrderByExpr<T> {
        return this.thenBy(func, 'DESC');
    }

    page(pageNumber: number, itemsPerPage: number): SelectExpr<T> {
        if(pageNumber < 1){
            throw new Error('Page numbers start at 1')
        }

        const rowNumberAlias = '_RowNumber'
        const orderBy = this.expr.orderBy!
        const rowNumberExpr = as(INTERNAL_ROW_NUMBER(orderBy), rowNumberAlias)

        const startIndex = (pageNumber - 1) * itemsPerPage
        const endIndex = (pageNumber * itemsPerPage)

        const rowNumberSelect = new ut.SelectStatementExpr({
            ...this.expr,
            projection: new ut.ProjectionExpr(
                            [rowNumberExpr.expr]
                                .concat(this.expr.projection.projections)
                        ),
            orderBy: undefined
        })

        const rowNumberColumnExpr = new ColumnExpr<number>(t.number, new ut.FieldExpr(rowNumberAlias));

        const predicate = operators.greaterThan(rowNumberColumnExpr, startIndex)
                            .and(operators.lessThanOrEquals(rowNumberColumnExpr, endIndex));

        const newRow = GetColumnProjectionsFromRow(this.row)
        const projection = GetProjectionExprFromRow(newRow);
        
        const select = new ut.SelectStatementExpr({
            projection: projection,
            from: rowNumberSelect,
            where: new ut.WhereExpr(predicate.expr),
            alias: this.expr.alias
        })

        return new SelectExpr<T>(this.row, select);
    }
}

abstract class JoinBuilderStart<L, LAlias extends string, R, RAlias extends string, T, TResult> {
    constructor(
        public joinType: ut.JoinType,
        public left: SelectExpr<L> | FromExpr<L, LAlias> | Table<L>,
        public lalias: LAlias,
        public right: SelectExpr<R> | FromExpr<R, RAlias> | Table<R>,
        public ralias: RAlias
    ){}

    on(func: (t: T) => PredicateExpr<any>): JoinExpr<TResult> {
        const aliases: any = {}
        const leftDefaults = (() => {
            if(this.left instanceof SelectExpr){
                return GetColumnProjectionsFromRow(this.left.row, this.lalias);
            }

            if(this.left instanceof FromExpr){
                if(this.left.source instanceof SelectExpr){
                    return GetColumnProjectionsFromRow(this.left.source.row, this.lalias)
                }

                return GetColumnProjectionsFromTable(this.left.source, this.lalias);
            }

            return GetColumnProjectionsFromTable(this.left, this.lalias);
        })();

        const rightDefaults = (() => {
            if(this.right instanceof SelectExpr){
                return GetColumnProjectionsFromRow(this.right.row, this.ralias);
            }

            if(this.right instanceof FromExpr){
                if(this.right.source instanceof SelectExpr){
                    return GetColumnProjectionsFromRow(this.right.source.row, this.ralias)
                }

                return GetColumnProjectionsFromTable(this.right.source, this.ralias);
            }

            return GetColumnProjectionsFromTable(this.right, this.ralias);
        })();

        aliases[this.lalias] = leftDefaults;
        aliases[this.ralias] = rightDefaults;

        const leftSource = (() => {
            if(this.left instanceof SelectExpr){
                return cleanSelectForInnerQuery(this.left.expr);
            }

            if(this.left instanceof FromExpr){
                if(this.left.source instanceof SelectExpr){
                    return this.left.expr;
                }

                const leftTableName = getTableFromType(this.left.source);
                return new ut.FromExpr(leftTableName, this.lalias);
            }

            const leftTableName = getTableFromType(this.left);
            return new ut.FromExpr(leftTableName, this.lalias);
        })();

        const rightSource = (() => {
            if(this.right instanceof SelectExpr){
                return cleanSelectForInnerQuery(this.right.expr);
            }

            if(this.right instanceof FromExpr){
                if(this.right.source instanceof SelectExpr){
                    return this.right.expr;
                }

                const rightTableName = getTableFromType(this.right.source);
                return new ut.TableReferenceExpr(rightTableName);
            }

            const rightTableName = getTableFromType(this.right);
            return new ut.TableReferenceExpr(rightTableName);
        })();

        const predicate = func(aliases as T);
        const joinExpr = new ut.JoinExpr({
            parent: leftSource,
            joinType: this.joinType,
            joinSource: rightSource,
            alias: this.ralias,
            on: predicate.expr
        })

        return new JoinExpr<TResult>(aliases, joinExpr);
    }
}

export class InnerJoinBuilderStart<L, LAlias extends string, R, RAlias extends string> extends 
    JoinBuilderStart<
        L, 
        LAlias, 
        R, 
        RAlias, 
        { [K in LAlias]: Row<L> } & { [K in RAlias]: Row<R>}, 
        { [K in LAlias]: Row<L> } & { [K in RAlias]: Row<R>}
    >
{
    constructor(
        public left: SelectExpr<L> | FromExpr<L, LAlias> | Table<L>,
        public lalias: LAlias,
        public right: SelectExpr<R> | FromExpr<R, RAlias> | Table<R>,
        public ralias: RAlias
    ){
        super(ut.JoinType.inner, left, lalias, right, ralias);
    }
}

export class LeftOuterJoinBuilderStart<L, LAlias extends string, R, RAlias extends string> extends 
    JoinBuilderStart<
        L, 
        LAlias, 
        R, 
        RAlias, 
        { [K in LAlias]: Row<L> } & { [K in RAlias]: Row<R>}, 
        { [K in LAlias]: Row<L> } & { [K in RAlias]: ToOuterRow<R>}
    >
{
    constructor(
        public left: SelectExpr<L> | FromExpr<L, LAlias>| Table<L>,
        public lalias: LAlias,
        public right: SelectExpr<R> | FromExpr<R, RAlias> | Table<R>,
        public ralias: RAlias
    ){
        super(ut.JoinType.leftOuter, left, lalias, right, ralias);
    }
}

export class RightOuterJoinBuilderStart<L, LAlias extends string, R, RAlias extends string> extends 
    JoinBuilderStart<
        L, 
        LAlias, 
        R, 
        RAlias, 
        { [K in LAlias]: Row<L> } & { [K in RAlias]: Row<R>}, 
        { [K in LAlias]: ToOuterRow<L> } & { [K in RAlias]: Row<R>}
    >
{
    constructor(
        public left: SelectExpr<L> | FromExpr<L, LAlias>  | Table<L>,
        public lalias: LAlias,
        public right: SelectExpr<R> | FromExpr<R, RAlias>  | Table<R>,
        public ralias: RAlias
    ){
        super(ut.JoinType.rightOuter, left, lalias, right, ralias);
    }
}

function cleanSelectForInnerQuery(select: ut.SelectStatementExpr): ut.SelectStatementExpr {
    const take = select.take
    const hasTake = take !== undefined
    const orderBy = hasTake ? select.orderBy : undefined
    const expr = new ut.SelectStatementExpr({
        ...select,
        orderBy: orderBy
    })

    return expr;
}

abstract class JoinBuilder<L, R, RAlias extends string, T, TResult> {
    constructor(
        public joinType: ut.JoinType, 
        public aliases: L, 
        public parent: ut.JoinExpr, 
        public right: SelectExpr<R> | FromExpr<R, RAlias>  | Table<R>, 
        public ralias: RAlias
    ){}

    on(func: (t: T) => PredicateExpr<any>): JoinExpr<TResult> {
        const aliases: any = {};
        Object.assign(aliases, this.aliases);
        
        const rightDefaults = (() => {
            if(this.right instanceof SelectExpr){
                return GetColumnProjectionsFromRow(this.right.row, this.ralias);
            }

            if(this.right instanceof FromExpr){
                if(this.right.source instanceof SelectExpr){
                    return GetColumnProjectionsFromRow(this.right.source.row, this.ralias)
                }

                return GetColumnProjectionsFromTable(this.right.source, this.ralias);
            }

            return GetColumnProjectionsFromTable(this.right, this.ralias);
        })();

        aliases[this.ralias] = rightDefaults;

        const rightSource = (() => {
            if(this.right instanceof SelectExpr){
                return cleanSelectForInnerQuery(this.right.expr);
            }

            if(this.right instanceof FromExpr){
                if(this.right.source instanceof SelectExpr){
                    return this.right.expr;
                }

                const rightTableName = getTableFromType(this.right.source);
                return new ut.TableReferenceExpr(rightTableName);
            }

            const rightTableName = getTableFromType(this.right);
            return new ut.TableReferenceExpr(rightTableName);
        })();

        const predicate = func(aliases as T);

        const where = this.parent.where
        const groupBy = this.parent.groupBy
        const orderBy = this.parent.orderBy

        const parent = new ut.JoinExpr({ 
                            ...this.parent, 
                            where: undefined ,
                            groupBy: undefined,
                            orderBy: undefined
                        })

        const joinExpr = new ut.JoinExpr({
            parent: parent,
            joinType: this.joinType,
            joinSource: rightSource,
            alias: this.ralias,
            on: predicate.expr,
            where: where,
            groupBy: groupBy,
            orderBy: orderBy
        })

        return new JoinExpr<TResult>(aliases, joinExpr);
    }
}

export class InnerJoinBuilder<L, R, RAlias extends string> extends 
    JoinBuilder<L, R, RAlias, L & { [K in RAlias]: Row<R>}, L & { [K in RAlias]: Row<R>}> {
    constructor(public aliases: L, public parent: ut.JoinExpr, public right: SelectExpr<R> | FromExpr<R, RAlias> | Table<R>, public ralias: RAlias){
        super(ut.JoinType.inner, aliases, parent, right, ralias);
    }
}

export class LeftOuterJoinBuilder<L, R, RAlias extends string> extends 
JoinBuilder<L, R, RAlias, L & { [K in RAlias]: Row<R>}, L & { [K in RAlias]: ToOuterRow<R>}> {
    constructor(public aliases: L, public parent: ut.JoinExpr, public right: SelectExpr<R> | FromExpr<R, RAlias> | Table<R>, public ralias: RAlias){
        super(ut.JoinType.leftOuter, aliases, parent, right, ralias);
    }
}

export class RightOuterJoinBuilder<L, R, RAlias extends string> extends 
JoinBuilder<
    L, 
    R, 
    RAlias, 
    L & { [K in RAlias]: Row<R>}, 
    { [K in keyof L]: { [P in keyof L[K]]: L[K][P] extends ColumnExpr<infer U> ? ColumnExpr<U | null> : never } } 
        & { [K in RAlias]: Row<R>}
    > {
    constructor(public aliases: L, public parent: ut.JoinExpr, public right: SelectExpr<R> | FromExpr<R, RAlias> | Table<R>, public ralias: RAlias){
        super(ut.JoinType.rightOuter, aliases, parent, right, ralias);
    }
}

export class JoinExpr<T> extends TypedExpr<T> {
    constructor(protected aliases: T, public expr: ut.JoinExpr){
        super(expr);
    }

    join<R, RAlias extends string>(right: SelectExpr<R>| FromExpr<R, RAlias>  | Table<R>, alias: RAlias): InnerJoinBuilder<T, R, RAlias> {
        return this.innerJoin(right, alias);
    }

    innerJoin<R, RAlias extends string>(right: SelectExpr<R> | FromExpr<R, RAlias> | Table<R>, alias: RAlias): InnerJoinBuilder<T, R, RAlias> {
        return new InnerJoinBuilder<T, R, RAlias>(this.aliases, this.expr, right, alias);
    }

    leftOuterJoin<R, RAlias extends string>(right: SelectExpr<R> | FromExpr<R, RAlias> | Table<R>, alias: RAlias): LeftOuterJoinBuilder<T, R, RAlias> {
        return new LeftOuterJoinBuilder<T, R, RAlias>(this.aliases, this.expr, right, alias);
    }

    rightOuterJoin<R, RAlias extends string>(right: SelectExpr<R> | FromExpr<R, RAlias> | Table<R>, alias: RAlias): RightOuterJoinBuilder<T, R, RAlias> {
        return new RightOuterJoinBuilder<T, R, RAlias>(this.aliases, this.expr, right, alias);
    }

    

    select<Projection>(func: (r: T) => Row<Projection>): 
        SelectExpr<Projection>
    {
        const row = func(this.aliases);

        const projection = GetProjectionExprFromRow(row);

        const orderBy = this.expr.orderBy
        const where = this.expr.where
        const groupBy = this.expr.groupBy
        const join = new ut.JoinExpr({
            ...this.expr,
            where: undefined,
            orderBy: undefined,
            groupBy: undefined
        })

        const select = new ut.SelectStatementExpr({ 
                        projection: projection, 
                        from: join, 
                        where: where,
                        groupBy: groupBy,
                        orderBy: orderBy 
                    });

        return new SelectExpr<Projection>(row, select);
    }

    where(func: (t: T) => PredicateExpr<T>): JoinExpr<T> {

        const pred = func(this.aliases);

        const where = ut.isWhereExpr(this.expr.where) ? 
                            new ut.WhereExpr(new ut.AndExpr(this.expr.where.clause, pred.expr))
                            : new ut.WhereExpr(pred.expr);

        const joinExpr = new ut.JoinExpr({
            ...this.expr,
            where: where
        });
        
        return new JoinExpr<T>(this.aliases, joinExpr);
    }

    orderBy(func: (t: T) => ColumnExpr<ColumnType>, direction?: 'ASC' | 'DESC'): JoinOrderByExpr<T> {
        const field = func(this.aliases);
        const orderBy = new ut.OrderByExpr(field.expr, direction || 'ASC')
        
        const join = new ut.JoinExpr({
            ...this.expr,
            orderBy: orderBy
        })

        return new JoinOrderByExpr<T>(this.aliases, join);
    }

    orderByDesc(func: (t: T) => ColumnExpr<ColumnType>): JoinOrderByExpr<T> {
        return this.orderBy(func, 'DESC');
    }

    groupBy<G>(func: (t: T) => Row<G>): GroupByExpr<G> {
        const row = func(this.aliases);
        const projection = GetProjectionExprFromRow(row);

        const join = new ut.JoinExpr(
            {
                ...this.expr,
                groupBy: new ut.GroupByExpr(projection)
            });

        return new GroupByExpr<G>(row, join)
    }
}

export class JoinOrderByExpr<T> extends JoinExpr<T> {
    constructor(aliases: T, expr: ut.JoinExpr){
        super(aliases, expr);
    }

    thenBy(func: (t: T) => ColumnExpr<ColumnType>, direction?: 'ASC' | 'DESC'): JoinOrderByExpr<T> {
        const field = func(this.aliases);
        const orderBy = new ut.OrderByExpr(field.expr, direction || 'ASC', this.expr.orderBy)
        const join = new ut.JoinExpr({
            ...this.expr,
            orderBy: orderBy
        });

        return new JoinOrderByExpr<T>(this.aliases, join);
    }

    thenByDesc(func: (t: T) => ColumnExpr<ColumnType>): JoinOrderByExpr<T> {
        return this.thenBy(func, 'DESC');
    }
}

export class GroupByExpr<T> extends TypedExpr<T> {
    constructor(public row: Row<T>, expr: ut.SelectStatementExpr | ut.JoinExpr){
        super(expr)
    }

    select<Projection>(func: (t: AggregateRow<T>) => AggregateRow<Projection>): 
        SelectExpr<Projection>
    {
        const aggRow = toAggregateRow(this.row);
        const row = func(aggRow);
        const projection = GetProjectionExprFromRow(row);

        const select = (() => {
            if(ut.isSelectStatementExpr(this.expr)){
                return new ut.SelectStatementExpr({ ...this.expr, projection: projection });
            }

            if(ut.isJoinExpr(this.expr)){
                return new ut.SelectStatementExpr({ projection: projection, from: this.expr });
            }

            throw new Error('GroupBy only supports selects and join expressions');
        })();

        return new SelectExpr<Projection>(row as any, select);
    }
}

export class FromExpr<T, Talias extends string> extends TypedExpr<T> {
    constructor(
        public source: SelectExpr<T> | Table<T>, 
        public alias: Talias, 
        public expr: ut.FromSelectExpr | ut.FromExpr) {
        super(expr)
    }

    where<P extends Row<T>>(func: (t: Row<P>) => PredicateExpr<T>): SelectExpr<P> {
        return this.selectAll<P>().where(t => func(t));
    }

    selectAll<P extends Row<T>>() {
        return this.select<P>((p: any) => { return { ...p } });
    }

    select<Projection>(func: (r: Row<T>) => Row<Projection>): 
        SelectExpr<Projection>
    {
        if(this.source instanceof SelectExpr){
            let newRow = GetColumnProjectionsFromRow(this.source.row, this.alias)
            let row = func(newRow as any)
            let projection = GetProjectionExprFromRow(row);
            const select = new ut.SelectStatementExpr( { projection: projection, from: this.expr });

            return new SelectExpr<Projection>(row, select);
        }

        let projectionDefaults = GetColumnProjectionsFromTable<T>(this.source, this.alias)
        let row = func(projectionDefaults as any)
        let projection = GetProjectionExprFromRow(row);
        const select = new ut.SelectStatementExpr({ projection: projection, from: this.expr });

        return new SelectExpr<Projection>(row, select);
    }

    join<R, Ralias extends string>(joinedTable: SelectExpr<R> | Table<R>, ralias: Ralias): InnerJoinBuilderStart<T, Talias, R, Ralias> {
        return this.innerJoin(joinedTable, ralias);
    }

    innerJoin<R, Ralias extends string>(joinedTable: SelectExpr<R> | Table<R>, ralias: Ralias): InnerJoinBuilderStart<T, Talias, R, Ralias> {
        return new InnerJoinBuilderStart<T, Talias, R, Ralias>(this, this.alias, joinedTable, ralias);
    }

    leftOuterJoin<R, Ralias extends string>(joinedTable: SelectExpr<R> | Table<R>, ralias: Ralias): LeftOuterJoinBuilderStart<T, Talias, R, Ralias> {
        return new LeftOuterJoinBuilderStart<T, Talias, R, Ralias>(this, this.alias, joinedTable, ralias);
    }

    rightOuterJoin<R, Ralias extends string>(joinedTable: SelectExpr<R> | Table<R>, ralias: Ralias): RightOuterJoinBuilderStart<T, Talias, R, Ralias> {
        return new RightOuterJoinBuilderStart<T, Talias, R, Ralias>(this, this.alias, joinedTable, ralias);
    }
}

export function from<T, Talias extends string >(source: SelectExpr<T> | Table<T>, alias: Talias): FromExpr<T, Talias> {
    if(source instanceof SelectExpr){
        return new FromExpr<T, Talias>(source, alias, new ut.FromSelectExpr(source.expr, alias));
    }
    
    let tableName = getTableFromType(source);

    return new FromExpr<T, Talias>(source, alias, new ut.FromExpr(tableName, alias));
}

export class UnionExpr<T> extends TypedExpr<T> {
    constructor(public row: Row<T>, public expr: ut.UnionExpr) {
        super(expr)
    }

    union(select: SelectExpr<T>, all: boolean = false): UnionExpr<T> {
        const union = new ut.UnionExpr(this.expr, select.expr, all)
        return new UnionExpr<T>(this.row, union)
    }

    select(): SelectExpr<T> {
        const projection = GetProjectionExprFromRow(this.row);
        const select = new ut.SelectStatementExpr({
            projection: projection,
            from: this.expr
        })

        return new SelectExpr<T>(this.row, select);
    }
}

export function UNION<T>(select1: SelectExpr<T> | UnionExpr<T>, select2: SelectExpr<T>, all: boolean = false): UnionExpr<T> {
    const union = new ut.UnionExpr(select1.expr, select2.expr, all)
    return new UnionExpr<T>(select1.row, union)
}