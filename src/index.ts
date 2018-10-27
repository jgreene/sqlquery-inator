import * as ut from './untyped_ast'
import * as t from 'io-ts'
import * as tdc from 'io-ts-derive-class'

export type ColumnType = ut.ColumnType

type Constructor<T> = new (...args: any[]) => T
type Table<T> = Constructor<tdc.ITyped<any, T, any, t.mixed>>

type ToRow<T> = {
    [P in keyof T]: T[P] extends ColumnType ? ColumnExpr<T[P]> : never
}

type ToOuterRow<T> = {
    [P in keyof T]: T[P] extends ColumnType ? ColumnExpr<T[P] | null> : never
}

type FromRow<T> = {
    [P in keyof T]: T[P] extends ColumnExpr<infer U> ? U : never
}

type Row<T> = {
    [P in keyof T]: T[P] extends ColumnExpr<infer U> ? T[P] : never
}

type OrderBy<T> = {
    [P in keyof T]?: T[P] extends OrderColumnExpr<ColumnType> ? T[P] : never
}

const tableNameMap: any = {}

export function registerTable<T>(ctor: Constructor<T>, tableName: string) {
    tableNameMap[ctor as any] = tableName;
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
}

abstract class TypedExpr<T> {
    constructor(public expr: ut.Expr) {}
}

export class ColumnExpr<C extends ColumnType> extends TypedExpr<C> {
    constructor(public expr: ut.Expr) {
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

    get asc(): OrderColumnExpr<C> {
        return new OrderColumnExpr<C>(new ut.OrderByExpr(this.expr, 'ASC'))
    }

    get desc(): OrderColumnExpr<C> {
        return new OrderColumnExpr<C>(new ut.OrderByExpr(this.expr, 'DESC'))
    }
}

export class OrderColumnExpr<C extends ColumnType> extends TypedExpr<C> {
    constructor(public expr: ut.OrderByExpr) {
        super(expr)
    }
}

export class WindowFunctionColumnExpr<C extends ColumnType> extends ColumnExpr<C> {
    constructor(expr: ut.Expr){
        super(expr)
    }
}

export class AsExpr<T extends ColumnType, name extends string> extends ColumnExpr<T> {
    constructor(public column: ColumnExpr<T>, public name: name) {
        super(new ut.AsExpr(column.expr, name))
    }
}

export function as<C extends ColumnType, name extends string>(expr: ColumnExpr<C>, alias: name): AsExpr<C, name> {
    return new AsExpr<C, name>(expr, alias);
}

export class ValueExpr<T extends ColumnType> extends ColumnExpr<T> {
    constructor(public expr: ut.Expr) {
        super(expr)
    }
}

export function val<T extends ColumnType>(value: T): ValueExpr<T> {
    const expr = value === '' ? new ut.EmptyStringExpr() : value === null ? new ut.NullExpr : new ut.ValueExpr(value)

    return new ValueExpr(expr);
}

export function ISNULL<C extends ColumnType>(expr: TypedExpr<C | null>, value: ValueExpr<Exclude<C, null>>): ColumnExpr<Exclude<C, null>> {
    return new ColumnExpr(new ut.ScalarFunctionExpr('ISNULL', [expr.expr, value.expr]))
}

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

export function ROW_NUMBER<T>(ordering: OrderColumnExpr<any>[]): WindowFunctionColumnExpr<number> {
    const expr = getOrderByExpr(ordering);

    return new WindowFunctionColumnExpr<number>(new ut.RowNumberExpr(expr));
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
        projection[key] = new ColumnExpr<any>(new ut.ColumnExpr(tableName, key, alias));
    })

    return projection;
}

function GetColumnProjectionsFromRow<T>(row: Row<T>, alias?: string | undefined): Row<T> {
    const projection: any = {}
    for(let key in row){
        projection[key] = new ColumnExpr<any>(new ut.FieldExpr(key, alias));
    }

    return projection;
}

function GetProjectionExprFromRow<T>(row: Row<T>): ut.ProjectionExpr {
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
    constructor(expr: ut.Expr) {
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

function isSelectExpr<T>(input: any): input is SelectExpr<T> {
    return !!input && input['_tag'] === 'SelectExpr<T>';
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
    readonly _tag: 'SelectExpr<T>' = 'SelectExpr<T>'
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

            let expr = new ut.SelectStatementExpr(
                            this.expr.projection, 
                            this.expr.from, 
                            where,
                            this.expr.alias,
                            this.expr.orderBy,
                            this.expr.take
                       );
            return new SelectExpr<T>(this.row, expr);
        }

        const newRow = GetColumnProjectionsFromRow(this.row, this.expr.alias);
        const pred = func(newRow);
        const where = new ut.WhereExpr(pred.expr);
        let innerExpr = new ut.SelectStatementExpr(
                            this.expr.projection, 
                            this.expr.from,
                            undefined,
                            this.expr.alias,
                            this.expr.orderBy,
                            undefined
                        );

        const newProjection = GetProjectionExprFromRow(newRow);
        let expr = new ut.SelectStatementExpr(
                            newProjection, 
                            innerExpr, 
                            where,
                            this.expr.alias,
                            undefined,
                            this.expr.take
                        );

        return new SelectExpr<T>(newRow, expr);
    }

    select<Projection>(func: (t: Row<T>) => Row<Projection>, alias?: string | undefined): 
        SelectExpr<Projection>
    {
        
        const newRow = GetColumnProjectionsFromRow(this.row);
        const row = func(newRow);

        const projection = GetProjectionExprFromRow(row);

        const select = new ut.SelectStatementExpr(projection, this.expr, undefined, alias);

        return new SelectExpr<Projection>(row, select);
    }

    orderBy(func: (t: Row<T>) => ColumnExpr<ColumnType>, direction?: 'ASC' | 'DESC'): OrderByExpr<T> {
        
        const field = func(this.row);
        const orderBy = new ut.OrderByExpr(field.expr, direction || 'ASC')
        const select = new ut.SelectStatementExpr(this.expr.projection, this.expr.from, this.expr.where, this.expr.alias, orderBy);

        return new OrderByExpr<T>(this.row, select);
    }

    orderByDesc(func: (t: Row<T>) => ColumnExpr<ColumnType>): OrderByExpr<T> {
        return this.orderBy(func, 'DESC');
    }

    take(num: number): SelectExpr<T> {
        const select = new ut.SelectStatementExpr(
                            this.expr.projection, 
                            this.expr.from, 
                            this.expr.where, 
                            this.expr.alias, 
                            this.expr.orderBy,
                            new ut.TakeExpr(num)
                        );
        return new SelectExpr<T>(this.row, select);
    }
}

export class OrderByExpr<T> extends SelectExpr<T> {
    constructor(row: Row<T>, expr: ut.SelectStatementExpr) {
        super(row, expr)
    }

    thenBy(func: (t: Row<T>) => ColumnExpr<ColumnType>, direction?: 'ASC' | 'DESC'): OrderByExpr<T> {
        
        const field = func(this.row);
        const orderBy = new ut.OrderByExpr(field.expr, direction || 'ASC', this.expr.orderBy)
        const select = new ut.SelectStatementExpr(this.expr.projection, this.expr.from, this.expr.where, this.expr.alias, orderBy);

        return new OrderByExpr<T>(this.row, select);
    }

    thenByDesc(func: (t: Row<T>) => ColumnExpr<ColumnType>): OrderByExpr<T> {
        return this.thenBy(func, 'DESC');
    }
}

abstract class JoinBuilderStart<L, LAlias extends string, R, RAlias extends string, T, TResult> {
    constructor(
        public joinType: ut.JoinType,
        public left: SelectExpr<L> | Table<L>,
        public lalias: LAlias,
        public right: SelectExpr<R> | Table<R>,
        public ralias: RAlias
    ){}

    on(func: (t: T) => PredicateExpr<any>): JoinExpr<TResult> {
        const aliases: any = {}
        const leftDefaults = (() => {
            if(isSelectExpr(this.left)){
                return GetColumnProjectionsFromRow(this.left.row, this.lalias);
            }

            return GetColumnProjectionsFromTable(this.left, this.lalias);
        })();

        const rightDefaults = (() => {
            if(isSelectExpr(this.right)){
                return GetColumnProjectionsFromRow(this.right.row, this.ralias);
            }

            return GetColumnProjectionsFromTable(this.right, this.ralias);
        })();

        aliases[this.lalias] = leftDefaults;
        aliases[this.ralias] = rightDefaults;

        const leftSource = (() => {
            if(isSelectExpr(this.left)){
                return this.left.expr;
            }

            const leftTableName = getTableFromType(this.left);
            return new ut.FromExpr(leftTableName, this.lalias);
        })();

        const rightSource = (() => {
            if(isSelectExpr(this.right)){
                return this.right.expr;
            }

            const rightTableName = getTableFromType(this.right);
            return new ut.TableReferenceExpr(rightTableName);
        })();

        const predicate = func(aliases as T);
        const joinExpr = new ut.JoinExpr(leftSource, this.joinType, rightSource, this.ralias, predicate.expr);

        return new JoinExpr<TResult>(aliases, joinExpr);
    }
}

export class InnerJoinBuilderStart<L, LAlias extends string, R, RAlias extends string> extends 
    JoinBuilderStart<
        L, 
        LAlias, 
        R, 
        RAlias, 
        { [K in LAlias]: ToRow<L> } & { [K in RAlias]: ToRow<R>}, 
        { [K in LAlias]: ToRow<L> } & { [K in RAlias]: ToRow<R>}
    >
{
    constructor(
        public left: SelectExpr<L> | Table<L>,
        public lalias: LAlias,
        public right: SelectExpr<R> | Table<R>,
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
        { [K in LAlias]: ToRow<L> } & { [K in RAlias]: ToRow<R>}, 
        { [K in LAlias]: ToRow<L> } & { [K in RAlias]: ToOuterRow<R>}
    >
{
    constructor(
        public left: SelectExpr<L> | Table<L>,
        public lalias: LAlias,
        public right: SelectExpr<R> | Table<R>,
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
        { [K in LAlias]: ToRow<L> } & { [K in RAlias]: ToRow<R>}, 
        { [K in LAlias]: ToOuterRow<L> } & { [K in RAlias]: ToRow<R>}
    >
{
    constructor(
        public left: SelectExpr<L> | Table<L>,
        public lalias: LAlias,
        public right: SelectExpr<R> | Table<R>,
        public ralias: RAlias
    ){
        super(ut.JoinType.rightOuter, left, lalias, right, ralias);
    }
}

abstract class JoinBuilder<L, R, RAlias extends string, T, TResult> {
    constructor(
        public joinType: ut.JoinType, 
        public aliases: L, 
        public parent: ut.Expr, 
        public right: SelectExpr<R> | Table<R>, 
        public ralias: RAlias
    ){}

    on(func: (t: T) => PredicateExpr<any>): JoinExpr<TResult> {
        const aliases: any = {};
        Object.assign(aliases, this.aliases);
        
        const rightDefaults = (() => {
            if(isSelectExpr(this.right)){
                return GetColumnProjectionsFromRow(this.right.row, this.ralias);
            }

            return GetColumnProjectionsFromTable(this.right, this.ralias);
        })();

        aliases[this.ralias] = rightDefaults;

        const rightSource = (() => {
            if(isSelectExpr(this.right)){
                return this.right.expr;
            }

            const rightTableName = getTableFromType(this.right);
            return new ut.TableReferenceExpr(rightTableName);
        })();

        const predicate = func(aliases as T);

        const joinExpr = new ut.JoinExpr(this.parent, this.joinType, rightSource, this.ralias, predicate.expr);

        return new JoinExpr<TResult>(aliases, joinExpr);
    }
}

export class InnerJoinBuilder<L, R, RAlias extends string> extends 
    JoinBuilder<L, R, RAlias, L & { [K in RAlias]: ToRow<R>}, L & { [K in RAlias]: ToRow<R>}> {
    constructor(public aliases: L, public parent: ut.Expr, public right: SelectExpr<R> | Table<R>, public ralias: RAlias){
        super(ut.JoinType.inner, aliases, parent, right, ralias);
    }
}

export class LeftOuterJoinBuilder<L, R, RAlias extends string> extends 
JoinBuilder<L, R, RAlias, L & { [K in RAlias]: ToRow<R>}, L & { [K in RAlias]: ToOuterRow<R>}> {
    constructor(public aliases: L, public parent: ut.Expr, public right: SelectExpr<R> | Table<R>, public ralias: RAlias){
        super(ut.JoinType.leftOuter, aliases, parent, right, ralias);
    }
}

export class RightOuterJoinBuilder<L, R, RAlias extends string> extends 
JoinBuilder<
    L, 
    R, 
    RAlias, 
    L & { [K in RAlias]: ToRow<R>}, 
    { [K in keyof L]: { [P in keyof L[K]]: L[K][P] extends ColumnExpr<infer U> ? ColumnExpr<U | null> : never } } 
        & { [K in RAlias]: ToRow<R>}
    > {
    constructor(public aliases: L, public parent: ut.Expr, public right: SelectExpr<R> | Table<R>, public ralias: RAlias){
        super(ut.JoinType.rightOuter, aliases, parent, right, ralias);
    }
}

export class JoinExpr<T> extends TypedExpr<T> {
    constructor(private aliases: T, expr: ut.Expr){
        super(expr);
    }

    join<R, RAlias extends string>(right: SelectExpr<R> | Table<R>, alias: RAlias): InnerJoinBuilder<T, R, RAlias> {
        return this.innerJoin(right, alias);
    }

    innerJoin<R, RAlias extends string>(right: SelectExpr<R> | Table<R>, alias: RAlias): InnerJoinBuilder<T, R, RAlias> {
        return new InnerJoinBuilder<T, R, RAlias>(this.aliases, this.expr, right, alias);
    }

    leftOuterJoin<R, RAlias extends string>(right: SelectExpr<R> | Table<R>, alias: RAlias): LeftOuterJoinBuilder<T, R, RAlias> {
        return new LeftOuterJoinBuilder<T, R, RAlias>(this.aliases, this.expr, right, alias);
    }

    rightOuterJoin<R, RAlias extends string>(right: SelectExpr<R> | Table<R>, alias: RAlias): RightOuterJoinBuilder<T, R, RAlias> {
        return new RightOuterJoinBuilder<T, R, RAlias>(this.aliases, this.expr, right, alias);
    }

    select<Projection>(func: (r: T) => Row<Projection>): 
        SelectExpr<Projection>
    {
        const row = func(this.aliases);

        const projection = GetProjectionExprFromRow(row);

        const select = new ut.SelectStatementExpr(projection, this.expr);

        return new SelectExpr<Projection>(row, select);
    }
}

export class FromExpr<T, Talias extends string> extends TypedExpr<T> {
    constructor(public table: Table<T>, public alias: Talias, public expr: ut.FromExpr) {
        super(expr)
    }

    selectAll<P extends ToRow<T>>() {
        return this.select<P>((p: any) => { return { ...p } });
    }

    select<Projection>(func: (r: ToRow<T>) => Row<Projection>): 
        SelectExpr<Projection>
    {
        let projectionDefaults = GetColumnProjectionsFromTable<T>(this.table, this.alias)

        let row = func(projectionDefaults as any)

        let projection = GetProjectionExprFromRow(row);

        const select = new ut.SelectStatementExpr(projection, this.expr);

        return new SelectExpr<Projection>(row, select);
    }

    join<R, Ralias extends string>(joinedTable: SelectExpr<R> | Table<R>, ralias: Ralias): InnerJoinBuilderStart<T, Talias, R, Ralias> {
        return this.innerJoin(joinedTable, ralias);
    }

    innerJoin<R, Ralias extends string>(joinedTable: SelectExpr<R> | Table<R>, ralias: Ralias): InnerJoinBuilderStart<T, Talias, R, Ralias> {
        return new InnerJoinBuilderStart<T, Talias, R, Ralias>(this.table, this.alias, joinedTable, ralias);
    }

    leftOuterJoin<R, Ralias extends string>(joinedTable: SelectExpr<R> | Table<R>, ralias: Ralias): LeftOuterJoinBuilderStart<T, Talias, R, Ralias> {
        return new LeftOuterJoinBuilderStart<T, Talias, R, Ralias>(this.table, this.alias, joinedTable, ralias);
    }

    rightOuterJoin<R, Ralias extends string>(joinedTable: SelectExpr<R> | Table<R>, ralias: Ralias): RightOuterJoinBuilderStart<T, Talias, R, Ralias> {
        return new RightOuterJoinBuilderStart<T, Talias, R, Ralias>(this.table, this.alias, joinedTable, ralias);
    }
}

export function from<T, Talias extends string >(ctor: Table<T>, alias: Talias): FromExpr<T, Talias> {
    let tableName = getTableFromType(ctor);

    return new FromExpr<T, Talias>(ctor, alias, new ut.FromExpr(tableName, alias));
}