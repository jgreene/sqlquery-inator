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

const operators = {
    equals: function<T, C1 extends ColumnType, C2 extends ColumnType>(c1: C1 | ColumnExpr<C1>, c2: C2 | ColumnExpr<C2>): PredicateExpr<T> {
        const left = c1 instanceof ColumnExpr ? c1.expr : val(c1).expr;
        const right = c2 instanceof ColumnExpr ? c2.expr : val(c2).expr;

        const pred = new ut.PredicateExpr(left, ut.PredicateOperator.equals, right)
        
        return new PredicateExpr<T>(pred);
    }
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
}

export class AsExpr<T extends ColumnType, name extends string> extends ColumnExpr<T> {
    constructor(public expr: ut.Expr, public name: name) {
        super(expr)
    }
}

export function as<C extends ColumnType, name extends string>(expr: TypedExpr<C>, alias: name): AsExpr<C, name> {
    return new AsExpr<C, name>(new ut.AsExpr(expr.expr, alias), alias);
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

export class SelectExpr<T> extends TypedExpr<T> {
    constructor(public row: Row<T>, public expr: ut.FromExpr | ut.SelectStatementExpr, public alias?: string | undefined) {
        super(expr)
    }

    where(func: (t: Row<T>) => PredicateExpr<T>): SelectExpr<T> {
        if(!ut.isSelectStatementExpr(this.expr)){
            throw new Error('Invalid select statement detected!');
        }

        const pred = func(this.row);

        const where = ut.isWhereExpr(this.expr.where) ? 
                        new ut.WhereExpr(new ut.AndExpr(this.expr.where.clause, pred.expr))
                        : new ut.WhereExpr(pred.expr);

        let expr = new ut.SelectStatementExpr(this.expr.projection, this.expr.from, where);
        return new SelectExpr<T>(this.row, expr);
    }

    select<Projection>(func: (t: Row<T>) => Row<Projection>, alias?: string | undefined): 
        SelectExpr<Projection>
    {
        
        const newRow = GetColumnProjectionsFromRow(this.row);
        const row = func(newRow);

        const projection = GetProjectionExprFromRow(row);

        const select = new ut.SelectStatementExpr(projection, this.expr, undefined, alias || this.alias);

        return new SelectExpr<Projection>(row, select);
    }
}

abstract class JoinBuilderStart<L, LAlias extends string, R, RAlias extends string, T, TResult> {
    constructor(
        public joinType: ut.JoinType,
        public left: Table<L>,
        public lalias: LAlias,
        public right: Table<R>,
        public ralias: RAlias
    ){}

    on(func: (t: T) => PredicateExpr<any>): JoinExpr<TResult> {
        const aliases: any = {}
        const leftDefaults = GetColumnProjectionsFromTable(this.left, this.lalias);
        const rightDefaults = GetColumnProjectionsFromTable(this.right, this.ralias);

        aliases[this.lalias] = leftDefaults;
        aliases[this.ralias] = rightDefaults;

        const leftTableName = getTableFromType(this.left);
        const rightTableName = getTableFromType(this.right);

        const fromExpr = new ut.FromExpr(leftTableName, this.lalias);

        const predicate = func(aliases as T);
        const joinExpr = new ut.JoinExpr(fromExpr, this.joinType, rightTableName, this.ralias, predicate.expr);

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
        public left: Table<L>,
        public lalias: LAlias,
        public right: Table<R>,
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
        public left: Table<L>,
        public lalias: LAlias,
        public right: Table<R>,
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
        public left: Table<L>,
        public lalias: LAlias,
        public right: Table<R>,
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
        public right: Table<R>, 
        public ralias: RAlias
    ){}

    on(func: (t: T) => PredicateExpr<any>): JoinExpr<TResult> {
        const aliases: any = {};
        Object.assign(aliases, this.aliases);
        const rightDefaults = GetColumnProjectionsFromTable(this.right, this.ralias);
        const rightTableName = getTableFromType(this.right);
        aliases[this.ralias] = rightDefaults;

        const predicate = func(aliases as T);

        const joinExpr = new ut.JoinExpr(this.parent, this.joinType, rightTableName, this.ralias, predicate.expr);

        return new JoinExpr<TResult>(aliases, joinExpr);
    }
}


export class InnerJoinBuilder<L, R, RAlias extends string> extends 
    JoinBuilder<L, R, RAlias, L & { [K in RAlias]: ToRow<R>}, L & { [K in RAlias]: ToRow<R>}> {
    constructor(public aliases: L, public parent: ut.Expr, public right: Table<R>, public ralias: RAlias){
        super(ut.JoinType.inner, aliases, parent, right, ralias);
    }
}

export class LeftOuterJoinBuilder<L, R, RAlias extends string> extends 
JoinBuilder<L, R, RAlias, L & { [K in RAlias]: ToRow<R>}, L & { [K in RAlias]: ToOuterRow<R>}> {
    constructor(public aliases: L, public parent: ut.Expr, public right: Table<R>, public ralias: RAlias){
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
    constructor(public aliases: L, public parent: ut.Expr, public right: Table<R>, public ralias: RAlias){
        super(ut.JoinType.rightOuter, aliases, parent, right, ralias);
    }
}

export class JoinExpr<T> extends TypedExpr<T> {
    constructor(private aliases: T, expr: ut.Expr){
        super(expr);
    }

    join<R, RAlias extends string>(right: Table<R>, alias: RAlias): InnerJoinBuilder<T, R, RAlias> {
        return this.innerJoin(right, alias);
    }

    innerJoin<R, RAlias extends string>(right: Table<R>, alias: RAlias): InnerJoinBuilder<T, R, RAlias> {
        return new InnerJoinBuilder<T, R, RAlias>(this.aliases, this.expr, right, alias);
    }

    leftOuterJoin<R, RAlias extends string>(right: Table<R>, alias: RAlias): LeftOuterJoinBuilder<T, R, RAlias> {
        return new LeftOuterJoinBuilder<T, R, RAlias>(this.aliases, this.expr, right, alias);
    }

    rightOuterJoin<R, RAlias extends string>(right: Table<R>, alias: RAlias): RightOuterJoinBuilder<T, R, RAlias> {
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

    join<R, Ralias extends string>(joinedTable: Table<R>, ralias: Ralias): InnerJoinBuilderStart<T, Talias, R, Ralias> {
        return this.innerJoin(joinedTable, ralias);
    }

    innerJoin<R, Ralias extends string>(joinedTable: Table<R>, ralias: Ralias): InnerJoinBuilderStart<T, Talias, R, Ralias> {
        return new InnerJoinBuilderStart<T, Talias, R, Ralias>(this.table, this.alias, joinedTable, ralias);
    }

    leftOuterJoin<R, Ralias extends string>(joinedTable: Table<R>, ralias: Ralias): LeftOuterJoinBuilderStart<T, Talias, R, Ralias> {
        return new LeftOuterJoinBuilderStart<T, Talias, R, Ralias>(this.table, this.alias, joinedTable, ralias);
    }

    rightOuterJoin<R, Ralias extends string>(joinedTable: Table<R>, ralias: Ralias): RightOuterJoinBuilderStart<T, Talias, R, Ralias> {
        return new RightOuterJoinBuilderStart<T, Talias, R, Ralias>(this.table, this.alias, joinedTable, ralias);
    }
}

export function from<T, Talias extends string >(ctor: Table<T>, alias: Talias): FromExpr<T, Talias> {
    let tableName = getTableFromType(ctor);

    return new FromExpr<T, Talias>(ctor, alias, new ut.FromExpr(tableName, alias));
}

type QueryContext = {
    aliases: { [key: string]: string }
}

function GetNextAlias(alias: string): string {
    if(alias.length === 1){
        alias = alias + '1';
        return alias;
    }

    const start = alias.substr(0, 1);
    const rest = alias.substr(1, alias.length - 2);
    const id = parseInt(rest, 10) || 2;
    const newId = id + 1;
    return start + (newId.toString())
}

function GetAliasFromTableName(tableName: string) {
    const parts = tableName.split('.')
    const last = parts[parts.length - 1];
    return last.substring(0, 1).toLowerCase()
}

function insertAlias(tableName: string, ctx: QueryContext, alias?: string | undefined): string {
    alias = alias || GetAliasFromTableName(tableName);
    if(ctx.aliases[alias]){
        return insertAlias(tableName, ctx, GetNextAlias(alias))
    }

    ctx.aliases[alias] = tableName;
    return alias;
}





