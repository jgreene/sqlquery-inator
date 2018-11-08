import * as ut from './untyped_ast'
import * as query from './sqlquery'
import { DBSchema, TableSchema, ColumnSchema } from 'dbschema-inator'

function predicateOperatorToSql(comparison: ut.PredicateOperator): string {
    if(!ut.isValidPredicateOperator(comparison))
    {
        throw new Error(`Invalid comparison found: ${comparison}`);
    }

    if(comparison == ut.PredicateOperator.equals){
        return '='
    }

    if(comparison == ut.PredicateOperator.notEquals){
        return '<>'
    }

    if(comparison == ut.PredicateOperator.greaterThan){
        return '>'
    }

    if(comparison == ut.PredicateOperator.lessThan){
        return '<'
    }

    if(comparison == ut.PredicateOperator.greaterThanOrEquals){
        return '>='
    }

    if(comparison == ut.PredicateOperator.lessThanOrEquals){
        return '<='
    }

    if(comparison == ut.PredicateOperator.isNull){
        return 'is null'
    }

    if(comparison == ut.PredicateOperator.isNotNull){
        return 'is not null'
    }

    throw new Error(`Could not map ${comparison} to sql operator!`);
}

function indent(input: string, ctx: Context): string {
    const level = ctx.indent_level;
    const parts = input.split('\n')
    if(parts.length > 1){
        return parts.map(p => indent(p, ctx)).join('\n');
    }
    return ' '.repeat(4 * level) + parts[0];
}

function GetFromSql(expr: ut.Expr, ctx: Context): string {
    if(!ut.isFromExpr(expr)){
        throw new Error('Not a from expression!');
    }
    const table = ctx.getTable(expr.tableName);
    return `from [${table.name.db_name}].[${table.name.schema}].[${table.name.name}] as ${expr.alias}`
}

function GetJoinTypeSql(joinType: ut.JoinType) {
    if(joinType === "inner"){
        return 'join'
    }

    if(joinType === "leftOuter"){
        return 'left outer join'
    }

    if(joinType === 'rightOuter'){
        return 'right outer join'
    }

    throw new Error('Invalid join type!');
}

function GetJoinSql(expr: ut.Expr, ctx: Context): string {
    if(!ut.isJoinExpr(expr)){
        throw new Error('Not a join expression!');
    }

    if(!ut.isValidJoinType(expr.joinType)){
        throw new Error('Not a valid join type!');
    }

    const parentSql = toSql(expr.parent, ctx);

    const joinTypeSql = indent(GetJoinTypeSql(expr.joinType), ctx);

    const joinSourceSql = (() => {
        if(ut.isSelectStatementExpr(expr.joinSource)){
            const innerCtx = increaseIndent(ctx);
            const source = indent(toSql(expr.joinSource, ctx), innerCtx);
            return `(\n${source}\n)`
        }

        return toSql(expr.joinSource, ctx)
    })();

    const predicateSql = toSql(expr.on, ctx);
    const where = expr.where ? '\n' + toSql(expr.where, ctx) : '';
    const orderBy = expr.orderBy ? '\norder by ' + toSql(expr.orderBy, ctx) : '';
    const groupBy = expr.groupBy ? '\n' + toSql(expr.groupBy, ctx) : '';
    
    return `${parentSql}\n${joinTypeSql} ${joinSourceSql} as ${expr.alias} on ${predicateSql}${where}${groupBy}`
}

function GetProjectionSql(expr: ut.Expr, ctx: Context, alias?: string | undefined) {
    if(!ut.isProjectionExpr(expr)) {
        throw new Error('not a projection expression!');
    }

    const projections = expr.projections.map(p => indent(alias ? `${alias}.${toSql(p, ctx)}` : toSql(p, ctx), ctx)).join(',\n')
    return projections;
}

function GetSelectSql(expr: ut.Expr, parentCtx: Context): string {
    if(!ut.isSelectStatementExpr(expr)) {
        throw new Error('Not a select expression!');
    }
    const ctx = { 
        parameters: parentCtx.parameters, 
        aliasCount: 0, 
        column_aliases: {}, 
        tableAliasCount: 0, 
        table_aliases: {}, 
        indent_level: parentCtx.indent_level,
        getTable: parentCtx.getTable
    };

    return GetSelectSqlInternal(expr, ctx);
}

function GetSelectSqlInternal(expr: ut.Expr, ctx: Context): string {
    if(!ut.isSelectStatementExpr(expr)) {
        throw new Error('Not a select expression!');
    }

    const top = expr.take ? ` top (${parseInt(expr.take.take.toString())})` : '';
    const distinct = expr.distinct === true ? ` distinct` : '';

    const alias = expr.alias || getTableAlias(ctx);
    const projections = GetProjectionSql(expr.projection, increaseIndent(ctx), expr.alias);
    const isFromChildSelect = ut.isSelectStatementExpr(expr.from)
    const where = expr.where ? '\n' + toSql(expr.where, ctx) : '';
    const groupBy = expr.groupBy ? '\n' + toSql(expr.groupBy, ctx) : '';
    const orderBy = expr.orderBy ? '\norder by ' + toSql(expr.orderBy, ctx) : ''

    const select = `select${distinct}${top}\n`;
    
    if(!isFromChildSelect){
        const from = expr.from ? '\n' + toSql(expr.from, ctx) : '';
        return `${select}${projections}${from}${where}${groupBy}${orderBy}`;
    }

    const innerCtx = increaseIndent(ctx);
    const sourceSql = indent(toSql(expr.from, ctx), innerCtx);
    
    const source = `(\n${sourceSql}\n) as ${alias}`
    return `${select}${projections}\nfrom ${source}${where}${groupBy}${orderBy}`;
}

function GetOrderBySql(expr: ut.Expr | undefined, ctx: Context): string {
    if(!ut.isOrderByExpr(expr)) {
        throw new Error('Not an OrderBy expression!');
    }

    const direction = expr.direction === 'DESC' ? 'DESC' : 'ASC'
    const column = toSql(expr.field, ctx);
    if(expr.parent){
        const parent = toSql(expr.parent, ctx);
        return `${parent}, ${column} ${direction}`
    }

    return `${column} ${direction}`;
}

function toSql(expr: ut.Expr | undefined, ctx: Context): string {
    if(expr === undefined){
        return ''
    }

    if(ut.isFromSelectExpr(expr)){
        const newCtx = increaseIndent(ctx);
        const select = indent(toSql(expr.expr, ctx), newCtx);
        return `from (\n${select}\n) as ${expr.alias}`
    }

    if(ut.isTableReferenceExpr(expr)) {
        const table = ctx.getTable(expr.tableName);
        return `[${table.name.db_name}].[${table.name.schema}].[${table.name.name}]`;
    }

    if(ut.isGroupByExpr(expr)){
        const projection = GetProjectionSql(expr.projection, ctx);
        return `group by ${projection}`
    }

    if(ut.isOrderByExpr(expr)){
        return GetOrderBySql(expr, ctx);
    }

    if(ut.isFromExpr(expr)) {
        return GetFromSql(expr, ctx);
    }

    if(ut.isAggregateFunctionExpr(expr)) {
        const arg = toSql(expr.arg, ctx);
        const distinct = expr.distinct ? 'DISTINCT ' : ''

        return `${expr.name}(${distinct}${arg})`;
    }

    if(ut.isRowNumberExpr(expr)) {
        const orderBy = toSql(expr.orderBy, ctx);
        return `ROW_NUMBER() OVER (ORDER BY ${orderBy})`;
    }

    if(ut.isJoinExpr(expr)){
        return GetJoinSql(expr, ctx);
    }

    if(ut.isSelectStatementExpr(expr)) {
        return GetSelectSql(expr, ctx);
    }

    if(ut.isWhereExpr(expr)) {
        const clause = toSql(expr.clause, ctx)
        return `where ${clause}`
    }
    
    if(ut.isAndExpr(expr)) {
        const left = toSql(expr.left, ctx);
        const right = toSql(expr.right, ctx);

        return `(${left} AND ${right})`
    }

    if(ut.isOrExpr(expr)) {
        const left = toSql(expr.left, ctx);
        const right = toSql(expr.right, ctx);

        return `(${left} OR ${right})`
    }

    if(ut.isPredicateExpr(expr)) {
        const left = toSql(expr.left, ctx);
        const right = expr.right ? ' ' + toSql(expr.right, ctx) : '';
        
        const operator = predicateOperatorToSql(expr.operator)
        
        return `${left} ${operator}${right}`;
    }

    if(ut.isProjectionExpr(expr)) {
        return GetProjectionSql(expr, ctx);
    }

    if(ut.isAsExpr(expr)) {
        const left = toSql(expr.left, ctx);
        if(ut.isColumnExpr(expr.left) && expr.left.columnName === expr.alias){
            return left;
        }

        if(ut.isFieldExpr(expr.left) && expr.left.name === expr.alias){
            return left;
        }
        
        //const alias = insertColumnAlias(ctx, expr.alias)
        return `(${left}) as '${expr.alias}'`;
    }

    if(ut.isScalarFunctionExpr(expr)) {
        const args = expr.args.map(a => toSql(a, ctx)).join(', ')
        return `${expr.name}(${args})`
    }

    if(ut.isValueExpr(expr)) {
        const parameter = query.addParameter(ctx.parameters, { name: 'v', value: expr.value})

        return `@${parameter.name}`;
    }

    if(ut.isColumnExpr(expr)){
        const table = ctx.getTable(expr.tableName);
        const column = table.columns.find(c => c.name === expr.columnName);
        if(column === undefined)
        {
            throw new Error(`Could not find column ${expr.columnName} in ${expr.tableName}!`);
        }
        const alias = expr.alias;
        return alias ? `${alias}.[${column.name}]` : `[${column.name}]`;
    }

    if(ut.isFieldExpr(expr)) {
        const alias = expr.alias;
        return alias ? `${alias}.[${expr.name}]` : `[${expr.name}]`;
    }

    if(ut.isNullExpr(expr)){
        return `null`;
    }

    if(ut.isEmptyStringExpr(expr)){
        return `''`;
    }

    if(ut.isStarExpr(expr)) {
        return '*';
    }

    throw new Error('Could not generate query for expr: ' + JSON.stringify(expr, null, 2));
}

const tableCache: { [key: string]: TableSchema } = {};

function getFindTable(schema: DBSchema) {
    return function(tableName: string) {
        const cached = tableCache[tableName];
        if(cached !== undefined){
            return cached;
        }

        const table = schema.tables.find(t => `${t.name.db_name}.${t.name.schema}.${t.name.name}` === tableName);
        if(table === undefined){
            throw new Error(`Could not find table ${tableName} in schema!`);
        }

        tableCache[tableName] = table;
        return table;
    }
}

function insertColumnAlias(ctx: Context, alias: string): string {
    ctx.aliasCount = ctx.aliasCount + 1;
    const alias_id = 'alias' + ctx.aliasCount;
    ctx.column_aliases[alias_id] = alias;
    return alias_id;
}

function getTableAlias(ctx: Context): string {
    ctx.tableAliasCount = ctx.tableAliasCount + 1;
    const alias_id = 'ta' + ctx.tableAliasCount;
    ctx.table_aliases[alias_id] = alias_id;
    return alias_id;
}

type Context = {
    parameters: query.SqlParameters,
    aliasCount: number,
    column_aliases: { [key: string]: string },
    tableAliasCount: number,
    table_aliases: { [key: string]: string },
    indent_level: number,
    getTable: (tableName: string) => TableSchema
}

function increaseIndent(ctx: Context): Context {
    const newCtx = Object.assign({}, ctx)
    newCtx.indent_level = newCtx.indent_level + 1;
    return newCtx;
}

export function toQuery(schema: DBSchema, expr: ut.Expr): query.SqlQuery {
    const ctx = { 
        parameters: {}, 
        aliasCount: 0, 
        column_aliases: {}, 
        tableAliasCount: 0, 
        table_aliases: {}, 
        indent_level: 0,
        getTable: getFindTable(schema) 
    };
    const sql = toSql(expr, ctx);

    return {
        sql: sql,
        column_aliases: ctx.column_aliases,
        parameters: ctx.parameters
    }

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