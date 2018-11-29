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

    if(comparison === ut.PredicateOperator.like) {
        return 'like'
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

function toBracketedTableName(tableName: string) {
    const parts = tableName.split('.')
    return parts.map(p => `[${p}]`).join('.')
}

function GetFromSql(expr: ut.Expr, ctx: Context): string {
    if(!ut.isFromExpr(expr)){
        throw new Error('Not a from expression!');
    }

    if(ctx.getTableSchema){
        const table = ctx.getTableSchema(expr.tableName);
        const alias = mapTableAlias(ctx, expr.alias);
        return `from [${table.name.schema}].[${table.name.name}] as ${alias}`
    }
    
    //UNSAFE BRANCH
    return `from ${toBracketedTableName(expr.tableName)} as ${expr.alias}`
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

    throw new Error('Invalid join type: ' + JSON.stringify(joinType, null, 2));
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

    const alias = ctx.getTableSchema ? mapTableAlias(ctx, expr.alias) : expr.alias;

    const predicateSql = toSql(expr.on, ctx);
    const where = expr.where ? '\n' + toSql(expr.where, ctx) : '';
    //const orderBy = expr.orderBy ? '\norder by ' + toSql(expr.orderBy, ctx) : '';
    const groupBy = expr.groupBy ? '\n' + toSql(expr.groupBy, ctx) : '';

    
    
    return `${parentSql}\n${joinTypeSql} ${joinSourceSql} as ${alias} on ${predicateSql}${where}${groupBy}`
}

function GetProjectionSql(expr: ut.Expr, ctx: Context, tableAlias?: string | undefined) {
    if(!ut.isProjectionExpr(expr)) {
        throw new Error('not a projection expression!');
    }

    tableAlias = tableAlias && ctx.getTableSchema ? mapTableAlias(ctx, tableAlias) : tableAlias;

    const projections = expr.projections.map(p => {
        const projectionSql = toSql(p, ctx);
        return indent(tableAlias ? `${tableAlias}.${projectionSql}` : projectionSql, ctx)
    }).join(',\n');

    return projections;
}

function GetSelectSql(expr: ut.Expr, parentCtx: Context): string {
    if(!ut.isSelectStatementExpr(expr)) {
        throw new Error('Not a select expression!');
    }
    const ctx: Context = {
        parameters: parentCtx.parameters,
        columnAliasCount: parentCtx.columnAliasCount,
        column_aliases: parentCtx.column_aliases,
        tableAliasCount: 0,
        table_aliases: {},
        field_ctx: GetNewFieldContext(),
        indent_level: parentCtx.indent_level,
        getTableSchema: parentCtx.getTableSchema
    };

    const result = GetSelectSqlInternal(expr, ctx);
    parentCtx.field_ctx = ctx.field_ctx;
    return result;
}

function GetSelectSqlInternal(expr: ut.Expr, ctx: Context): string {
    if(!ut.isSelectStatementExpr(expr)) {
        throw new Error('Not a select expression!');
    }

    const top = expr.take ? ` top (${parseInt(expr.take.take.toString())})` : '';
    const distinct = expr.distinct === true ? ` distinct` : '';
    const alias = expr.alias && ctx.getTableSchema ? mapTableAlias(ctx, expr.alias) : expr.alias || insertAndGetTableAlias(ctx);
    const isFromChildSelect = ut.isSelectStatementExpr(expr.from) || ut.isUnionExpr(expr.from);
    const from = expr.from ? toSql(expr.from, ctx) : '';
    const projections = GetProjectionSql(expr.projection, increaseIndent(ctx), expr.alias);
    const where = expr.where ? '\n' + toSql(expr.where, ctx) : '';
    const groupBy = expr.groupBy ? '\n' + toSql(expr.groupBy, ctx) : '';
    const orderBy = expr.orderBy ? '\norder by ' + toSql(expr.orderBy, ctx) : '';

    const select = `select${distinct}${top}\n`;
    
    if(!isFromChildSelect){
        const selectFrom = (from.length > 0 ? '\n' + from : '');
        return `${select}${projections}${selectFrom}${where}${groupBy}${orderBy}`;
    }

    const innerCtx = increaseIndent(ctx);
    const sourceSql = indent(from, innerCtx);
    
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

export function toSql(expr: ut.Expr | undefined, ctx: Context): string {
    if(expr === undefined){
        return ''
    }

    if(ut.isUnionExpr(expr)){
        const newCtx = increaseIndent(ctx);
        const select1 = indent(toSql(expr.select1, ctx), newCtx);
        const union = expr.all ? 'union all' : 'union'
        const select2 = indent(toSql(expr.select2, ctx), newCtx);

        return `${select1}\n${union}\n${select2}`
    }

    if(ut.isFromSelectExpr(expr)){
        const newCtx = increaseIndent(ctx);
        const select = indent(toSql(expr.expr, ctx), newCtx);
        if(ctx.getTableSchema){
            const alias = mapTableAlias(ctx, expr.alias)
            return `from (\n${select}\n) as ${alias}`
        }

        //UNSAFE BRANCH
        return `from (\n${select}\n) as ${expr.alias}`
    }

    if(ut.isTableReferenceExpr(expr)) {
        if(ctx.getTableSchema){
            const table = ctx.getTableSchema(expr.tableName);
            return `[${table.name.schema}].[${table.name.name}]`;
        }
        
        //UNSAFE BRANCH
        return toBracketedTableName(expr.tableName);
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

        if(ctx.getTableSchema && ut.isValidFunction(expr.name) !== true){
            throw new Error('Invalid aggregate function: ' + expr.name);
        }

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

    if(ut.isScalarFunctionExpr(expr)) {
        if(ctx.getTableSchema && !ut.isValidFunction(expr.name)){
            throw new Error('Invalid scalar function: ' + expr.name)
        }
        
        const args = expr.args.map(a => toSql(a, ctx)).join(', ')
        return `${expr.name}(${args})`
    }

    if(ut.isOperatorExpr(expr)){
        if(ctx.getTableSchema && !ut.isValidOperator(expr.name)){
            throw new Error('Invalid operator: ' + expr.name)
        }

        const left = toSql(expr.left, ctx)
        const operator = expr.name
        const right = toSql(expr.right, ctx)
        return `${left} ${operator} ${right}`
    }

    if(ut.isValueExpr(expr)) {
        const parameter = query.addParameter(ctx.parameters, { name: 'v', value: expr.value})

        return `@${parameter.name}`;
    }

    if(ut.isAsExpr(expr)) {
        const left = toSql(expr.left, ctx);
        if(ut.isColumnExpr(expr.left) && expr.left.columnName === expr.alias){
            return left;
        }

        if(ut.isFieldExpr(expr.left) && expr.left.name === expr.alias){
            return left;
        }

        if(ctx.getTableSchema){
            const alias = mapColumnAlias(ctx, expr.alias)
            ctx.field_ctx.addValidField(alias);
            return `(${left}) as '${alias}'`;
        }

        //UNSAFE BRANCH
        const alias = expr.alias;
        return `(${left}) as '${alias}'`;
    }

    if(ut.isColumnExpr(expr)){
        if(ctx.getTableSchema){
            const table = ctx.getTableSchema(expr.tableName);
            const column = table.columns.find(c => c.name === expr.columnName);
            if(column === undefined)
            {
                throw new Error(`Could not find column ${expr.columnName} in ${expr.tableName}!`);
            }

            ctx.field_ctx.addValidField(column.name);
            
            const tableAlias = expr.alias ? mapTableAlias(ctx, expr.alias) : undefined;
            return tableAlias ? `${tableAlias}.[${column.name}]` : `[${column.name}]`;
        }

        //UNSAFE BRANCH
        const tableAlias = expr.alias;
        return tableAlias ? `${tableAlias}.[${expr.columnName}]` : `[${expr.columnName}]`;
    }

    if(ut.isFieldExpr(expr)) {
        if(ctx.getTableSchema){
            const tableAlias = expr.alias ? mapTableAlias(ctx, expr.alias) : undefined;
            const name = ctx.field_ctx.isValidField(expr.name) ? expr.name : mapColumnAlias(ctx, expr.name);
            return tableAlias ? `${tableAlias}.[${name}]` : `[${name}]`;
        }

        //UNSAFE BRANCH
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

type ValidFieldContext = {
    valid_fields: string[],
    addValidField: (fieldName: string) => void,
    isValidField: (fieldName: string) => boolean,
    ensureIsValidField: (fieldName: string) => void
}

function GetNewFieldContext(): ValidFieldContext {
    return {
        valid_fields: [],
        addValidField(fieldName: string) {
            if(this.isValidField(fieldName)){
                return;
            }

            this.valid_fields.push(fieldName);
        },
        isValidField(fieldName: string) {
            return this.valid_fields.indexOf(fieldName) !== -1
        },
        ensureIsValidField(fieldName: string){
            if(this.isValidField(fieldName) !== true){
                throw new Error('Invalid field: ' + fieldName);
            }
        }
    }
}

export type Context = {
    parameters: query.SqlParameters,
    columnAliasCount: number,
    column_aliases: { [key: string]: string },
    tableAliasCount: number,
    table_aliases: { [key: string]: string },
    field_ctx: ValidFieldContext,
    indent_level: number,
    getTableSchema?: ((tableName: string) => TableSchema) | undefined
}

function increaseIndent(ctx: Context): Context {
    const newCtx = Object.assign({}, ctx)
    newCtx.indent_level = newCtx.indent_level + 1;
    return newCtx;
}

export type QueryOptions = {
    allowSqlInjection: boolean
    getTableSchema?: ((tableName: string) => TableSchema) | undefined
}

export function createContext(options: QueryOptions): Context {
    if(options.allowSqlInjection !== true && options.getTableSchema === undefined){
        throw new Error('getTableSchema must be defined when allowSqlInjection is not true!')
    }

    const ctx: Context = {
        parameters: {},
        columnAliasCount: 0,
        column_aliases: {},
        tableAliasCount: 0,
        table_aliases: {},
        field_ctx: GetNewFieldContext(),
        indent_level: 0,
        getTableSchema: options.getTableSchema
    };

    return ctx;
}

export function toQuery(expr: ut.SelectStatementExpr, options: QueryOptions): query.SqlQuery {
    const ctx = createContext(options);
    const sql = toSql(expr, ctx);

    return {
        sql: sql,
        column_aliases: ctx.column_aliases,
        parameters: ctx.parameters
    }
}

function mapColumnAlias(ctx: Context, alias: string): string {
    const mappedAlias = ctx.column_aliases[alias];
    if(mappedAlias !== undefined){
        return mappedAlias;
    }

    ctx.columnAliasCount = ctx.columnAliasCount + 1;
    const alias_id = 'ca' + ctx.columnAliasCount;
    ctx.column_aliases[alias] = alias_id;
    return alias_id;
}

function mapTableAlias(ctx: Context, alias: string): string {
    const mappedAlias = ctx.table_aliases[alias]
    if(mappedAlias !== undefined){
        return mappedAlias;
    }

    const newAlias = insertAndGetTableAlias(ctx);
    ctx.table_aliases[alias] = newAlias;
    return newAlias;
}

function insertAndGetTableAlias(ctx: Context): string {
    ctx.tableAliasCount = ctx.tableAliasCount + 1;
    const alias_id = 'ta' + ctx.tableAliasCount;
    ctx.table_aliases[alias_id] = alias_id;
    return alias_id;
}