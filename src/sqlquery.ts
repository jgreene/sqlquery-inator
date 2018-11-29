
export type SqlParameter = {
    name: string,
    value: any
}

export type SqlParameters = {
    [key: string]: SqlParameter
}

export type SqlQuery = {
    sql: string,
    column_aliases: { [key: string]: string },
    parameters: SqlParameters
}

function swapAliasMap(map: { [key: string]: string }): { [key: string]: string } {
    const result: { [key: string]: string } = {}

    const keys = Object.keys(map);
    keys.forEach(key => {
        const value = map[key];
        result[value] = key;
    });
    return result;
}

export function hydrateResults(query: SqlQuery, results: any[]): any[] {
    const aliasKeys = Object.keys(query.column_aliases)
    if(aliasKeys.length < 1){
        return results;
    }

    const aliases = swapAliasMap(query.column_aliases)

    return results.map(r => {
        const result: any = {}

        Object.keys(r).forEach(key => {
            const alias = aliases[key]
            if(alias === undefined){
                result[key] = r[key]
            }
            else {
                result[alias] = r[key]
            }
        })
        return result;
    });
}

export function addParameter(parameters: SqlParameters, parameter: SqlParameter): SqlParameter {
    const current = parameters[parameter.name];
    if(current === undefined){
        parameters[parameter.name] = parameter;
        return parameter;
    }

    parameter.name = GetNextParameterName(parameter.name)
    return addParameter(parameters, parameter);
}

function GetNextParameterName(name: string): string {
    if(name.length === 1){
        name = name + '1';
        return name;
    }

    const start = name.substr(0, 1);
    const rest = name.substr(1, name.length - 1);
    const id = parseInt(rest, 10);
    if(isNaN(id)){
        throw new Error('Could not parse int');
    }
    const newId = id + 1;
    return start + (newId.toString())
}