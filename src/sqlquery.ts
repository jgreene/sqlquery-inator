
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

export function addParameter(parameters: SqlParameters, parameter: SqlParameter): SqlParameter {
    const current = parameters[parameter.name];
    if(current === undefined){
        parameters[parameter.name] = parameter;
        return parameter;
    }

    parameter.name = parameter.name + '0'
    return addParameter(parameters, parameter);
}