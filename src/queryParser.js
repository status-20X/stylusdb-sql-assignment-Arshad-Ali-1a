const { parse } = require("json2csv");

const parseQuery = (query) => {
  const re =
    /^SELECT (?<fields>(\w|\.)+(,\s?(\w|\.)+)*) FROM (?<table>\w+)( (?<join_type>\w+) JOIN (?<join_table>\w+) ON (?<join_left>(\w|\.)+)\s?=\s?(?<join_right>(\w|\.)+))?( WHERE (?<where>(\w|\.|[=><!]|\s)+))?$/;
  const re_where_operator = /([=><!]+)/;

  const matches = query.match(re);

  if (!matches) {
    throw new Error("Invalid query");
  }
  // console.log(matches);

  //extracting fields
  fields = matches.groups.fields.split(",").map((field) => field.trim());

  //extracting where clause
  whereClauses = [];

  if (matches.groups.where) {
    matches.groups.where.split(/ ?AND ?/).forEach((clause) => {
      if (!clause.match(re_where_operator)) {
        throw new Error("Invalid where clause");
      }
      const [field, operator, value] = clause.split(re_where_operator);

      whereClauses.push({
        field: field.trim(),
        operator: operator.trim(),
        value: value.trim(),
      });
    });
  }

  return {
    fields,
    table: matches.groups.table,
    whereClauses: whereClauses,
    joinCondition: matches.groups.join_table
      ? { left: matches.groups.join_left, right: matches.groups.join_right }
      : null,
    joinTable: matches.groups.join_table ?? null,
    joinType: matches.groups.join_type ?? null,
  };
};

module.exports = { parseQuery };
