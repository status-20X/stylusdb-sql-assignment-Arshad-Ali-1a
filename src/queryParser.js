const { parse } = require("json2csv");

const parseQuery = (query) => {
  const re =
    /^SELECT (?<fields>(\w+)(, ?\w+)*) FROM (?<table>\w+)( WHERE (?<where>(\w|[=><!]| )+))?$/;
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
  };
};

module.exports = parseQuery;
