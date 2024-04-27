const { parse } = require("json2csv");

const parseJoinClause = (query) => {
  const re =
    /( (?<join_type>\w+) JOIN (?<join_table>\w+) ON (?<join_left>(\w|\.)+)\s?=\s?(?<join_right>(\w|\.)+))/;
  const matches = query.match(re);
  if (!matches) {
    return { joinCondition: null, joinTable: null, joinType: null };
  }

  return {
    joinCondition: {
      left: matches.groups.join_left,
      right: matches.groups.join_right,
    },
    joinTable: matches.groups.join_table,
    joinType: matches.groups.join_type,
  };
};

const parseGroupBy = (query) => {
  const re = /(GROUP BY (?<group_fields>(\w|\.|(,\s?))+))/;

  const matches = query.match(re);

  if (!matches) {
    return { groupByFields: null };
  }

  return {
    groupByFields: matches.groups.group_fields
      .replaceAll(/\s+/g, "")
      .split(","),
  };
};

const hasAggregateFunction = (fieldsString) => {
  const re = /(COUNT|SUM|AVG|MIN|MAX)\((\w|\.)+|\*\)/;
  return fieldsString.match(re) !== null;
};

const parseQuery = (query) => {
  const re =
    /SELECT (?<fields>(\w|\.|\(|\)|\*)+(,\s?(\w|\.|\(|\)|\*)+)*) FROM (?<table>\w+)( (?<join_type>\w+) JOIN (?<join_table>\w+) ON (?<join_left>(\w|\.)+)\s?=\s?(?<join_right>(\w|\.)+))?( WHERE (?<where>(\w|\.|[=><!]|\s|'|")+?)(?=\s*GROUP BY|\s*$))?/;

  //! removed full match from regex
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
      // console.log(clause.split(re_where_operator));

      whereClauses.push({
        field: field.trim(),
        operator: operator.trim(),
        value: value.replaceAll(/'|"/g, "").trim(),
      });
    });
  }

  let groupByFieldsObject = parseGroupBy(query);

  return {
    fields,
    table: matches.groups.table,
    whereClauses: whereClauses,
    groupByFields: groupByFieldsObject.groupByFields,
    joinCondition: matches.groups.join_table
      ? { left: matches.groups.join_left, right: matches.groups.join_right }
      : null,
    joinTable: matches.groups.join_table ?? null,
    joinType: matches.groups.join_type ?? null,
    hasAggregateWithoutGroupBy:
      hasAggregateFunction(matches.groups.fields) &&
      !(groupByFieldsObject.groupByFields !== null),
  };
};

module.exports = { parseQuery, parseJoinClause };

// console.log(
//   parseQuery(`SELECT age, COUNT(*) FROM student WHERE age > 22 GROUP BY age`)
// );
