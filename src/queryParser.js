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

const parseLimit = (query) => {
  const re = /LIMIT (?<limit>\d+)/;

  const matches = query.match(re);

  if (!matches) {
    return null;
  }

  return parseInt(matches.groups.limit.trim());
};

const parseOrderBy = (query) => {
  const re = /ORDER BY (?<orderby_field>(\w|\.)+) (?<orderby_order>(ASC|DESC))/;

  const matches = query.match(re);

  if (!matches) {
    return null;
  }

  return {
    field: matches.groups.orderby_field.trim(),
    order: matches.groups.orderby_order.trim(),
  };
};

const parseQuery = (query) => {
  const re =
    /SELECT (?<distinct>\s*DISTINCT\s*)?(?<fields>(\w|\.|\(|\)|\*|\s)+(,\s?(\w|\.|\(|\)|\*|\s)+)*) FROM (?<table>\w+)( (?<join_type>\w+) JOIN (?<join_table>\w+) ON (?<join_left>(\w|\.)+)\s?=\s?(?<join_right>(\w|\.)+))?( WHERE (?<where>(\w|\.|[=><!]|\s|'|"|%)+?)(?=\s*(GROUP BY|ORDER BY|LIMIT|UNION|\s*$)))?/;

  //! removed full match from regex
  const re_where_operator = /(LIKE|<=|>=|==|!=|<|>|=)/;

  const matches = query.match(re);

  if (!matches) {
    throw new Error(
      "Error executing query: Query parsing error: Invalid SELECT format"
    );
  }
  // console.log(matches);

  //extracting fields
  fields = matches.groups.fields.split(",").map((field) => field.trim());

  //extracting where clause
  whereClauses = [];
  console.log(matches.groups.where);

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
    orderByFields: parseOrderBy(query), //! assuming only one order by field for now
    limit: parseLimit(query),
    isDistinct: matches.groups.distinct !== undefined,
  };
};

module.exports = { parseQuery, parseJoinClause };

// console.log(parseQuery(`SELECT name FROM student WHERE name <8`));
