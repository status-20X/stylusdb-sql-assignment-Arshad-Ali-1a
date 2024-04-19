const parseQuery = (query) => {
  const re =
    /^SELECT (?<fields>(\w+)(, ?\w+)*) FROM (?<table>\w+)( WHERE (?<where>(\w|=| )+))?$/;

  const matches = query.match(re);

  if (!matches) {
    throw new Error("Invalid query");
  }

  fields = matches.groups.fields.split(",").map((field) => field.trim());

  return {
    fields,
    table: matches.groups.table,
    whereClause: matches.groups.where ?? null,
  };
};

module.exports = parseQuery;
