const parseQuery = (query) => {
  const re = /SELECT (\w+)(, \w+)* FROM (\w+)/;

  const matches = re.exec(query);

  if (!matches) {
    throw new Error("Invalid query");
  }

  console.log(matches.length);
  fields = matches
    .slice(1, matches.length - 1)
    .map((field) => field.replace(/,/g, "").trim());

  return { fields, table: matches[matches.length - 1] };
};

module.exports = parseQuery;
