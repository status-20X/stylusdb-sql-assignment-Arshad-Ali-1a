const readCSV = require("./csvReader");
const parseQuery = require("./queryParser");

const executeSELECTQuery = async (query) => {
  const data = await readCSV("./sample.csv");
  const parsed_query = parseQuery(query);

  const result = [];

  await data.forEach((row) => {
    let allWhereClausesMatch = true;
    if (parsed_query.whereClauses.length) {
      parsed_query.whereClauses.forEach(({ field, operator, value }) => {
        //prettier-ignore
        if (!eval(`"${row[field]}" ${operator === "=" ? "==" : operator} "${value}"`))
          allWhereClausesMatch = false;
      });
    }

    if (!allWhereClausesMatch) {
      return;
    }

    const obj = {};
    parsed_query.fields.forEach((field) => {
      obj[field] = row[field];
    });
    result.push(obj);
  });

  return result;
};

module.exports = executeSELECTQuery;
