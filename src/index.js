const readCSV = require("./csvReader");
const parseQuery = require("./queryParser");

const executeSELECTQuery = async (query) => {
  const data = await readCSV("./sample.csv");
  const parsed_query = parseQuery(query);

  const result = [];

  await data.forEach((row) => {
    if (parsed_query.whereClause) {
      const [field, value] = parsed_query.whereClause.split(" = ");
      if (row[field] !== value) {
        return;
      }
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
