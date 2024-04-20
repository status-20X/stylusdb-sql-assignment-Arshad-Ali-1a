const readCSV = require("./csvReader");
const { parseQuery } = require("./queryParser");

const executeSELECTQuery = async (query) => {
  const parsed_query = parseQuery(query);
  let data = await readCSV(`./${parsed_query.table}.csv`);

  if (parsed_query.joinTable) {
    const JoinedData = [];
    let joinTableData = await readCSV(`./${parsed_query.joinTable}.csv`);

    data = data.map((row) => {
      const newRow = {};
      Object.keys(row).forEach((key) => {
        newRow[`${parsed_query.table}.${key}`] = row[key];
      });
      return newRow;
    });

    joinTableData = joinTableData.map((row) => {
      const newRow = {};
      Object.keys(row).forEach((key) => {
        newRow[`${parsed_query.joinTable}.${key}`] = row[key];
      });
      return newRow;
    });

    //lll
    data.forEach((row) => {
      let leftTableRowAddedAtLeastOnce = false;
      joinTableData.forEach((joinRow) => {
        if (
          row[parsed_query.joinCondition.left] ===
          joinRow[parsed_query.joinCondition.right]
        ) {
          JoinedData.push({ ...row, ...joinRow });
          leftTableRowAddedAtLeastOnce = true;
        }
      });
      if (!leftTableRowAddedAtLeastOnce && parsed_query.joinType === "LEFT") {
        const rightObjectWithNullValues = Object.fromEntries(
          Object.entries(joinTableData[0]).map(([key, value]) => [key, null])
        );
        JoinedData.push({ ...row, ...rightObjectWithNullValues });
      }
    });

    data = JoinedData;
  }

  // console.log(data);

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

  // console.log(result);
  return result;
};

module.exports = executeSELECTQuery;
