const readCSV = require("./csvReader");
const { parseQuery } = require("./queryParser");

const leftJoin = (data, joinTableData, parsed_query) => {
  const JoinedData = [];

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
    if (!leftTableRowAddedAtLeastOnce) {
      const rightObjectWithNullValues = Object.fromEntries(
        Object.entries(joinTableData[0]).map(([key, value]) => [key, null])
      );
      JoinedData.push({ ...row, ...rightObjectWithNullValues });
    }
  });

  return JoinedData;
};

const innerJoin = (data, joinTableData, parsed_query) => {
  const JoinedData = [];

  data.forEach((row) => {
    joinTableData.forEach((joinRow) => {
      if (
        row[parsed_query.joinCondition.left] ===
        joinRow[parsed_query.joinCondition.right]
      ) {
        JoinedData.push({ ...row, ...joinRow });
      }
    });
  });
  return JoinedData;
};

const rightJoin = (data, joinTableData, parsed_query) => {
  const JoinedData = [];

  joinTableData.forEach((joinRow) => {
    let rightTableRowAddedAtLeastOnce = false;
    data.forEach((row) => {
      if (
        row[parsed_query.joinCondition.left] ===
        joinRow[parsed_query.joinCondition.right]
      ) {
        JoinedData.push({ ...row, ...joinRow });
        rightTableRowAddedAtLeastOnce = true;
      }
    });
    if (!rightTableRowAddedAtLeastOnce) {
      const leftObjectWithNullValues = Object.fromEntries(
        Object.entries(data[0]).map(([key, value]) => [key, null])
      );
      JoinedData.push({ ...joinRow, ...leftObjectWithNullValues });
    }
  });

  return JoinedData;
};

const handleWhereClauses = (data, parsed_query) => {
  const filtered_data = [];

  data.forEach((row) => {
    let allWhereClausesMatch = true; //!this is for AND, make for OR also
    if (parsed_query.whereClauses.length) {
      parsed_query.whereClauses.forEach(({ field, operator, value }) => {
        //prettier-ignore
        if (!eval(`"${row[field]}" ${operator === "=" ? "==" : operator} "${value}"`)){ //!added "row[field]==null" for ignoring where when field is null, which happensS in case of joins

          if(parsed_query.joinTable && field.split(".")[0]==parsed_query.table && row[field]==null){
            //if tables are joined AND field is from the main table AND field is null() because it was from the join.. then allow it in the result
            //do nothing
          }
          else
            allWhereClausesMatch = false;
        }
      });
    }

    if (!allWhereClausesMatch) {
      return;
    }

    filtered_data.push(row);
  });

  return filtered_data;
};

const rowWithAggregates = (
  groupedDataObj,
  row,
  aggregateFunctions,
  groupFieldData,
  isNewGroup
) => {
  if (isNewGroup) {
    aggregateFunctions.forEach((aggregateFunction) => {
      //identify the aggregate function
      const aggregateFunctionName = aggregateFunction.split("(")[0]; //TODO: might need to handle lowercase aggregate functions
      const aggregateFunctionField = aggregateFunction
        .split("(")[1]
        .split(")")[0];

      switch (aggregateFunctionName) {
        case "COUNT":
          row[aggregateFunction] = 1;
          break;

        case "AVG":
          row[aggregateFunction] = parseInt(row[aggregateFunctionField]);
          row[`_SUM(${aggregateFunctionField})`] = parseInt(
            row[aggregateFunctionField]
          ); //for calculating average later
          break;

        default:
          row[aggregateFunction] = parseInt(row[aggregateFunctionField]); //MAX,MIN,SUM
          break;
      }
    });
    row["_count"] = 1;
    return row;
  } else {
    //for existing group
    const groupRow = groupedDataObj[groupFieldData];

    aggregateFunctions.forEach((aggregateFunction) => {
      //identify the aggregate function
      const aggregateFunctionName = aggregateFunction.split("(")[0]; //TODO: might need to handle lowercase aggregate functions
      const aggregateFunctionField = aggregateFunction
        .split("(")[1]
        .split(")")[0];

      switch (aggregateFunctionName) {
        case "COUNT":
          groupRow[aggregateFunction] += 1; //! in case of no null values, the answer for count(x) and count(*) will be same.. so this works.
          break;

        case "SUM":
          groupRow[aggregateFunction] += parseInt(row[aggregateFunctionField]);
          break;

        case "AVG":
          groupRow[`_SUM(${aggregateFunctionField})`] += parseInt(
            row[aggregateFunctionField]
          );

          groupRow[aggregateFunction] =
            groupRow[`_SUM(${aggregateFunctionField})`] /
            (groupRow["_count"] + 1); //groupRow["_count"]+1 as the count is increased after the switch case

          break;

        case "MAX":
          groupRow[aggregateFunction] = Math.max(
            groupRow[aggregateFunction],
            parseInt(row[aggregateFunctionField])
          );
          break;

        case "MIN":
          groupRow[aggregateFunction] = Math.min(
            groupRow[aggregateFunction],
            parseInt(row[aggregateFunctionField])
          );
          break;

        default:
          throw new Error(`Invalid aggregate function: ${aggregateFunction}`);
      }
      groupRow["_count"] += 1;
    });

    return groupRow;
  }
};

const applyGroupBy = (data, parsed_query) => {
  if (parsed_query.groupByFields == null) {
    return data;
  }

  //identifying aggregate functions
  const aggregateFunctions = [...parsed_query.fields].filter(
    (field) => field.includes("(") && field.includes(")")
  );

  const groupedDataObj = {};
  let groupField = parsed_query.groupByFields[0]; //!assuming only one group by field for now, change this

  data.forEach((row) => {
    const groupFieldData = row[groupField];
    if (groupFieldData in groupedDataObj) {
      groupedDataObj[groupFieldData] = rowWithAggregates(
        groupedDataObj,
        row,
        aggregateFunctions,
        groupFieldData,
        false
      );
    } else {
      groupedDataObj[groupFieldData] = rowWithAggregates(
        groupedDataObj,
        row,
        aggregateFunctions,
        groupFieldData,
        true
      );
    }
    // console.log(groupedDataObj);
  });

  return Object.values(groupedDataObj);
};

const applyAggrateWithoutGroupBy = (data, parsed_query) => {
  const aggregateFunctions = [...parsed_query.fields].filter(
    (field) => field.includes("(") && field.includes(")")
  );

  const groupedDataObj = {};
  const keyToReplaceGroupFieldData = null;

  data.forEach((row) => {
    if (keyToReplaceGroupFieldData in groupedDataObj) {
      groupedDataObj[keyToReplaceGroupFieldData] = rowWithAggregates(
        groupedDataObj,
        row,
        aggregateFunctions,
        keyToReplaceGroupFieldData,
        false
      );
    } else {
      groupedDataObj[keyToReplaceGroupFieldData] = rowWithAggregates(
        groupedDataObj,
        row,
        aggregateFunctions,
        keyToReplaceGroupFieldData,
        true
      );
    }
    // console.log(groupedDataObj);
  });

  return Object.values(groupedDataObj);
};

const executeSELECTQuery = async (query) => {
  const parsed_query = parseQuery(query);
  // console.log("query:", parsed_query);
  let data = await readCSV(`./${parsed_query.table}.csv`);

  if (parsed_query.joinTable) {
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

    switch (parsed_query.joinType) {
      case "LEFT":
        data = leftJoin(data, joinTableData, parsed_query);
        break;

      case "RIGHT":
        data = rightJoin(data, joinTableData, parsed_query);
        break;

      default:
        data = innerJoin(data, joinTableData, parsed_query);
        break;
    }
  }

  // console.log(data);
  data = handleWhereClauses(data, parsed_query);

  if (parsed_query.groupByFields != null)
    data = applyGroupBy(data, parsed_query);
  //!assuming only one group by field for now, change this
  else if (parsed_query.hasAggregateWithoutGroupBy)
    data = applyAggrateWithoutGroupBy(data, parsed_query);

  if (parsed_query.orderByFields) {
    data.sort((a, b) => {
      if (parsed_query.orderByFields.order == "ASC")
        return a[parsed_query.orderByFields.field] >
          b[parsed_query.orderByFields.field]
          ? 1
          : -1;
      else
        return a[parsed_query.orderByFields.field] >
          b[parsed_query.orderByFields.field]
          ? -1
          : 1;
    });
  }

  if (parsed_query.limit != null) {
    data = data.slice(0, parsed_query.limit);
  }

  const result = [];

  data.forEach((row) => {
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

// (async () => {
//   console.log(
//     await executeSELECTQuery("SELECT AVG(age) FROM student WHERE age > 22")
//   );
// })();
