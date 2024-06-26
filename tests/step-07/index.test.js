const { readCSV } = require("../../src/csvReader");
const { parseSelectQuery } = require("../../src/queryParser");
const { executeSELECTQuery } = require("../../src/queryExecutor");

test("Read CSV File", async () => {
  const data = await readCSV("./student.csv");
  expect(data.length).toBeGreaterThan(0);
  expect(data.length).toBe(4);
  expect(data[0].name).toBe("John");
  expect(data[0].age).toBe("30");
}, 5000);

test("Parse SQL Query", () => {
  const query = "SELECT id, name FROM student";
  const parsed = parseSelectQuery(query);
  expect(parsed).toEqual({
    fields: ["id", "name"],
    groupByFields: null,
    hasAggregateWithoutGroupBy: false,
    isDistinct: false,
    joinCondition: null,
    joinTable: null,
    joinType: null,
    limit: null,
    orderByFields: null,
    table: "student",
    whereClauses: [],
  });
});

test("Execute SQL Query", async () => {
  const query = "SELECT id, name FROM student";
  const result = await executeSELECTQuery(query);
  expect(result.length).toBeGreaterThan(0);
  expect(result[0]).toHaveProperty("id");
  expect(result[0]).toHaveProperty("name");
  expect(result[0]).not.toHaveProperty("age");
  expect(result[0]).toEqual({ id: "1", name: "John" });
}, 30000);

test("Parse SQL Query with WHERE Clause", () => {
  const query = "SELECT id, name FROM sample WHERE age = 25";
  const parsed = parseSelectQuery(query);
  expect(parsed).toEqual({
    fields: ["id", "name"],
    table: "sample",
    whereClauses: [
      {
        field: "age",
        operator: "=",
        value: "25",
      },
    ],
    groupByFields: null,
    hasAggregateWithoutGroupBy: false,
    isDistinct: false,
    joinCondition: null,
    joinTable: null,
    joinType: null,
    limit: null,
    orderByFields: null,
  });
});

test("Execute SQL Query with WHERE Clause", async () => {
  const query = "SELECT id, name FROM student WHERE age = 25";
  const result = await executeSELECTQuery(query);
  expect(result.length).toBe(1);
  expect(result[0]).toHaveProperty("id");
  expect(result[0]).toHaveProperty("name");
  expect(result[0].id).toBe("2");
});

test("Parse SQL Query with Multiple WHERE Clauses", () => {
  const query = "SELECT id, name FROM sample WHERE age = 30 AND name = John";
  const parsed = parseSelectQuery(query);
  expect(parsed).toEqual({
    fields: ["id", "name"],
    table: "sample",
    whereClauses: [
      {
        field: "age",
        operator: "=",
        value: "30",
      },
      {
        field: "name",
        operator: "=",
        value: "John",
      },
    ],
    groupByFields: null,
    hasAggregateWithoutGroupBy: false,
    isDistinct: false,
    joinCondition: null,
    joinTable: null,
    joinType: null,
    limit: null,
    orderByFields: null,
  });
});

test("Execute SQL Query with Multiple WHERE Clause", async () => {
  const query = "SELECT id, name FROM student WHERE age = 30 AND name = John";
  const result = await executeSELECTQuery(query);
  expect(result.length).toBe(1);
  expect(result[0]).toEqual({ id: "1", name: "John" });
});

test("Execute SQL Query with Greater Than", async () => {
  const queryWithGT = "SELECT id FROM student WHERE age > 22";
  const result = await executeSELECTQuery(queryWithGT);
  expect(result.length).toEqual(3);
  expect(result[0]).toHaveProperty("id");
});

test("Execute SQL Query with Not Equal to", async () => {
  const queryWithGT = "SELECT name FROM student WHERE age != 25";
  const result = await executeSELECTQuery(queryWithGT);
  expect(result.length).toEqual(3);
  expect(result[0]).toHaveProperty("name");
});
