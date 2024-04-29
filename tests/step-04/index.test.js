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
