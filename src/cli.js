const readline = require("readline");
const {
  executeSELECTQuery,
  executeINSERTQuery,
  executeDELETEQuery,
} = require("./queryExecutor");

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
});

rl.setPrompt("SQL> ");
console.log(
  'SQL Query Engine CLI. Enter your SQL commands, or type "exit" to quit.'
);

rl.prompt();

rl.on("line", async (line) => {
  if (line.toLowerCase() === "exit") {
    rl.close();
    return;
  }

  const UppercaseQuery = line.toUpperCase();

  try {
    if (UppercaseQuery.startsWith("SELECT")) {
      const result = await executeSELECTQuery(line);
      console.log(result);
    } else if (UppercaseQuery.startsWith("INSERT")) {
      await executeINSERTQuery(line);
      console.log("Query executed successfully");
    } else if (UppercaseQuery.startsWith("DELETE")) {
      await executeDELETEQuery(line);
      console.log("Query executed successfully");
    } else {
      console.log(
        "Error: Query not supported. Only SELECT, INSERT, DELETE queries are supported."
      );
    }
  } catch (error) {
    console.error("Error:", error.message);
  }

  rl.prompt();
}).on("close", () => {
  console.log("Exiting SQL CLI");
  process.exit(0);
});
