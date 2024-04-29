const csv = require("csv-parser");
const createCsvWriter = require("csv-writer").createObjectCsvWriter;
const fs = require("fs");

const readCSV = async (path) => {
  const results = [];

  return new Promise((resolve, reject) => {
    try {
      fs.createReadStream(path)
        .pipe(csv())
        .on("data", (data) => results.push(data))
        .on("end", () => {
          resolve(results);
        });
    } catch (err) {
      reject(err);
    }
  });
};

const writeCSV = async (path, data) => {
  return new Promise((resolve, reject) => {
    try {
      const writer = createCsvWriter({
        path: path,
        header: Object.keys(data[0]).map((key) => ({ id: key, title: key })),
      });

      writer.writeRecords(data).then(() => {
        resolve();
      });
    } catch (err) {
      reject(err);
    }
  });
};

// (async () => {
//   await writeCSV("grades.csv", [
//     { class: "Mathematics", grade: "A" },
//     { class: "Chemistry", grade: "B" },
//   ]);
// })();

module.exports = { readCSV, writeCSV };
