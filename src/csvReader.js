const csv = require("csv-parser");
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

module.exports = readCSV;
