const axios = require("axios");
const cron = require("node-cron");
const admin = require("firebase-admin");

// 🔐 Firebase init
const serviceAccount = require("./firebase-key.json");

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
  databaseURL: "https://outflow-offline-validation-default-rtdb.firebaseio.com/"
});

const db = admin.database();

// ================= CONFIG =================
const USERNAME = "gss.kurunegala@gssintl.biz";
const PASSWORD = "Gssk@2021";

// =========================================

const projectValues = [
  'marsuae','marsbh','marskw','marsom','marsqa'
].map(v => `'${v}'`);

const projectList = projectValues.join(",");

async function runQuery() {
  try {
    console.log("Running query...");

    const queryUrl = "https://monitor.trax-cloud.com/api/datasources/proxy/133/bigquery/v2/projects/trax-retail/queries";

    const query = {
      query: `
        #standardSQL
        SELECT
          TIMESTAMP_TRUNC(event_timestamp, HOUR) AS timestamp,
          project_name,
          task_name,
          staff_id,
          SUM(count) AS value
        FROM \`trax-retail.backoffice.tl_hourly_report\`
        WHERE
          event_timestamp BETWEEN TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY) AND CURRENT_TIMESTAMP()
          AND project_name IN (${projectList})
        GROUP BY 1,2,3,4
        ORDER BY 1
      `,
      useLegacySql: false
    };

    const response = await axios.post(queryUrl, query, {
      headers: {
        Authorization: "Basic " + Buffer.from(USERNAME + ":" + PASSWORD).toString("base64"),
        "Content-Type": "application/json"
      }
    });

    const jobId = response.data.jobReference.jobId;
    const location = response.data.jobReference.location;

    const resultUrl = `${queryUrl}/${jobId}?location=${location}`;

    let result;

    for (let i = 0; i < 5; i++) {
      const res = await axios.get(resultUrl, {
        headers: {
          Authorization: "Basic " + Buffer.from(USERNAME + ":" + PASSWORD).toString("base64")
        }
      });

      if (res.data.jobComplete) {
        result = res.data;
        break;
      }

      await new Promise(r => setTimeout(r, 1000));
    }

    if (!result) throw new Error("Timeout");

    const rows = processResults(result);

    await saveToFirebase(rows);

    console.log("Firebase updated successfully");

  } catch (err) {
    console.error("Error:", err.message);
  }
}

function processResults(result) {
  if (!result.rows) return [];

  const fields = result.schema.fields.map(f => f.name);

  return result.rows.map(r => {
    let obj = {};
    r.f.forEach((c, i) => obj[fields[i]] = c.v);

    return {
      timestamp: obj.timestamp
        ? new Date(parseFloat(obj.timestamp) * 1000).toISOString()
        : null,
      project: obj.project_name || "N/A",
      task: obj.task_name || "N/A",
      staff_id: obj.staff_id || "N/A",
      value: obj.value || 0
    };
  });
}

// 🔥 SAVE TO FIREBASE
async function saveToFirebase(data) {
  const ref = db.ref("My Project");

  // Option 1: overwrite all
  await ref.set(data);

  // Option 2 (better): push each row
  // data.forEach(d => ref.push(d));
}

// ⏱ every 1 minute
cron.schedule("* * * * *", () => {
  runQuery();
});

console.log("Scheduler started...");
