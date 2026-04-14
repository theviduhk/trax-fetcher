const axios = require("axios");
const admin = require("firebase-admin");

// 🔐 Firebase init
const serviceAccount = require("./firebase-key.json");

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
  databaseURL: "https://outflow-offline-validation-default-rtdb.firebaseio.com/"
});

const db = admin.database();

// 🔐 ENV variables (GitHub Secrets)
const USERNAME = process.env.API_USERNAME;
const PASSWORD = process.env.API_PASSWORD;

async function runQuery() {
  try {
    console.log("🚀 Script started");

    if (!USERNAME || !PASSWORD) {
      throw new Error("Missing API credentials");
    }

    const queryUrl =
      "https://monitor.trax-cloud.com/api/datasources/proxy/133/bigquery/v2/projects/trax-retail/queries";

    const projectValues = [
      "marsuae", "marsbh", "marskw", "marsom", "marsqa"
    ].map(v => `'${v}'`);

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
          event_timestamp BETWEEN TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY)
          AND CURRENT_TIMESTAMP()
          AND project_name IN (${projectValues.join(",")})
        GROUP BY 1,2,3,4
        ORDER BY 1
      `,
      useLegacySql: false
    };

    const authHeader =
      "Basic " + Buffer.from(USERNAME + ":" + PASSWORD).toString("base64");

    // Start query
    const response = await axios.post(queryUrl, query, {
      headers: {
        Authorization: authHeader,
        "Content-Type": "application/json"
      }
    });

    const jobId = response.data.jobReference.jobId;
    const location = response.data.jobReference.location;

    const resultUrl = `${queryUrl}/${jobId}?location=${location}`;

    let result;

    // Wait for result
    for (let i = 0; i < 10; i++) {
      const res = await axios.get(resultUrl, {
        headers: { Authorization: authHeader }
      });

      if (res.data.jobComplete) {
        result = res.data;
        break;
      }

      await new Promise(r => setTimeout(r, 1000));
    }

    if (!result) throw new Error("Query timeout");

    const data = processResults(result);

    await saveToFirebase(data);

    console.log("✅ Firebase updated successfully");

  } catch (err) {
    console.error("❌ ERROR:", err.message);
  }
}

// 🔄 PROCESS DATA
function processResults(result) {
  if (!result.rows) return [];

  const fields = result.schema.fields.map(f => f.name);

  return result.rows.map(row => {
    let obj = {};
    row.f.forEach((c, i) => (obj[fields[i]] = c.v));

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
  const ref = db.ref("reg_masking");
  await ref.set(data);
}

// ▶️ RUN
runQuery();
