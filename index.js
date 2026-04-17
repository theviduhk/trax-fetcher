const fetch = require("node-fetch");

// ==============================
// CONFIG
// ==============================
const QUERY_URL = "https://monitor.trax-cloud.com/api/datasources/proxy/133/bigquery/v2/projects/trax-retail/queries";

const USERNAME = process.env.API_USERNAME;
const PASSWORD = process.env.API_PASSWORD;

// 🔥 Overwrite target
const FIREBASE_URL = "https://project-outflow-stitching-default-rtdb.firebaseio.com/My_Project.json";

// ==============================
// PROJECT LIST
// ==============================
const projectValues = [
  'marsuae','marsbh','marskw','marsom','marsqa','beiersdorfcz','bdftr','beiersdorfgr','beiersdorfng','beiersdorfpt','beiersdorfru','beiersdorfsp','beiersdorfuk','diageoes','diageotz','dlcpt','jtihr','marspl','marssa','mondelezde','mdlzrusf','mondelezse','mondelezza','pgcz','pngza2','ulbe','ulpt','pepsicouk','jdetr','diageoie','mondelezno','mondelezkaza',
  'beiersdorfde','mondelezsa','straussdryil','mondelezuz','gskhu','tevapl','pgpl','inbevci','gsklt','ulnl','beiersdorfkz','beiersdorfuae','tevaru','inbevnl','ulit','pernodricardes',
  'pgbaltics2','pghu','pgcroatia','pges','pgpt','pgespharma','pepsicofr','haleonaesa','haleonil','haleonse','gskpl','schwartautkde','gskgr','gskuz','straussfritolayil','straussil','cbcdairyil','cbcil'
].map(v => `'${v}'`);

const projectList = projectValues.join(", ");

// ==============================
// MAIN FUNCTION
// ==============================
async function main() {
  try {
    const auth = Buffer.from(`${USERNAME}:${PASSWORD}`).toString("base64");

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
          AND task_name IN (
            'stitching', 'voting_engine', 'offline_validation', 'offline_posm',
            'scene_recognition', 'voting', 'validation', 'offline_pricing'
          )
          AND project_name IN (${projectList})
        GROUP BY 1, 2, 3, 4
        ORDER BY 1
      `,
      useLegacySql: false
    };

    // ==============================
    // STEP 1: RUN QUERY
    // ==============================
    const res = await fetch(QUERY_URL, {
      method: "POST",
      headers: {
        "Authorization": `Basic ${auth}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify(query)
    });

    const data = await res.json();

    if (!data.jobReference) throw new Error("No job ID received");

    const jobId = data.jobReference.jobId;
    const location = data.jobReference.location;

    // ==============================
    // STEP 2: WAIT FOR RESULT
    // ==============================
    const resultUrl = `${QUERY_URL}/${jobId}?location=${location}`;

    let result;

    for (let i = 0; i < 15; i++) {
      const r = await fetch(resultUrl, {
        headers: { "Authorization": `Basic ${auth}` }
      });

      result = await r.json();

      if (result.jobComplete) break;

      console.log("⏳ Waiting for query...");
      await new Promise(r => setTimeout(r, 1500));
    }

    if (!result.rows) {
      console.log("⚠️ No data returned → skip overwrite");
      return;
    }

    // ==============================
    // STEP 3: PROCESS DATA
    // ==============================
    const fields = result.schema.fields.map(f => f.name);

    const rows = result.rows.map(row => {
      let obj = {};

      row.f.forEach((col, i) => {
        obj[fields[i]] = col.v;
      });

      return {
        timestamp: obj.timestamp || null,
        project: obj.project_name,
        task: obj.task_name,
        gid: obj.staff_id,
        value: Number(obj.value)
      };
    });

    // ==============================
    // STEP 4: OVERWRITE FIREBASE
    // ==============================
    if (rows.length > 0) {
      await fetch(FIREBASE_URL, {
        method: "PUT", // 🔥 FULL REPLACE
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(rows)
      });

      console.log("✅ Firebase overwritten:", rows.length);
    } else {
      console.log("⚠️ Empty data → not overwriting");
    }

  } catch (err) {
    console.error("❌ ERROR:", err.message);
  }
}

// ==============================
// SCHEDULER (EVERY 1 MINUTE)
// ==============================
let isRunning = false;

async function run() {
  if (isRunning) {
    console.log("⏳ Previous job still running, skipping...");
    return;
  }

  isRunning = true;
  console.log("🚀 Running job at:", new Date().toISOString());

  try {
    await main();
  } finally {
    isRunning = false;
  }
}

// run immediately
run();

// run every 1 minute
setInterval(run, 60 * 1000);
