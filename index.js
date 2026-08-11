const fetch = require("node-fetch");

// ==============================
// CONFIG
// ==============================
const QUERY_URL =
  "https://monitor.trax-cloud.com/api/datasources/proxy/133/bigquery/v2/projects/trax-retail/queries";

const USERNAME = process.env.API_USERNAME;
const PASSWORD = process.env.API_PASSWORD;

const FIREBASE_URL =
  "https://project-outflow-stitching-default-rtdb.firebaseio.com/My_Project.json";

// ==============================
// PROJECT LIST
// ==============================
const projectValues = [
  "marsuae",
  "marsbh",
  "marskw",
  "marsom",
  "marsqa",
  "beiersdorfcz",
  "bdftr",
  "beiersdorfgr",
  "beiersdorfng",
  "beiersdorfpt",
  "beiersdorfru",
  "beiersdorfsp",
  "beiersdorfuk",
  "diageoes",
  "diageotz",
  "dlcpt",

  // JTI
  "jtihr",

  "marspl",
  "marssa",
  "mondelezde",
  "mdlzrusf",
  "mondelezse",
  "mondelezza",
  "pgcz",
  "pngza2",
  "ulbe",
  "ulpt",
  "pepsicouk",
  "jdetr",
  "diageoie",
  "mondelezno",
  "mondelezkaza",
  "beiersdorfde",
  "mondelezsa",
  "straussdryil",
  "mondelezuz",
  "gskhu",
  "tevapl",
  "pgpl",
  "inbevci",
  "gsklt",
  "diageofr",
  "ulnl",
  "beiersdorfkz",
  "beiersdorfuae",
  "tevaru",
  "inbevnl",
  "ulit",
  "pernodricardes",
  "pgbaltics2",
  "pghu",
  "pgcroatia",
  "pges",
  "pgpt",
  "pgespharma",
  "pepsicofr",
  "haleonaesa",
  "haleonil",
  "haleonse",
  "gskpl",
  "schwartautkde",
  "gskgr",
  "gskuz",
  "straussfritolayil",
  "straussil",
  "cbcdairyil",
  "cbcil"
];

// Convert project names to BigQuery IN clause
const projectList = projectValues
  .map((project) => `'${project}'`)
  .join(", ");

// ==============================
// MAIN FUNCTION
// ==============================
async function main() {
  // ==============================
  // CHECK ENV VARIABLES
  // ==============================
  if (!USERNAME || !PASSWORD) {
    throw new Error(
      "API_USERNAME or API_PASSWORD environment variable is missing."
    );
  }

  // ==============================
  // BASIC AUTH
  // ==============================
  const auth = Buffer.from(`${USERNAME}:${PASSWORD}`).toString("base64");

  // ==============================
  // BIGQUERY SQL
  // ==============================
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
        event_timestamp BETWEEN
          TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY)
          AND CURRENT_TIMESTAMP()

        AND task_name IN (
          'stitching',
          'voting_engine',
          'offline_validation',
          'offline_posm',
          'scene_recognition',
          'voting',
          'validation',
          'offline_pricing'
        )

        AND project_name IN (
          ${projectList}
        )

      GROUP BY
        1,
        2,
        3,
        4

      ORDER BY
        1
    `,
    useLegacySql: false
  };

  console.log("==========================================");
  console.log("🚀 Starting BigQuery request");
  console.log("==========================================");
  console.log("📊 Projects:", projectValues.length);
  console.log("🎯 JTIHR included:", projectValues.includes("jtihr"));
  console.log("⏰ Started:", new Date().toISOString());

  // ==============================
  // STEP 1: RUN QUERY
  // ==============================
  console.log("\n🔵 Step 1: Running BigQuery query...");

  const res = await fetch(QUERY_URL, {
    method: "POST",
    headers: {
      Authorization: `Basic ${auth}`,
      "Content-Type": "application/json"
    },
    body: JSON.stringify(query)
  });

  if (!res.ok) {
    const errorText = await res.text();

    throw new Error(
      `BigQuery request failed: ${res.status} ${res.statusText}\n${errorText}`
    );
  }

  const data = await res.json();

  if (!data.jobReference) {
    console.log("BigQuery response:", data);
    throw new Error("No BigQuery job ID received.");
  }

  const jobId = data.jobReference.jobId;
  const location = data.jobReference.location;

  console.log("✅ Query started");
  console.log("🆔 Job ID:", jobId);
  console.log("📍 Location:", location);

  // ==============================
  // STEP 2: GET RESULTS
  // ==============================
  console.log("\n🔵 Step 2: Waiting for BigQuery results...");

  const resultUrl =
    `${QUERY_URL}/${jobId}?location=${location}`;

  let result = null;

  for (let i = 0; i < 30; i++) {
    console.log(`⏳ Checking query status... ${i + 1}/30`);

    const r = await fetch(resultUrl, {
      method: "GET",
      headers: {
        Authorization: `Basic ${auth}`
      }
    });

    if (!r.ok) {
      const errorText = await r.text();

      throw new Error(
        `BigQuery result request failed: ${r.status} ${r.statusText}\n${errorText}`
      );
    }

    result = await r.json();

    if (result.jobComplete) {
      console.log("✅ BigQuery job completed");
      break;
    }

    await new Promise((resolve) => setTimeout(resolve, 1000));
  }

  // ==============================
  // CHECK RESULT
  // ==============================
  if (!result) {
    throw new Error("No result received from BigQuery.");
  }

  if (result.errors && result.errors.length > 0) {
    console.error("BigQuery errors:", result.errors);

    throw new Error("BigQuery query failed.");
  }

  if (!result.jobComplete) {
    throw new Error("BigQuery query did not complete within timeout.");
  }

  // ==============================
  // NO DATA
  // ==============================
  if (!result.rows || result.rows.length === 0) {
    console.log("⚠️ No data found for today.");
    return;
  }

  // ==============================
  // STEP 3: PROCESS DATA
  // ==============================
  console.log("\n🔵 Step 3: Processing data...");

  const fields = result.schema.fields.map((field) => field.name);

  const rows = result.rows.map((row) => {
    const obj = {};

    row.f.forEach((col, index) => {
      obj[fields[index]] = col.v;
    });

    return {
      timestamp: obj.timestamp
        ? new Date(Number(obj.timestamp) * 1000).toISOString()
        : null,

      project: obj.project_name || null,

      task: obj.task_name || null,

      gid: obj.staff_id || null,

      value: Number(obj.value || 0)
    };
  });

  console.log("✅ Rows processed:", rows.length);

  // ==============================
  // CHECK JTIHR DATA
  // ==============================
  const jtihrRows = rows.filter(
    (row) => row.project === "jtihr"
  );

  console.log("==========================================");
  console.log("🔎 JTIHR DATA CHECK");
  console.log("==========================================");
  console.log("JTIHR rows:", jtihrRows.length);

  if (jtihrRows.length > 0) {
    console.log("✅ jtihr data found");

    console.log(
      "Sample JTIHR row:",
      JSON.stringify(jtihrRows[0], null, 2)
    );
  } else {
    console.log("⚠️ No jtihr data found for today.");
  }

  // ==============================
  // PROJECT SUMMARY
  // ==============================
  const projectSummary = {};

  rows.forEach((row) => {
    if (!projectSummary[row.project]) {
      projectSummary[row.project] = 0;
    }

    projectSummary[row.project]++;
  });

  console.log("\n📊 Project summary:");

  Object.entries(projectSummary)
    .sort((a, b) => a[0].localeCompare(b[0]))
    .forEach(([project, count]) => {
      console.log(`   ${project}: ${count}`);
    });

  // ==============================
  // STEP 4: PUSH TO FIREBASE
  // ==============================
  console.log("\n🔵 Step 4: Uploading to Firebase...");

  const firebaseResponse = await fetch(FIREBASE_URL, {
    method: "PUT",
    headers: {
      "Content-Type": "application/json"
    },
    body: JSON.stringify(rows)
  });

  if (!firebaseResponse.ok) {
    const errorText = await firebaseResponse.text();

    throw new Error(
      `Firebase upload failed: ${firebaseResponse.status} ${firebaseResponse.statusText}\n${errorText}`
    );
  }

  console.log("==========================================");
  console.log("✅ Firebase successfully overwritten");
  console.log("==========================================");
  console.log("📦 Total rows:", rows.length);
  console.log("🎯 JTIHR rows:", jtihrRows.length);
  console.log("⏰ Finished:", new Date().toISOString());
}

// ==============================
// RETRY WRAPPER
// ==============================
async function runWithRetry(retries = 3) {
  for (let i = 1; i <= retries; i++) {
    try {
      console.log("\n==========================================");
      console.log(`🚀 Attempt ${i} / ${retries}`);
      console.log("==========================================");

      await main();

      console.log("\n✅ Script completed successfully.");
      return;

    } catch (err) {
      console.error(
        `\n❌ Error on attempt ${i}:`,
        err.message
      );

      if (i === retries) {
        console.error("\n🔥 All retries failed.");
        process.exit(1);
      }

      console.log("\n🔁 Retrying in 10 seconds...");

      await new Promise((resolve) =>
        setTimeout(resolve, 10000)
      );
    }
  }
}

// ==============================
// START
// ==============================
runWithRetry();
