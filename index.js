const functions = require("firebase-functions");
const fetch = require("node-fetch");

// ==============================
// CONFIG (SECURE)
// ==============================
const USERNAME = functions.config().api.username;
const PASSWORD = functions.config().api.password;

const QUERY_URL = "https://monitor.trax-cloud.com/api/datasources/proxy/133/bigquery/v2/projects/trax-retail/queries";

const FIREBASE_URL = "https://outflow-offline-validation-default-rtdb.firebaseio.com/reg_masking.json";

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
// MAIN FUNCTION (RUN EVERY 1 MIN)
// ==============================
exports.syncData = functions.pubsub
  .schedule("* * * * *") // EVERY 1 MINUTE
  .timeZone("UTC")
  .onRun(async () => {

    try {
      console.log("Starting job...");

      const auth = Buffer.from(`${USERNAME}:${PASSWORD}`).toString("base64");

      // ==============================
      // STEP 1: RUN QUERY
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

      const res = await fetch(QUERY_URL, {
        method: "POST",
        headers: {
          "Authorization": `Basic ${auth}`,
          "Content-Type": "application/json"
        },
        body: JSON.stringify(query)
      });

      const data = await res.json();

      if (!data.jobReference) {
        throw new Error("No jobReference returned");
      }

      const jobId = data.jobReference.jobId;
      const location = data.jobReference.location;

      // ==============================
      // STEP 2: GET RESULTS (WAIT)
      // ==============================
      const resultUrl = `${QUERY_URL}/${jobId}?location=${location}`;

      let result;

      for (let i = 0; i < 10; i++) {
        const r = await fetch(resultUrl, {
          method: "GET",
          headers: {
            "Authorization": `Basic ${auth}`
          }
        });

        result = await r.json();

        if (result.jobComplete) break;

        await new Promise(resolve => setTimeout(resolve, 1000));
      }

      if (!result || !result.rows) {
        console.log("No data returned");
        return null;
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
          timestamp: obj.timestamp
            ? new Date(obj.timestamp * 1000).toISOString()
            : null,
          project: obj.project_name || null,
          task: obj.task_name || null,
          gid: obj.staff_id || null,
          value: Number(obj.value) || 0
        };
      });

      // ==============================
      // STEP 4: PUSH TO FIREBASE
      // ==============================
      await fetch(FIREBASE_URL, {
        method: "PUT", // overwrite data
        headers: {
          "Content-Type": "application/json"
        },
        body: JSON.stringify({
          lastUpdated: new Date().toISOString(),
          count: rows.length,
          data: rows
        })
      });

      console.log("Firebase updated:", rows.length);

    } catch (error) {
      console.error("ERROR:", error.message);
    }

    return null;
  });
