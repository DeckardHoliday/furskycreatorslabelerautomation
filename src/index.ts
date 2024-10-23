// TODO: consider switching to jetstream; @skyware/jetstream
import { Firehose, RepoOp } from "@skyware/firehose";

import { BskyAgent, AppBskyActorDefs, AppBskyActorProfile } from "@atproto/api";

import * as fs from "fs";

import { Client as PgClient } from "pg";
import dayjs from "dayjs";
import relativeTime from "dayjs/plugin/relativeTime";
dayjs.extend(relativeTime);

const GENERATE_JSON_FILE_OF_SPECIES =
  process.env.GENERATE_JSON_FILE_OF_SPECIES === "true";
const GENERATE_JSON_FILE_OF_SPECIES__DIR = "/tmp/labels";
const GENERATE_JSON_FILE_OF_SPECIES__NAME = "labels.json";

const ozone_service_user_did = process.env.OZONE_SERVICE_USER_DID as string;

const dbclient = new PgClient({
  host: process.env.POSTGRES_HOST || "localhost",
  port: process.env.POSTGRES_PORT
    ? parseInt(process.env.POSTGRES_PORT as string)
    : 5432,
  user: process.env.POSTGRES_USER,
  password: process.env.POSTGRES_PASSWORD,
  database: process.env.POSTGRES_DB,
});

const agent = new BskyAgent({
  service: "https://bsky.social",
});

BskyAgent.configure({
  appLabelers: [process.env.OZONE_SERVICE_USER_DID ?? ""],
});

function toTitleCase(str: string) {
  return str.replace(
    /\w\S*/g,
    (text) => text.charAt(0).toUpperCase() + text.substring(1).toLowerCase()
  );
}

async function main() {
  await agent.login({
    identifier: process.env.BSKY_USER as string,
    password: process.env.BSKY_PASS as string,
  }).then(() => {
    fs.writeFile(
      `${GENERATE_JSON_FILE_OF_SPECIES__DIR}/ratelimit.json`,
      JSON.stringify({ "status": "OK" }),
      function (err) {
        if (err) {
          console.log(err);
        }
      }
    );
  }).catch((err) => {
    console.error(err);
    if (err.toString().includes("Rate Limit Exceeded")) {
      let errDetails = {
        error: err.error,
        date: err.headers.date,
        "ratelimit-limit": err.headers["ratelimit-limit"],
        "ratelimit-policy": err.headers["ratelimit-policy"],
        "ratelimit-remaining": err.headers["ratelimit-remaining"],
        "ratelimit-reset": err.headers["ratelimit-reset"],
      }
      fs.writeFile(
        `${GENERATE_JSON_FILE_OF_SPECIES__DIR}/ratelimit.json`,
        JSON.stringify({ "status": "RATE_LIMIT", "details": errDetails }, undefined, 4),
        function (err) {
          if (err) {
            console.log(err);
          }
        }
      );
    }
  });

  const getOzoneCurrentPolicies = async () => {
    await agent.refreshSession();
    let response = await agent
      .withProxy("atproto_labeler", ozone_service_user_did)
      .api.com.atproto.repo.getRecord({
        repo: ozone_service_user_did,
        collection: "app.bsky.labeler.service",
        rkey: "self",
      });
    if (GENERATE_JSON_FILE_OF_SPECIES) {
      if (!fs.existsSync(GENERATE_JSON_FILE_OF_SPECIES__DIR)) {
        fs.mkdirSync(GENERATE_JSON_FILE_OF_SPECIES__DIR, { recursive: true });
      }
      const knownSonaPosts = await dbclient.query(`
                SELECT postid, role FROM posts
            `);
      const current_simple = (
        response as any
      ).data.value.policies.labelValueDefinitions.map((i: any) => {
        let postsLink = `https://bsky.app/search?q=from%3A${process.env.BSKY_USER
          }+%role%3A%22+%22${i.identifier.replace(/\-/g, "+")}%22`;
        let sonaPostDefined =
          knownSonaPosts.rows.filter((j) => j.species === i.identifier)
            .length === 1;
        let sonaPost;
        if (sonaPostDefined) {
          sonaPost = `https://bsky.app/profile/${ozone_service_user_did}/post/${knownSonaPosts.rows.filter((j) => j.species === i.identifier)[0]
            .postid
            }`;
        }
        return {
          id: i.identifier,
          name: i.locales[0].name,
          description: i.locales[0].description,
          locales: i.locales,
          posts: sonaPost || postsLink,
        };
      });
      fs.writeFile(
        `${GENERATE_JSON_FILE_OF_SPECIES__DIR}/${GENERATE_JSON_FILE_OF_SPECIES__NAME}`,
        JSON.stringify(current_simple),
        function (err) {
          if (err) {
            console.log(err);
          }
        }
      );
    }
    return (response as any).data.value.policies;
  };

  const addLabelIfNotInOzoneCurrentPolicies = async (
    label: string,
    safeLabel?: string,
    postId?: string,
    isMetaTag?: boolean
  ) => {
    await agent.refreshSession();
    let current_policies = await getOzoneCurrentPolicies();
    let name = toTitleCase(label.replace(/-/g, " "));
    const knownSonaPosts = await dbclient.query(
      `
            SELECT postid, role FROM posts
            WHERE roles = $1
            LIMIT 1;
        `,
      [label]
    );
    if (knownSonaPosts.rowCount === 0) {
      dbclient
        .query(
          `
                insert into posts (postid,roles) VALUES ($1, $2) RETURNING *;
            `,
          [postId || "", label]
        )
        .then((res) => {
          console.log(`Logged to checkpoint table: ${JSON.stringify(res)}`);
        })
        .catch((err) => console.error(err));
    }
    if (
      current_policies.labelValues.filter((i: string) => i === safeLabel)
        .length === 0
    ) {
      let labelValues = current_policies.labelValues;
      let labelValueDefinitions = current_policies.labelValueDefinitions;

      labelValues.push(safeLabel);
      labelValueDefinitions.push({
        blurs: "none",
        locales: [
          {
            lang: "en",
            name: name,
            description:
              isMetaTag === true
                ? `${name} [Category: Meta]`
                : `This user is a${["a", "e", "i", "o", "u"].includes(
                  name.at(0)!.toLowerCase()
                )
                  ? "n"
                  : ""
                } ${name}!`,
          },
        ],
        severity: "inform",
        adultOnly: false,
        identifier: safeLabel ? safeLabel : label,
        defaultSetting: "warn",
      });
      let body = {
        repo: ozone_service_user_did,
        collection: "app.bsky.labeler.service",
        rkey: "self",
        record: {
          $type: "app.bsky.labeler.service",
          policies: {
            labelValues: labelValues,
            labelValueDefinitions: labelValueDefinitions,
          },
          createdAt: new Date().toISOString(),
        },
      };
      let response = await agent
        .withProxy("atproto_labeler", ozone_service_user_did)
        .api.com.atproto.repo.putRecord(body);
      console.log("added new label: ", label, `[${response.success}]`);
    } else {
      console.log("Label already exists, didn't have to add a new one.");
    }
  };

  const updateDidRecordOzone = async (
    subject: string | AppBskyActorDefs.ProfileView,
    _cid: string,
    action: "add" | "remove",
    labelVal: string
  ) => {
    await agent.refreshSession();
    let labeller_did = agent.session?.did;
    const did = AppBskyActorDefs.isProfileView(subject) ? subject.did : subject;
    const repo = await agent
      .withProxy("atproto_labeler", labeller_did!)
      .api.tools.ozone.moderation.getRepo({ did: did })
      .catch((err) => {
        console.log(err);
      });
    if (!repo) return;
    if (action === "remove") {
      await agent
        .withProxy("atproto_labeler", labeller_did!)
        .api.tools.ozone.moderation.emitEvent({
          event: {
            $type: "tools.ozone.moderation.defs#modEventLabel",
            createLabelVals: [],
            negateLabelVals: [labelVal],
          },
          subject: {
            $type: "com.atproto.admin.defs#repoRef",
            did: did,
          },
          createdBy: agent.session!.did,
          createdAt: new Date().toISOString(),
          subjectBlobCids: [],
        })
        .catch((err) => {
          console.log(err);
        })
        .then(() => console.log(`Deleted label ${labelVal} for ${did}`));
    } else {
      await agent
        .withProxy("atproto_labeler", labeller_did!)
        .api.tools.ozone.moderation.emitEvent({
          event: {
            $type: "tools.ozone.moderation.defs#modEventLabel",
            createLabelVals: [labelVal],
            negateLabelVals: [],
          },
          subject: {
            $type: "com.atproto.admin.defs#repoRef",
            did: did,
          },
          createdBy: agent.session!.did,
          createdAt: new Date().toISOString(),
          subjectBlobCids: [],
        })
        .catch((err) => {
          console.log(err);
        })
        .then(() => console.log(`Labeled ${did} with ${labelVal}`));
    }
  };

  await dbclient.connect();
  await dbclient.query(`
    CREATE TABLE IF NOT EXISTS roles (
        id SERIAL PRIMARY KEY,
        did VARCHAR(255) NOT NULL,
        likepath VARCHAR(255) NOT NULL,
        posturi VARCHAR(255) NOT NULL,
        role VARCHAR(255) NOT NULL,
        ts TIMESTAMP NOT NULL
    );
    `);

  await dbclient.query(`
    CREATE TABLE IF NOT EXISTS posts (
        id SERIAL PRIMARY KEY,
        postid VARCHAR(255) NOT NULL,
        role VARCHAR(255) NOT NULL
    );
    `);

  await dbclient.query(`
    CREATE TABLE IF NOT EXISTS firehosecheckpoint (
        id SERIAL PRIMARY KEY,
        cursor VARCHAR(255) NOT NULL,
        ts TIMESTAMP NOT NULL
    );
    `);

  let cursorFirehose = 0;
  let cursorFirehoseTs = "";

  // Cycles through 0-9, and any time it hits 0, update display name with approximate label delay.
  // In effect, every 10 minutes, the display name will reflect how far behind the labeler is
  let updateDisplayNameTicker = 0;

  // get last known cursor if exists
  const lastKnownCheckpoint = await dbclient.query(`
        SELECT ts, cursor FROM firehosecheckpoint
        ORDER BY ts DESC
        LIMIT 1;
    `);
  let lastCursor;
  const lastKnownCheckpointObj = lastKnownCheckpoint.rows[0] || null;
  if (lastKnownCheckpointObj !== null) {
    let lastTs = lastKnownCheckpointObj.ts;
    lastCursor = lastKnownCheckpointObj.cursor;
    console.log(`Last checkpoint added at ${lastTs} at cursor: ${lastCursor}`);
  } else {
    console.log("No known last checkpoint, just gonna... start.");
  }

  const firehose = new Firehose({ cursor: lastCursor ?? "" });

  firehose.on("error", ({ cursor, error }) => {
    console.log(`Firehose errored on cursor: ${cursor}`, error);
    throw new Error("EXITING DUE TO FIREHOSE ERROR");
  });

  firehose.on("open", () => {
    const updateDisplayName = async () => {
      // I would prefer to use agent.upsertProfile(), but this consistently throws an error: Record/avatar should be a blob ref
      // so I'm just re-doing this myself
      // First get current profile info:
      if (!agent.session) {
        throw new Error("Not logged in");
      }
      const existing = await agent.com.atproto.repo
        .getRecord({
          repo: agent.session.did,
          collection: "app.bsky.actor.profile",
          rkey: "self",
        })
        .catch((_) => undefined);
      if (!existing) {
        throw new Error("No profile somehow");
      }
      const newRecord: AppBskyActorProfile.Record = { ...existing.data.value };
      // newRecord.displayName = `SonaSky [Currently @${cursorFirehoseTs}|Delay~= ${dayjs(cursorFirehoseTs).fromNow(true)}]`;
      let newDisplayName = "SonaSky";
      // only show the delay in the display name if more than 5 minutes
      if (dayjs().diff(dayjs(cursorFirehoseTs), "minute") > 5) {
        newDisplayName = `SonaSky [Delayed by ~${dayjs(
          cursorFirehoseTs
        ).fromNow(true)}]`;
      }
      newRecord.displayName = newDisplayName;
      //             newRecord.description = `Show off your fursona (label)!
      // ❓: Find a species post, and like it! To remove, unlike it.
      // 🔍: Browse/Species Request Instructions: https://sonasky-browse.bunnys.ky/ <-- PLEASE READ/POR FAVOR LEIA

      // 🐇🧑‍💻: @astra.bunnys.ky <- 18+
      // 🖼️: @snowfox.gay <- 18+`;
      newRecord.description = `Show off what you do!
❓: Find a role post, and like it! To remove, unlike it.

Automation provided by @sonasky.bsky.social :
🐇🧑‍💻: @astra.bunnys.ky <- 18+

Labeler managed by Deckard:
🦌: @deckardholiday.audioelk.com <- 18+

Cursor @ ${cursorFirehoseTs.split(".")[0]}Z, Delays ~= ${dayjs(
        cursorFirehoseTs
      ).fromNow(true)}`;
      await agent.com.atproto.repo.putRecord({
        repo: agent.session.did,
        collection: "app.bsky.actor.profile",
        rkey: "self",
        record: newRecord,
        swapRecord: existing?.data.cid || null,
      });
    };
    updateDisplayName();
    updateDisplayNameTicker = (updateDisplayNameTicker + 1) % 10; // update ticker;

    setInterval(() => {
      const timestamp = new Date().toISOString();
      console.log(
        `${timestamp} cursor: ${cursorFirehose} (which is ~${cursorFirehoseTs})`
      );
      getOzoneCurrentPolicies(); // trigger dump to file if enabled
      try {
        if (!fs.existsSync(GENERATE_JSON_FILE_OF_SPECIES__DIR)) {
          fs.mkdirSync(GENERATE_JSON_FILE_OF_SPECIES__DIR, { recursive: true });
        }
        fs.writeFile(
          `${GENERATE_JSON_FILE_OF_SPECIES__DIR}/cursortime.txt`,
          cursorFirehoseTs,
          function (err) {
            if (err) {
              console.log(err);
            }
          }
        );
      } catch { }
      dbclient
        .query(
          `
                insert into firehosecheckpoint (cursor,ts) VALUES ($1, current_timestamp) RETURNING *;
            `,
          [cursorFirehose.toString()]
        )
        .then((res) => {
          console.log(`Logged to checkpoint table: ${JSON.stringify(res)}`);
        })
        .catch((err) => console.error(err));

      // Delay ticker logic
      if (updateDisplayNameTicker === 0) {
        updateDisplayName();
      }
      updateDisplayNameTicker = (updateDisplayNameTicker + 1) % 10; // update ticker;
    }, 60000); // Every minute, store checkpoint in db; may raise this in the future.
  });

  firehose.on("commit", async (commit) => {
    cursorFirehose = commit.seq;
    cursorFirehoseTs = commit.time;
    commit.ops.forEach(async (op: RepoOp) => {
      try {
        if (op.path.includes(".like/")) {
          const dbrow: any = {
            did: commit.repo,
            ts: commit.time,
            likepath: op.path,
          };
          if (op.action === "create") {
            if (!JSON.stringify(op).includes(ozone_service_user_did)) {
              return;
            }
            dbrow["posturi"] = ((op as any).record.subject as any).uri;
            // check if post ID is in sonaposts table in db; if so, get the ID from there to save on API calls and slow down rate limiting
            const knownSonaPostsQuery = `SELECT postid, role FROM posts
WHERE postid like $1
LIMIT 1;`;
            const sonapostid = dbrow.posturi.split("/").slice(-1).toString();
            const knownSonaPosts = await dbclient.query(
              knownSonaPostsQuery,
              [sonapostid]
            );
            let sonaPostDefined =
              knownSonaPosts.rows.filter((j) => j.postid === dbrow.posturi.split("/").slice(-1))
                .length === 1;
            let speciesLabelId;
            let safe_species;
            if (sonaPostDefined) {
              speciesLabelId = knownSonaPosts.rows.filter((j) => j.postid === dbrow.posturi.split("/").slice(-1))[0].species;
            }
            if (speciesLabelId === undefined) {
              let p = await agent.getPost({
                repo: `${dbrow.posturi.replace("at://", "").split("/")[0]}`,
                rkey: `${dbrow.posturi.split("/").slice(-1).join("/")}`,
              });
              let post_role_text = p.value.text;
              if (
                !post_role_text.startsWith("Role: ") &&
                !post_role_text.startsWith("Meta: ")
              ) {
                return;
              }
              let species = post_role_text
                .replace("Role: ", "")
                .replace("Meta: ", "")
                .split("//")[0]
                .trim()
                .replace(/ /g, "-")
                .toLowerCase();
              safe_species = species.replace("'", "");
              addLabelIfNotInOzoneCurrentPolicies(
                species,
                safe_species,
                `${dbrow.posturi.split("/").slice(-1).join("/")}`,
                post_role_text.startsWith("Meta: ")
              );
              const insertResult = await dbclient.query(
                `
                              INSERT INTO roles (did, likepath, posturi, role, ts) VALUES ($1, $2, $3, $4, $5) RETURNING *;
                            `,
                [dbrow.did, dbrow.likepath, dbrow.posturi, safe_species, dbrow.ts]
              );
              console.log("Inserted:", insertResult.rows[0]);
            } else {
              safe_species = speciesLabelId;
            }
            await updateDidRecordOzone(dbrow.did, "n/a", "add", safe_species);
          } else {
            // removed
            try {
              const selectResult = await dbclient.query(
                `
                                SELECT * FROM roles WHERE likepath = $1 AND did = $2;
                              `,
                [dbrow.likepath, dbrow.did]
              );
              if (selectResult.rows.length > 0) {
                let species_vals = [
                  ...new Set(selectResult.rows.map((i) => i.species)),
                ];
                const deleteResult = await dbclient.query(
                  `
                                    DELETE FROM roles WHERE likepath = $1 AND did = $2 RETURNING *;
                                  `,
                  [dbrow.likepath, dbrow.did]
                );
                console.log("Deleted:", deleteResult.rows[0]);
                species_vals.forEach(async (species_val) => {
                  console.log(species_val);
                  await updateDidRecordOzone(
                    dbrow.did,
                    "",
                    "remove",
                    species_val
                  );
                  const deleteResult = await dbclient.query(
                    `
                                        DELETE FROM roles WHERE role = $1 AND did = $2 RETURNING *;
                                      `,
                    [species_val, dbrow.did]
                  );
                  console.log("Deleted:", deleteResult.rows[0]);
                });
              }
            } catch (err) {
              console.error(err);
            }
          }
        }
      } catch (err) {
        console.error(err);
      }
    });
  });

  firehose.start();
}

main().catch((err) => {
  throw Error(`Need to restart... ${err}`);
});
