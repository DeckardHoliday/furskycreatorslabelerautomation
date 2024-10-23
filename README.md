## This code is forked from SonaSky. The only changes made were ones specific to the needs of the fursky creator labeler.

## View SonaSky Repo: https://github.com/astrabun/sonasky

# SonaSky

> A Bluesky labeler allowing self-assignment of fursona species labels

[Read more about SonaSky](https://astrabun.com/projects/sonasky/)

---

## About

SonaSky is a [vanity](https://en.wikipedia.org/wiki/Vanity_plate) Bluesky labeler (https://bsky.app/profile/sonasky.bsky.social) that allows users to self-assign moderation labels to their ATProto account. Specifically, it was created with the furry fandom in mind. 

Users find a post by the SonaSky account that contains the species name they want to receive as a label, click "like" ❤️, and a bot listening to the Bluesky firehose will see this and apply the appropriate label. This repository covers that bot code.

A web app to browse available labels (because there are a lot of them now) can be found here: https://sonasky-browse.bunnys.ky/

## Setup

Follow the steps for hosting [Ozone](https://github.com/bluesky-social/ozone/blob/main/HOSTING.md), a web interface for labeling content in atproto / Bluesky, to create a labeler. Once created, move on to [Configuration](#configuration).

## Configuration

`$ cp .env.sample .env`, then edit:

```
BSKY_USER=labelservicehandlegoeshere.bsky.social
BSKY_PASS=some-app-password-here
OZONE_SERVICE_USER_DID=did:plc:didofthelabelserviceuserhere
POSTGRES_HOST=db
POSTGRES_USER=sona
POSTGRES_PASSWORD= # generate with: $ openssl rand -hex 32
POSTGRES_DB=sonabot
POSTGRES_PORT=5432
GENERATE_JSON_FILE_OF_SPECIES=false  # if true, will produce a file at /tmp/labels/labels.json
```

## Usage

```
$ docker compose up -d
```

As the labeler account, create one or more posts that start with `Species: ` &nbsp;or `Meta: `&nbsp;; Avoid using special characters as label IDs (at least in my experience) must be lowercase alphanumeric. Once a species label is liked on Bluesky, the bot will see this in the Firehose and 1) create the label with a default display name and description in English, then 2) assign the user that liked the post that label, then 3) add a record to a postgres database with the user's DID, the operation's CID, and the label ID that was assigned to them. The contents of this DB are dual purpose. First and most important, it makes it easy to handle users who want to *remove* a label from their account. Since "unlike" events on the firehose don't contain the DID of the user who performed the action, we must go on the operation's CID value instead, and if we keep that in the DB, we can look for the matching CID and label ID, then remove that from the user in that DB record. Secondly, we can use this DB for something like generating statistics on how many users are using each species label. This isn't nearly as important as the option to opt-out, but it is nice to have.

While on the subject of the DB, it's also used to store checkpoints on where the bot is in the Bluesky firehose. If there is a power failure or network connection interruption, the bot will have a cursor location to use when resuming. In that scenario, the bot will run behind real time until it manages to catch up to the present. The DB also stores the Bluesky post IDs for each label which is used to make navigation to posts easier from the SonaSky-browse web app. 

## Localization

To contribute to localization of SonaSky labels, see [sonasky-labels-def](https://github.com/astrabun/sonasky-labels-def). Labels are added in English by default, but I would love to be inclusive to others and localize to other languages :)

## Credits

SonaSky uses [bluesky-social/ozone](https://github.com/bluesky-social/ozone) to run the labeler itself; I know there are alternatives like `@skyware/labeler` but I kinda like the Ozone UI. Maybe I'll change how I want to run this in the future, but for now, I'm sticking with Ozone. Also, it's a cool name.

Shoutout to [notjuliet/@adorable.mom](https://github.com/notjuliet) for creating [notjuliet/pronouns-bsky](https://github.com/notjuliet/pronouns-bsky) which in part inspired me to create SonaSky! Also, without her pronouns labeler code I wouldn't have known about skyware's firehose client which has helped SonaSky stability greatly.
