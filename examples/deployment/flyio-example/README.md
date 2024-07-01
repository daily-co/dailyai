# Fly.io deployment example

This project modifies the `bot_runner.py` server to launch a new machine for each user session. This is a recommended approach for production vs. running shell processess as your deployment will quickly run out of system resources under load.

To speed up machine boot times, we also download and cache Silero VAD as part of the Dockerfile (`install_deps.py`). If you are using other custom models, you can add them here too.

For this example, we are using Daily as a WebRTC transport and provisioning a new room and token for each session. You can use another transport, such as WebSockets, by modifying the `bot.py` and `bot_runner.py` files accordingly.

## Setting up your fly.io deployment

### Create your fly.toml file

You can copy the `example-fly.toml` as a reference. Be sure to change the app name to something unique.

### Create your .env file

Copy the base `env.example` to `.env` and enter the necessary API keys. 

`FLYFLY_APP_NAME` should match that in the `fly.toml` file.

### Launch a new fly.io project

`fly launch` or `fly launch --org your-org-name`

### Set the necessary app secrets from your .env

Note: you can do this manually via the fly.io dashboard under the "secrets" sub-section of your deployment (e.g. "https://fly.io/apps/fly-app-name/secrets") or run the following terminal command:

`cat .env | tr '\n' ' ' | xargs flyctl secrets set`

### Deploy your machine

`fly deploy`


## Connecting to your bot

Send a post request to your running fly.io instance:

`curl --location --request POST 'https://YOUR_FLY_APP_NAME/start_bot'`

This request will wait until the machine enters into a `starting` state, before returning the a room URL and token to join.

