import os
import requests
import urllib
import subprocess
import time

from flask import Flask, jsonify, redirect, request
from flask_cors import CORS

from dotenv import load_dotenv
load_dotenv(override=True)

app = Flask(__name__)
CORS(app)

APPS = {
    "chatbot": "examples/starter-apps/chatbot.py",
    "patient-intake": "examples/starter-apps/patient-intake.py",
    "storybot": "examples/starter-apps/storybot.py",
    "translator": "examples/starter-apps/translator.py"
}

daily_api_key = os.getenv("DAILY_API_KEY")
api_path = os.getenv("DAILY_API_PATH") or "https://api.daily.co/v1"
fly_api_key = os.getenv("FLY_API_KEY")
fly_app_name = os.getenv("FLY_APP_NAME")
fly_headers = {
    'Authorization': f"Bearer {fly_api_key}",
    'Content-Type': 'application/json'
}
fly_api_host = "https://api.machines.dev/v1"

# grab the first machine image for lauching bots
res = requests.get(f"{fly_api_host}/apps/{fly_app_name}/machines", headers=fly_headers)
if res.status_code != 200:
    raise Exception(f"Unable to get machine info from Fly: {res.text}")
image = res.json()[0]['config']['image']
print(f"Image is: {image}")


def get_room_name(room_url):
    return urllib.parse.urlparse(room_url).path[1:]


def create_room(room_properties, exp):
    room_props = {
        "exp": exp,
        "enable_chat": True,
        "enable_emoji_reactions": True,
        "eject_at_room_exp": True,
        "enable_prejoin_ui": False,
        "enable_recording": "cloud"
    }
    if room_properties:
        room_props |= room_properties

    res = requests.post(
        f"{api_path}/rooms",
        headers={"Authorization": f"Bearer {daily_api_key}"},
        json={
            "properties": room_props
        },
    )
    if res.status_code != 200:
        raise Exception(f"Unable to create room: {res.text}")

    room_url = res.json()["url"]
    room_name = res.json()["name"]
    return (room_url, room_name)


def create_token(room_name, token_properties, exp):
    token_props = {"exp": exp, "is_owner": True}
    if token_properties:
        token_props |= token_properties
    # Force the token to be limited to the room
    token_props |= {"room_name": room_name}
    res = requests.post(
        f'{api_path}/meeting-tokens',
        headers={
            'Authorization': f'Bearer {daily_api_key}'},
        json={
            'properties': token_props})
    if res.status_code != 200:
        if res.status_code != 200:
            raise Exception(f"Unable to create meeting token: {res.text}")

    meeting_token = res.json()['token']
    return meeting_token


def start_bot(*, bot_path, room_url, token, bot_args, wait_for_bot):

    room_name = get_room_name(room_url)
    # proc = subprocess.Popen(
    #     [f"python {bot_path} -u {room_url} -t {token} -k {daily_api_key} {bot_args}"],
    #     shell=True,
    #     bufsize=1,
    # )
    cmd = f"python {bot_path} -u {room_url} -t {token} -k {daily_api_key} {bot_args}"
    cmd = cmd.split()
    # cmd = ["pwd"]
    print(f"!!! cmd: {cmd}")
    worker_props = {"config": {
        "image": image,
        "auto_destroy": True,
        "init": {
            "cmd": cmd}
    },
        "restart": {
            "policy": "no"
    },
    }
    res = requests.post(
        fly_api_host + f"/apps/{fly_app_name}/machines",
        headers=fly_headers,
        json=worker_props
    )
    print(f"!!! Got past the request to start a bot")
    if res.status_code != 200:
        raise Exception(f"Problem starting a bot worker: {res.text}")
    print(f"!!! worker creation response: {res.text}")
    if wait_for_bot:
        # Don't return until the bot has joined the room, but wait for at most 5
        # seconds.
        attempts = 0
        while attempts < 50:
            time.sleep(0.1)
            attempts += 1
            res = requests.get(
                f"{api_path}/rooms/{room_name}/get-session-data",
                headers={"Authorization": f"Bearer {daily_api_key}"},
            )
            if res.status_code == 200:
                print(f"Took {attempts} attempts to join room {room_name}")
                return True

        # If we don't break from the loop, that means we never found the bot in the room
        raise Exception("The bot was unable to join the room. Please try again.")

    return True


@app.route("/start/<string:botname>", methods=["GET", "POST"])
def start(botname):
    try:
        if botname not in APPS:
            raise Exception(f"Bot '{botname}' is not in the allowlist.")

        bot_path = APPS[botname]
        props = {
            "room_url": None,
            "room_properties": None,
            "token_properties": None,
            "bot_args": None,
            "wait_for_bot": True,
            "duration": None,
            "redirect": True
        }
        props |= request.values.to_dict()  # gets URL params as well as plaintext POST body
        try:
            props |= request.json
        except BaseException:
            pass
        if props['redirect'] == "false":
            props['redirect'] = False
        if props['wait_for_bot'] == "false":
            props['wait_for_bot'] = False

        duration = int(os.getenv("DAILY_BOT_DURATION") or 7200)
        if props['duration']:
            duration = props['duration']
        exp = time.time() + duration
        if (props['room_url']):
            room_url = props['room_url']
            try:
                room_name = get_room_name(room_url)
            except ValueError:
                raise Exception(
                    "There was a problem detecting the room name. Please double-check the value of room_url.")
        else:
            room_url, room_name = create_room(props['room_properties'], exp)
        token = create_token(room_name, props['token_properties'], exp)
        bot = start_bot(
            room_url=room_url,
            bot_path=bot_path,
            token=token,
            bot_args=props['bot_args'],
            wait_for_bot=props['wait_for_bot'])
        print(f"!!! Bot is: {bot}")
        if props['redirect'] and request.method == "GET":
            return redirect(room_url, 302)
        else:
            return jsonify({"room_url": room_url, "token": token})
    except BaseException as e:
        return f"There was a problem starting the bot: {e}", 500


@app.route("/healthz")
def health_check():
    return "ok", 200
