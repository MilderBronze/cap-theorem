import os
import time

from flask import Flask, request, jsonify
import requests as http_requests

app = Flask(__name__)

# In-memory key-value store
store = {}

# Configuration from environment
NODE_NAME = os.environ.get("NODE_NAME", "node1")
PEERS = [p for p in os.environ.get("PEERS", "").split(",") if p]
MODE = os.environ.get("MODE", "CP")


@app.route("/data", methods=["GET"])
def get_all_data():
    return jsonify(
        {
            "node": NODE_NAME,
            "mode": MODE,
            "store": store
        }
    )


@app.route("/data/<key>", methods=["GET"])
def get_data(key):
    if key in store:
        return jsonify(
            {
                "node": NODE_NAME,
                "key": key,
                "value": store[key]
            }
        )
    return jsonify({"error": "Key not found"}), 404


@app.route("/data/<key>", methods=["PUT"])
def put_data(key):
    global MODE
    value = request.json.get("value")

    if MODE == "CP":
        had_key = key in store
        prev_value = store.get(key)
        replicated_peers = []

        for peer in PEERS:
            try:
                resp = http_requests.post(
                    f"http://{peer}/replicate",
                    json={"key": key, "value": value},
                    timeout=2,
                )
                if resp.status_code != 200:
                    raise http_requests.exceptions.RequestException(
                        f"Replication to {peer} returned {resp.status_code}"
                    )
                replicated_peers.append(peer)
            except http_requests.exceptions.RequestException:
                # Roll back peers that already accepted the write
                for done_peer in replicated_peers:
                    try:
                        http_requests.post(
                            f"http://{done_peer}/rollback",
                            json={
                                "key": key,
                                "prev_value": prev_value,
                                "had_key": had_key,
                            },
                            timeout=2,
                        )
                    except http_requests.exceptions.RequestException:
                        pass

                # Do NOT write to node1's store
                return jsonify({
                    "error": f"Replication failed; rolled back {replicated_peers}",
                    "reason": "Write rejected to maintain consistency (CP mode)",
                }), 503

        # Only reached if EVERY peer succeeded
        store[key] = value
        return jsonify({
            "status": "ok",
            "node": NODE_NAME,
            "key": key,
            "value": value,
            "mode": "CP",
            "message": "All nodes consistent",
        })

    else:
        # AP Mode: Write locally first, replicate best-effort
        store[key] = value

        replication_results = []
        for peer in PEERS:
            try:
                http_requests.post(
                    f"http://{peer}/replicate",
                    json={"key": key, "value": value},
                    timeout=1,
                )
                replication_results.append({"peer": peer, "status": "replicated"})
            except http_requests.exceptions.RequestException:
                replication_results.append({"peer": peer, "status": "unreachable"})

        return jsonify({
            "status": "ok",
            "node": NODE_NAME,
            "key": key,
            "value": value,
            "mode": "AP",
            "replication": replication_results,
        })

@app.route("/rollback", methods=["POST"])
def rollback():
    key = request.json.get("key")
    prev_value = request.json.get("prev_value")
    had_key = request.json.get("had_key", False)

    if had_key:
        store[key] = prev_value
    else:
        store.pop(key, None)

    return jsonify({"status": "rolled_back", "node": NODE_NAME})