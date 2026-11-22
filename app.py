import os
import time

from flask import Flask, request, jsonify
import requests as http_requests

app = Flask(__name__)

store = {}

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
                "value": store[key]["value"],
                "timestamp": store[key]["timestamp"]
            }
        )
    return jsonify({"error": "Key not found"}), 404

@app.route("/data/<key>", methods=["PUT"])
def put_data(key):
    global MODE
    value = request.json.get("value")
    timestamp = time.time()

    if MODE == "CP":
        had_key = key in store
        prev_value = store.get(key)
        replicated_peers = []

        for peer in PEERS:
            try:
                resp = http_requests.post(
                    f"http://{peer}/replicate",
                    json={"key": key, "value": value, "timestamp": timestamp},
                    timeout=2,
                )
                if resp.status_code != 200:
                    raise http_requests.exceptions.RequestException(
                        f"Replication to {peer} returned {resp.status_code}"
                    )
                replicated_peers.append(peer)
            except http_requests.exceptions.RequestException:
                # roll back the peers that accepted the write op
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

                return jsonify({
                    "error": f"Replication failed; rolled back {replicated_peers}",
                    "reason": "Write rejected to maintain consistency (CP mode)",
                }), 503

        store[key] = {"value": value, "timestamp": timestamp}
        return jsonify({
            "status": "ok",
            "node": NODE_NAME,
            "key": key,
            "value": value,
            "mode": "CP",
            "message": "All nodes consistent",
            "timestamp": timestamp
        })

    else:
        # AP Mode: Write locally first, replicate best-effort
        store[key] = {"value": value, "timestamp": timestamp}

        replication_results = []
        for peer in PEERS:
            try:
                http_requests.post(
                    f"http://{peer}/replicate",
                    json={"key": key, "value": value, "timestamp": timestamp},
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
            "timestamp": timestamp,
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


@app.route("/replicate", methods=["POST"])
def replicate():
    key = request.json.get("key")
    timestamp = request.json.get("timestamp", time.time())
    value = request.json.get("value")
    # Last-write-wins: only accept if newer
    if key not in store or timestamp >= store[key]["timestamp"]:
        store[key] = {"value": value, "timestamp": timestamp}
    return jsonify({"status": "ok", "node": NODE_NAME})

@app.route("/sync", methods=["POST"])
def sync():
    """Fetch data from all peers and merge using last-write-wins."""
    merged_count = 0
    for peer in PEERS:
        try:
            resp = http_requests.get(f"http://{peer}/data", timeout=2)
            if resp.status_code == 200:
                peer_store = resp.json().get("store", {})
                for key, entry in peer_store.items():
                    peer_timestamp = entry["timestamp"]
                    if key not in store or peer_timestamp > store[key]["timestamp"]:
                        store[key] = entry
                        merged_count += 1
        except http_requests.exceptions.RequestException:
            continue
    return jsonify({
        "status": "ok",
        "node": NODE_NAME,
        "merged_keys": merged_count,
        "store": store,
    })


@app.route("/mode", methods=["GET"])
def get_mode():
    return jsonify({"node": NODE_NAME, "mode": MODE})


@app.route("/mode", methods=["POST"])
def set_mode():
    global MODE
    new_mode = request.json.get("mode", "").upper()
    if new_mode in ("CP", "AP"):
        MODE = new_mode
        return jsonify({"status": "ok", "node": NODE_NAME, "mode": MODE})
    return jsonify({"error": "Mode must be CP or AP"}), 400


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)