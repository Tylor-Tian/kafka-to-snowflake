# -*- coding: utf-8 -*-
import json, time, requests, datetime, pytz, os, sys, traceback
from kafka import KafkaProducer

BOOTSTRAP = os.getenv("BOOTSTRAP", "localhost:9092")     # 本机 producer 走 9092
TOPIC     = os.getenv("TOPIC", "events")

CITIES = [
  {"id":"tokyo","lat":35.6762,"lon":139.6503,"region":"JP"},
  {"id":"beijing","lat":39.9042,"lon":116.4074,"region":"CN"},
  {"id":"newyork","lat":40.7128,"lon":-74.0060,"region":"US"},
  {"id":"london","lat":51.5074,"lon":-0.1278,"region":"UK"},
  {"id":"sydney","lat":-33.8688,"lon":151.2093,"region":"AU"},
]

def now_utc():
    return datetime.datetime.now(tz=pytz.UTC)

def ts():
    return now_utc().strftime("%Y-%m-%d %H:%M:%S UTC")

def log(msg):
    print(f"[{ts()}] {msg}", flush=True)

def fetch_weather(lat, lon):
    url = ("https://api.open-meteo.com/v1/forecast?"
           f"latitude={lat}&longitude={lon}&current=temperature_2m,relative_humidity_2m,wind_speed_10m")
    r = requests.get(url, timeout=15)
    r.raise_for_status()
    cur = r.json().get("current", {})
    return {
        "temp_c": cur.get("temperature_2m"),
        "humidity": cur.get("relative_humidity_2m"),
        "wind_speed": cur.get("wind_speed_10m")
    }

def main():
    log(f"Starting producer -> {BOOTSTRAP}, topic={TOPIC}")
    producer = KafkaProducer(
        bootstrap_servers=[BOOTSTRAP],
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: v.encode("utf-8"),
        acks=1, linger_ms=0, retries=3
    )

    HEARTBEAT_SEC = 10
    PRODUCE_SEC   = 60
    next_hb   = time.time()
    next_pub  = time.time()
    send_ok_ts = None
    send_err   = None
    i = 0

    try:
        while True:
            now = time.time()
            if now >= next_hb:
                hb = {
                    "alive": True,
                    "bootstrap": BOOTSTRAP,
                    "topic": TOPIC,
                    "last_send_ok": send_ok_ts.strftime("%Y-%m-%d %H:%M:%S") if send_ok_ts else None,
                    "last_error": str(send_err) if send_err else None
                }
                log(f"[HB] {json.dumps(hb, ensure_ascii=False)}")
                next_hb = now + HEARTBEAT_SEC

            if now >= next_pub:
                city = CITIES[i % len(CITIES)]
                i += 1
                try:
                    met = fetch_weather(city["lat"], city["lon"])
                    ts_iso = now_utc().strftime("%Y-%m-%dT%H:%M:%SZ")
                    payload = {
                        "source": "open-meteo",
                        "ts": ts_iso,
                        "city": city["id"],
                        "region": city["region"],
                        "lat": city["lat"], "lon": city["lon"],
                        "temp_c": met.get("temp_c"),
                        "humidity": met.get("humidity"),
                        "wind_speed": met.get("wind_speed")
                    }
                    key = f"wx-{city['id']}-{int(now//60)}"
                    fut = producer.send(TOPIC, value=json.dumps(payload, ensure_ascii=False), key=key)
                    md = fut.get(timeout=10)
                    producer.flush()
                    send_ok_ts = now_utc()
                    send_err   = None
                    log(f"[SEND-OK] key={key} -> partition={md.partition}, offset={md.offset}, payload={payload}")
                except Exception as e:
                    send_err = e
                    log(f"[SEND-ERR] {e.__class__.__name__}: {e}")
                    traceback.print_exc()
                next_pub = now + PRODUCE_SEC

            time.sleep(0.2)
    except KeyboardInterrupt:
        log("Stopping by user...")
    finally:
        try:
            producer.flush(5)
            producer.close(5)
        except Exception:
            pass
        log("Producer closed.")

if __name__ == "__main__":
    try:
        main()
    except Exception as ex:
        log(f"FATAL: {ex}")
        traceback.print_exc()
        sys.exit(1)
