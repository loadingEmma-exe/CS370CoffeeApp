"""CS 370 teaching example. Local demo only; no authentication or production guarantees."""
import json, os, time
from flask import Flask, request, jsonify, render_template_string
import psycopg
from psycopg.rows import dict_row
import redis
from kafka import KafkaProducer, KafkaConsumer

DSN = os.environ['DATABASE_URL']
BROKER = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'w5-kafka:9092')
TOPIC = 'w5-orders'
cache = redis.Redis(host='w5-redis', decode_responses=True,
                    socket_connect_timeout=1, socket_timeout=1)
app = Flask(__name__)

def db():
    return psycopg.connect(DSN, row_factory=dict_row, connect_timeout=3)

def initialize():
    for attempt in range(120):
        try:
            with db() as conn:
                conn.execute('''CREATE TABLE IF NOT EXISTS orders (
                    id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    item text NOT NULL, status text NOT NULL DEFAULT 'pending')''')
            return
        except Exception:
            time.sleep(2)
    raise RuntimeError('Database did not become available')

def producer():
    return KafkaProducer(bootstrap_servers=BROKER, acks='all', retries=3,
        max_block_ms=10000, request_timeout_ms=10000,
        key_serializer=lambda k: str(k).encode(),
        value_serializer=lambda v: json.dumps(v).encode())

@app.get('/health')
def health():
    return 'ok'

@app.get('/')
def home():
    return render_template_string('''<!doctype html><title>CS 370 Order Demo</title>
    <style>body{font:20px system-ui;max-width:850px;margin:45px auto;padding:20px}
    button,input{font:inherit;padding:8px}pre{white-space:pre-wrap;background:#eee;padding:20px}</style>
    <h1>Our small order system</h1><p>Web app · PostgreSQL · Kafka · Redis</p>
    <input id="item" value="Coffee"><button onclick="place()">Place order</button>
    <p>Order ID: <input id="oid" type="number" value="1" style="width:85px">
    <button onclick="lookup()">Look up order</button></p><pre id="result">Place an order to begin.</pre>
    <p>Look up the same order twice. Watch the source change from database to cache.</p>
    <script>
    async function place(){let r=await fetch('/orders',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({item:document.getElementById('item').value})});let d=await r.json();
    if(d.id)document.getElementById('oid').value=d.id;document.getElementById('result').textContent=JSON.stringify(d,null,2);}
    async function lookup(){let r=await fetch('/orders/'+document.getElementById('oid').value);
    document.getElementById('result').textContent=JSON.stringify(await r.json(),null,2);}
    </script>''')

@app.post('/orders')
def place_order():
    item = str((request.get_json(silent=True) or {}).get('item', '')).strip()
    if not item or len(item) > 80:
        return jsonify(error='Use an item name of 1 to 80 characters'), 400
    with db() as conn:
        row = conn.execute('INSERT INTO orders(item) VALUES (%s) RETURNING *', (item,)).fetchone()
    # Teaching simplification: DB commit and Kafka send are two separate operations.
    # A production system needs a reliable bridge (e.g. an outbox) and retry handling.
    p = None
    try:
        p = producer()
        p.send(TOPIC, key=row['id'], value={'order_id': row['id']}).get(timeout=15)
        return jsonify(**row, event_published=True), 201
    except Exception:
        app.logger.exception('Order saved but event publication failed')
        return jsonify(**row, event_published=False,
                       warning='Order saved, but worker was not notified. Ask the instructor.'), 503
    finally:
        if p:
            p.close(timeout=2)

@app.get('/orders/<int:oid>')
def get_order(oid):
    key = f'order:{oid}'
    try:
        saved = cache.get(key)
        if saved:
            return jsonify(order=json.loads(saved), source='cache')
    except redis.RedisError:
        app.logger.warning('Cache unavailable; reading database')
    with db() as conn:
        row = conn.execute('SELECT * FROM orders WHERE id=%s', (oid,)).fetchone()
    if not row:
        return jsonify(error='Order not found'), 404
    try:
        cache.setex(key, 20, json.dumps(row))
    except redis.RedisError:
        pass
    return jsonify(order=row, source='database')

def worker():
    while True:
        consumer = None
        try:
            consumer = KafkaConsumer(TOPIC, bootstrap_servers=BROKER,
                group_id='w5-packers', auto_offset_reset='earliest',
                enable_auto_commit=False,
                value_deserializer=lambda v: json.loads(v.decode()))
            for message in consumer:
                oid = int(message.value['order_id'])
                time.sleep(2)  # Simulate packing, without email or another external service.
                with db() as conn:
                    conn.execute("UPDATE orders SET status='packed' WHERE id=%s", (oid,))
                try:
                    cache.delete(f'order:{oid}')
                except redis.RedisError:
                    pass  # Existing entries also expire after 20 seconds.
                consumer.commit()
                print(f'Packed order {oid}; committed offset', flush=True)
        except Exception as exc:
            print(f'Worker waiting/retrying: {exc}', flush=True)
            time.sleep(3)
        finally:
            if consumer:
                consumer.close()

if __name__ == '__main__':
    initialize()
    if os.environ.get('ROLE') == 'worker':
        worker()
    else:
        app.run(host='0.0.0.0', port=8080, threaded=False)
