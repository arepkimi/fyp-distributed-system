import os
import time
import io
import base64
import zipfile
from flask import Flask, render_template, request, send_file, redirect, url_for
import grpc
import photo_pb2
import photo_pb2_grpc
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

# IMPORT YOUR SEPARATE CORE ENGINE MODULE HERE
import monolith 

app = Flask(__name__)

SESSION_CACHE = {
    'processed_zip_bytes': None,
    'total_time': 0,
    'metrics': {},
    'batch_results': []
}

# High-speed memory cache array to track incoming finished images from the network loop
DISTRIBUTED_MEMORY_CACHE = {}

# The Dashboard only needs to talk directly to the entry point (Stage 1)
BW_WORKER_ADDR = os.getenv('BW_ADDR', 'dns:///bw-dns-service:50051')

# -----------------------------------------------------------------------
# SETUP GLOBAL ROUND-ROBIN CHANNEL POOL FOR DISTRIBUTED SCALE
# -----------------------------------------------------------------------
GRPC_ROUND_ROBIN_CONFIG = '{"loadBalancingConfig": [{"round_robin": {}}]}'

global_channel = grpc.insecure_channel(
    BW_WORKER_ADDR,
    options=[("grpc.service_config", GRPC_ROUND_ROBIN_CONFIG)]
)
global_stub = photo_pb2_grpc.PhotoProcessorStub(global_channel)


def push_to_assembly_line_shared(filename, image_bytes):
    """
    Uses the persistent global round-robin stub to distribute images
    evenly across all available scale replicas.
    """
    try:
        metadata = (('filename', filename),)
        resp = global_stub.Process(photo_pb2.PhotoRequest(image_data=image_bytes), metadata=metadata, timeout=10)
        
        if resp.processed_data == b"ACK_BW":
            return True
    except grpc.RpcError as e:
        print(f"Failed to split-route {filename} onto assembly line: {e}")
    return False


# -----------------------------------------------------------------------
# NEW: LIGHTWEIGHT HTTP RECEIVER ENDPOINT (Reuses existing Port 5000)
# -----------------------------------------------------------------------
@app.route('/receiver', methods=['POST'])
def receive_completed_image():
    if 'image' in request.files and 'filename' in request.form:
        file = request.files['image']
        filename = request.form['filename']
        # Intercept bytes directly from the network cable into cluster RAM
        DISTRIBUTED_MEMORY_CACHE[filename] = file.read()
        return "ACK_RECEIVE", 200
    return "BAD_REQUEST", 400


@app.route('/')
def index():
    return render_template('index.html')

@app.route('/process', methods=['POST'])
def process():
    mode = request.form.get('mode')
    uploaded_file = request.files['image']
    file_content = uploaded_file.read()
    
    SESSION_CACHE['batch_results'] = []
    SESSION_CACHE['processed_zip_bytes'] = None
    files_to_process = []
    
    if uploaded_file.filename.lower().endswith('.zip'):
        with zipfile.ZipFile(io.BytesIO(file_content)) as incoming_zip:
            for f in incoming_zip.namelist():
                if f.lower().endswith(('.png', '.jpg', '.jpeg')):
                    clean_filename = os.path.basename(f)
                    if clean_filename:
                        files_to_process.append({'filename': clean_filename, 'bytes': incoming_zip.read(f)})
    else:
        files_to_process.append({'filename': uploaded_file.filename, 'bytes': file_content})

    # Flush the memory matrix before launching a distributed batch test
    if mode == 'distributed':
        DISTRIBUTED_MEMORY_CACHE.clear()

    start_time = time.time()
    processed_outputs = []

    try:
        # EXECUTION MANAGEMENT ROUTER
        if mode == 'distributed':
            # Fire data pipeline downstream to Stage 1
            with ThreadPoolExecutor(max_workers=20) as executor:
                futures = [executor.submit(push_to_assembly_line_shared, f['filename'], f['bytes']) for f in files_to_process]
                results = [f.result() for f in futures]

            expected_count = len(files_to_process)
            timeout = 45  
            check_interval = 0.05  

            elapsed = 0
            start_poll = time.time()

            # --- FIXED: POLL CLUSTER RAM MEMORY METRIC, NOT LOCAL MOUNT DISK ---
            while len(DISTRIBUTED_MEMORY_CACHE) < expected_count and elapsed < timeout:
                time.sleep(check_interval)
                elapsed = time.time() - start_poll

            # Map compiled memory pieces back to output arrays sequentially
            for item in files_to_process:
                filename = item['filename']
                if filename in DISTRIBUTED_MEMORY_CACHE:
                    processed_outputs.append(DISTRIBUTED_MEMORY_CACHE[filename])
                else:
                    processed_outputs.append(None)
                
        elif mode == 'parallel':
            num_cores = 3
            def chunkify(lst, n):
                return [lst[i::n] for i in range(n)]
            
            file_chunks = chunkify(files_to_process, num_cores)
            
            with ProcessPoolExecutor(max_workers=num_cores) as executor:
                futures = [executor.submit(monolith.process_batch_of_images, chunk) for chunk in file_chunks]
                chunk_outputs = [f.result() for f in futures]
                processed_outputs = [None] * len(files_to_process)
                for chunk_idx, chunk_res in enumerate(chunk_outputs):
                    for item_idx, out_bytes in enumerate(chunk_res):
                        original_index = item_idx * num_cores + chunk_idx
                        if original_index < len(files_to_process):
                            processed_outputs[original_index] = out_bytes
        else:
            processed_outputs = [monolith.process_image_to_bytes(f['bytes']) for f in files_to_process]

        # Packaging outcomes back into the Session State Memory
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as clean_zip:
            for item, out_bytes in zip(files_to_process, processed_outputs):
                if out_bytes is None: continue
                
                filename = item['filename']
                clean_zip.writestr(f"standardized_{filename}", out_bytes)
                
                b64_str = base64.b64encode(out_bytes).decode('utf-8')
                SESSION_CACHE['batch_results'].append({'filename': filename, 'data': b64_str})

        total_time = round(time.time() - start_time, 4)
        zip_buffer.seek(0)
        
        SESSION_CACHE['processed_zip_bytes'] = zip_buffer.getvalue()
        SESSION_CACHE['total_time'] = total_time
        SESSION_CACHE['metrics'] = {
            'count': len(SESSION_CACHE['batch_results']),
            'throughput': round(len(SESSION_CACHE['batch_results']) / total_time, 2) if total_time > 0 else 0,
            'mode': mode
        }

        return render_template('results.html', 
                               results=SESSION_CACHE['batch_results'], 
                               total_time=total_time, 
                               mode=mode, 
                               count=len(SESSION_CACHE['batch_results']))

    except (grpc.RpcError, Exception) as infrastructure_fault:
        return render_template('error.html', error_msg=str(infrastructure_fault)), 503

@app.route('/update_metrics', methods=['POST'])
def update_metrics():
    try:
        data = request.get_json() or {}
        js_time = float(data.get('e2e_time', SESSION_CACHE['total_time']))
        SESSION_CACHE['total_time'] = js_time
        if 'metrics' in SESSION_CACHE and SESSION_CACHE['metrics'].get('count', 0) > 0:
            count = SESSION_CACHE['metrics']['count']
            SESSION_CACHE['metrics']['throughput'] = round(count / js_time, 2)
        return {"status": "success"}, 200
    except Exception:
        return {"status": "ignored"}, 200

@app.route('/analysis')
def analysis():
    return render_template('analysis.html', metrics=SESSION_CACHE['metrics'], total_time=SESSION_CACHE['total_time'])

@app.route('/download-results')
def download_results():
    if not SESSION_CACHE['processed_zip_bytes']:
        return redirect(url_for('index'))
    return send_file(io.BytesIO(SESSION_CACHE['processed_zip_bytes']), mimetype='application/zip', as_attachment=True, download_name='standardized_dataset.zip')

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
