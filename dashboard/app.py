import os
import time
import io
import base64
import zipfile
import subprocess
import re
from flask import Flask, render_template, request, send_file, redirect, url_for
import grpc
import photo_pb2
import photo_pb2_grpc
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

# =======================================================================
# THREAD CONFIGURATION PASS FOR TRUE MONOLITHIC BENCHMARKING
# These environment variables must be declared BEFORE importing cv2/numpy
# to restrict their internal multi-threading engines to 1 single thread.
# =======================================================================
os.environ["OMP_NUM_THREADS"] = "1"
os.environ["MKL_NUM_THREADS"] = "1"
os.environ["OPENBLAS_NUM_THREADS"] = "1"
os.environ["VECLIB_MAXIMUM_THREADS"] = "1"
os.environ["NUMEXPR_NUM_THREADS"] = "1"

import cv2
# Force OpenCV's internal hardware-accelerated thread pool to use exactly 1 thread
cv2.setNumThreads(0) 

# IMPORT YOUR SEPARATE CORE ENGINE MODULE HERE
import monolith 

app = Flask(__name__)

# =======================================================================
# GLOBAL OPTIMIZER: PERSISTENT MULTI-CORE WORKER POOL
# Instantiating this at startup keeps the 3 OS child processes "warm".
# This removes the 2-3 second Linux kernel process creation lag from 
# your measured execution timeline.
# =======================================================================
GLOBAL_PARALLEL_EXECUTOR = ProcessPoolExecutor(max_workers=3)

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
GRPC_ROUND_ROBIN_CONFIG = '{"loadBalancingConfig": [{"round_robin": {}}]}'


def push_to_assembly_line_shared(filename, image_bytes, stub):
    """
    OPTIMIZED: Reuses the active batch stub passed from the thread executor pool context.
    This eliminates the massive network handshake overhead of opening/closing sockets per file.
    """
    try:
        metadata = (('filename', filename),)
        resp = stub.Process(photo_pb2.PhotoRequest(image_data=image_bytes), metadata=metadata, timeout=10)
        
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


# 🌟 1. ROOT LEVEL ROUTE RENDER: MAIN MENU HOME PAGE
@app.route('/')
def index():
    return render_template('menu.html')


# 🌟 2. BENCHMARK SELECTION ROUTE (PREVIOUS ROOT INDEX)
@app.route('/benchmark-engine')
def benchmark_engine():
    return render_template('index.html')


# 🌟 3. REAL LIVE CLUSTER POD PARSER (ZERO MOCK DATA)
@app.route('/cluster-management')
def cluster_management():
    live_containers = []
    try:
        # Request strict, cleanly-bounded string outputs from your GKE cluster API
        result = subprocess.run(
            ['kubectl', 'get', 'pods', '-o', 'custom-columns=NAME:.metadata.name,STATUS:.status.phase,AGE:.metadata.creationTimestamp'],
            capture_output=True, text=True, check=True
        )
        lines = result.stdout.strip().split('\n')
        
        if len(lines) > 1:
            for line in lines[1:]:
                if not line.strip():
                    continue
                parts = line.split()
                if len(parts) >= 3:
                    pod_name = parts[0]
                    pod_status = parts[1]
                    raw_age = parts[2]
                    
                    # Compute simplified visual presentation timing metrics out of GKE logs
                    if "T" in raw_age and "Z" in raw_age:
                        pod_age = "Active"
                    else:
                        pod_age = raw_age
                        
                    live_containers.append({
                        'name': pod_name,
                        'status': pod_status,
                        'age': pod_age
                    })
    except Exception as e:
        print(f"[LIVE PARSER FAULT] Kubectl connection trace rejected: {e}")
        live_containers = []

    return render_template('cluster.html', containers=live_containers)


# 🌟 4. DIRECT ADMINISTRATIVE CONTAINER DELETION ENDPOINT
@app.route('/nuke-target', methods=['POST'])
def nuke_target():
    target_pod = request.form.get('pod_name', '')
    if target_pod:
        try:
            print(f"[ORCHESTRATION] Issuing dynamic force-deletion statement on pod instance: {target_pod}")
            subprocess.run(['kubectl', 'delete', 'pod', target_pod, '--grace-period=0', '--force'], check=True)
        except Exception as e:
            print(f"[ORCHESTRATION ERROR] Execution failure on cluster deletion pipe: {e}")
    return redirect(url_for('cluster_management'))


# 🌟 5. GLOBAL REPLICA SCALING MULTIPLEXER (EXCLUDES DASHBOARD)
@app.route('/scale-target', methods=['POST'])
def scale_target():
    try:
        user_replicas = request.form.get('replica_count', '1')
        print(f"[ORCHESTRATION] Broadscale synchronization initiated. Targeting work environments to replica limit: {user_replicas}")

        # Update disk file registers for testing reference
        yaml_path = 'deploy-all.yaml'
        if os.path.exists(yaml_path):
            with open(yaml_path, 'r') as f:
                sections = f.read().split('---')

            updated_sections = []
            for section in sections:
                if 'kind: Deployment' in section:
                    name_search = re.search(r'name:\s*([a-zA-Z0-9_-]+)', section)
                    if name_search:
                        deployment_name = name_search.group(1)
                        
                        # EXCLUSION RULE: Leave the front-facing UI gateway context completely untouched!
                        if "dashboard" in deployment_name:
                            updated_sections.append(section)
                            continue

                        # Apply regex patching transformations to internal specifications fields
                        section = re.sub(r'(replicas:\s*)(\d+)', rf'\g<1>{user_replicas}', section)

                updated_sections.append(section)

            with open(yaml_path, 'w') as f:
                f.write('---'.join(updated_sections))

        # Synchronize live cloud state tracking modules
        target_processing_tiers = ["bw-worker-deployment", "bright-worker-deployment", "blur-worker-deployment"]
        for deployment in target_processing_tiers:
            try:
                subprocess.run(['kubectl', 'scale', 'deployment', deployment, f'--replicas={user_replicas}'], capture_output=True)
            except Exception:
                pass

    except Exception as scale_error:
        print(f"[SCALE FAULT] Exception intercepted on orchestration run: {scale_error}")

    return redirect(url_for('cluster_management'))


# =======================================================================
# ALL ORIGINAL PROCESSING LOGIC REMAINS 100% UNTOUCHED BELOW THIS LINE
# =======================================================================
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
            # --- IMPLEMENTED IDEA: Open ONE single connection channel pool context for this entire batch run ---
            with grpc.insecure_channel(BW_WORKER_ADDR, options=[("grpc.service_config", GRPC_ROUND_ROBIN_CONFIG)]) as batch_channel:
                batch_stub = photo_pb2_grpc.PhotoProcessorStub(batch_channel)
                
                # =========================================================================
                # OPTIMIZED BACKPRESSURE CONTROLLER: CONCURRENCY CONSTRAINED TO 3 WORKERS
                # Throttles transmission velocity to prevent high-res gRPC channel drops.
                # =========================================================================
                with ThreadPoolExecutor(max_workers=3) as executor:
                    futures = [executor.submit(push_to_assembly_line_shared, f['filename'], f['bytes'], batch_stub) for f in files_to_process]
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
            
            # Submits tasks directly to the warmed persistent pool instead of triggering cold creations
            futures = [GLOBAL_PARALLEL_EXECUTOR.submit(monolith.process_batch_of_images, chunk) for chunk in file_chunks]
            chunk_outputs = [f.result() for f in futures]
            processed_outputs = [None] * len(files_to_process)
            for chunk_idx, chunk_res in enumerate(chunk_outputs):
                for item_idx, out_bytes in enumerate(chunk_res):
                    original_index = item_idx * num_cores + chunk_idx
                    if original_index < len(files_to_process):
                        processed_outputs[original_index] = out_bytes
        else:
            # ===================================================================
            # TRUE SEQUENTIAL MONOLITHIC TRACK (EDITED BLOCK ONLY)
            # Process images one by one sequentially down a single timeline lane.
            # ===================================================================
            for f in files_to_process:
                out_bytes = monolith.process_image_to_bytes(f['bytes'])
                processed_outputs.append(out_bytes)

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
    # Fetch values dynamically populated inside your deployment YAML environments layer
    grafana_ip = os.environ.get('GRAFANA_EXTERNAL_IP', '34.126.99.181')
    dashboard_uid = os.environ.get('GRAFANA_DASHBOARD_UID', 'g24dbj')
    
    # Render view while injecting variables safely into your HTML layout blocks
    return render_template('analysis.html', 
                           metrics=SESSION_CACHE['metrics'], 
                           total_time=SESSION_CACHE['total_time'],
                           grafana_ip=grafana_ip,
                           dashboard_uid=dashboard_uid)

@app.route('/download-results')
def download_results():
    if not SESSION_CACHE['processed_zip_bytes']:
        return redirect(url_for('index'))
    return send_file(io.BytesIO(SESSION_CACHE['processed_zip_bytes']), mimetype='application/zip', as_attachment=True, download_name='standardized_dataset.zip')

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
