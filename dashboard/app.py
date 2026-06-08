import os
import time
import io
import base64
import zipfile
import subprocess
import re
import ssl
import json
import urllib.request
from flask import Flask, render_template, request, send_file, redirect, url_for
import grpc
import photo_pb2
import photo_pb2_grpc
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

# to restrict multi-threading engines to 1 single thread.
os.environ["OMP_NUM_THREADS"] = "1"
os.environ["MKL_NUM_THREADS"] = "1"
os.environ["OPENBLAS_NUM_THREADS"] = "1"
os.environ["VECLIB_MAXIMUM_THREADS"] = "1"
os.environ["NUMEXPR_NUM_THREADS"] = "1"

import cv2
# Force OpenCV's to use exactly 1 thread
cv2.setNumThreads(0) 


import monolith 

app = Flask(__name__)



# This removes the 2-3 second Linux kernel process creation lag


GLOBAL_PARALLEL_EXECUTOR = ProcessPoolExecutor(max_workers=6)

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



#  HTTP RECEIVER ENDPOINT (Reuses existing Port 5000)

@app.route('/receiver', methods=['POST'])
def receive_completed_image():
    if 'image' in request.files and 'filename' in request.form:
        file = request.files['image']
        filename = request.form['filename']
        # Intercept bytes directly from the network cable into cluster RAM
        DISTRIBUTED_MEMORY_CACHE[filename] = file.read()
        return "ACK_RECEIVE", 200
    return "BAD_REQUEST", 400


#  ROUTE TO MENU.HTML
@app.route('/')
def index():
    return render_template('menu.html')


# MAP THE BENCHMARK OPTION 
@app.route('/benchmark-engine')
def benchmark_engine():
    return render_template('index.html')


#  READ LIVE CLUSTER POD DATA DIRECTLY
@app.route('/cluster-management')
def cluster_management():
    live_containers = []
    
    # GKE container environment structural token paths
    token_path = "/var/run/secrets/kubernetes.io/serviceaccount/token"
    namespace_path = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
    
    if os.path.exists(token_path) and os.path.exists(namespace_path):
        try:
            with open(token_path, "r") as f:
                token = f.read().strip()
            with open(namespace_path, "r") as f:
                namespace = f.read().strip()
                
            # Establish raw connection directly to the internal Kubernetes control engine 
            url = f"https://kubernetes.default.svc/api/v1/namespaces/{namespace}/pods"
            req = urllib.request.Request(url)
            req.add_header("Authorization", f"Bearer {token}")
            
            # Trust internal routing loops safely
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            
            with urllib.request.urlopen(req, context=ctx, timeout=5) as response:
                data = json.loads(response.read().decode())
                
                for item in data.get("items", []):
                    metadata = item.get("metadata", {})
                    status_block = item.get("status", {})
                    
                    pod_name = metadata.get("name")
                    
                    # 🌟 FIX: Explicitly check if a deletion timestamp exists to capture "Terminating" pods
                    if metadata.get("deletionTimestamp") is not None:
                        pod_status = "Terminating"
                    else:
                        pod_status = status_block.get("phase", "Unknown")
                    
                    # Parse dynamic system string values down to readable uptime loops
                    start_time_str = status_block.get("startTime")
                    pod_age = "Active"
                    if start_time_str:
                        try:
                            clean_time = start_time_str.replace("Z", "")
                            start_ts = time.mktime(time.strptime(clean_time.split(".")[0], "%Y-%m-%dT%H:%M:%S"))
                            diff_seconds = int(time.time() - start_ts)
                            
                            if diff_seconds < 60:
                                pod_age = f"{diff_seconds}s"
                            elif diff_seconds < 3600:
                                pod_age = f"{diff_seconds // 60}m"
                            else:
                                pod_age = f"{diff_seconds // 3600}h"
                        except Exception:
                            pod_age = "Active"

                    live_containers.append({
                        'name': pod_name,
                        'status': pod_status,
                        'age': pod_age
                    })
        except Exception as e:
            print(f"[REST API ERROR] Internal API query rejected: {e}")
            live_containers = []

    return render_template('cluster.html', containers=live_containers)


#  REAL-TIME INDEPENDENT CONTAINER DELETION 
@app.route('/nuke-target', methods=['POST'])
def nuke_target():
    target_pod = request.form.get('pod_name', '')
    token_path = "/var/run/secrets/kubernetes.io/serviceaccount/token"
    namespace_path = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
    
    if target_pod and os.path.exists(token_path) and os.path.exists(namespace_path):
        try:
            with open(token_path, "r") as f:
                token = f.read().strip()
            with open(namespace_path, "r") as f:
                namespace = f.read().strip()
                
            url = f"https://kubernetes.default.svc/api/v1/namespaces/{namespace}/pods/{target_pod}"
            req = urllib.request.Request(url, method="DELETE")
            req.add_header("Authorization", f"Bearer {token}")
            
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            
            with urllib.request.urlopen(req, context=ctx, timeout=5) as response:
                print(f"[ORCHESTRATION] API server accepted deletion stream for pod: {target_pod}")
        except Exception as e:
            print(f"[ORCHESTRATION FAULT] REST engine deletion call failed: {e}")
            
    return redirect(url_for('cluster_management'))


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

    if mode == 'distributed':
        DISTRIBUTED_MEMORY_CACHE.clear()

    start_time = time.time()
    processed_outputs = []

    try:
        if mode == 'distributed':
            with grpc.insecure_channel(BW_WORKER_ADDR, options=[("grpc.service_config", GRPC_ROUND_ROBIN_CONFIG)]) as batch_channel:
                batch_stub = photo_pb2_grpc.PhotoProcessorStub(batch_channel)
                
                with ThreadPoolExecutor(max_workers=3) as executor:
                    futures = [executor.submit(push_to_assembly_line_shared, f['filename'], f['bytes'], batch_stub) for f in files_to_process]
                    results = [f.result() for f in futures]

            expected_count = len(files_to_process)
            timeout = 45  
            check_interval = 0.05  

            elapsed = 0
            start_poll = time.time()

            while len(DISTRIBUTED_MEMORY_CACHE) < expected_count and elapsed < timeout:
                time.sleep(check_interval)
                elapsed = time.time() - start_poll

            for item in files_to_process:
                filename = item['filename']
                if filename in DISTRIBUTED_MEMORY_CACHE:
                    processed_outputs.append(DISTRIBUTED_MEMORY_CACHE[filename])
                else:
                    processed_outputs.append(None)
                
        elif mode == 'parallel':
            num_cores = 6
            def chunkify(lst, n):
                return [lst[i::n] for i in range(n)]
            
            file_chunks = chunkify(files_to_process, num_cores)
            
            futures = [GLOBAL_PARALLEL_EXECUTOR.submit(monolith.process_batch_of_images, chunk) for chunk in file_chunks]
            chunk_outputs = [f.result() for f in futures]
            processed_outputs = [None] * len(files_to_process)
            for chunk_idx, chunk_res in enumerate(chunk_outputs):
                for item_idx, out_bytes in enumerate(chunk_res):
                    original_index = item_idx * num_cores + chunk_idx
                    if original_index < len(files_to_process):
                        processed_outputs[original_index] = out_bytes
        else:
            for f in files_to_process:
                out_bytes = monolith.process_image_to_bytes(f['bytes'])
                processed_outputs.append(out_bytes)

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
    grafana_ip = os.environ.get('GRAFANA_EXTERNAL_IP', '34.126.99.181')
    dashboard_uid = os.environ.get('GRAFANA_DASHBOARD_UID', 'g24dbj')
    
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
