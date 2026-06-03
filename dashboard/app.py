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

# The Dashboard only needs to talk directly to the entry point (Stage 1)
# CRITICAL: We add 'dns:///' prefix so gRPC resolves individual pod IPs directly
BW_WORKER_ADDR = os.getenv('BW_ADDR', 'dns:///bw-dns-service:50051')

# The shared local output folder where Blur Worker drops completed images
SHARED_OUTPUT_DIR = "/app/shared_output"
os.makedirs(SHARED_OUTPUT_DIR, exist_ok=True)

# -----------------------------------------------------------------------
# NEW: SETUP GLOBAL ROUND-ROBIN CHANNEL POOL FOR DISTRIBUTED SCALE
# -----------------------------------------------------------------------
# This explicit config forces gRPC to rotate pods on EVERY single message payload
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
        # Send filename in metadata so it travels down the line
        metadata = (('filename', filename),)
        # Timeout extended slightly to handle cloud network queue depths comfortably
        resp = global_stub.Process(photo_pb2.PhotoRequest(image_data=image_bytes), metadata=metadata, timeout=10)
        
        if resp.processed_data == b"ACK_BW":
            return True
    except grpc.RpcError as e:
        print(f"Failed to split-route {filename} onto assembly line: {e}")
    return False


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

    if mode == 'distributed':
        for f in os.listdir(SHARED_OUTPUT_DIR):
            file_path = os.path.join(SHARED_OUTPUT_DIR, f)
            if os.path.isfile(file_path):
                os.remove(file_path)

    start_time = time.time()
    processed_outputs = []

    try:
        # EXECUTION MANAGEMENT ROUTER
        if mode == 'distributed':
            # Bumped to 20 threads to saturate your cluster scaling endpoints simultaneously
            with ThreadPoolExecutor(max_workers=20) as executor:
                futures = [executor.submit(push_to_assembly_line_shared, f['filename'], f['bytes']) for f in files_to_process]
                results = [f.result() for f in futures]

            # Wait for the last stage (Blur Worker) to finish writing the images to the shared disk
            expected_count = len(files_to_process)
            timeout = 45  # Extended for larger batches of 1,000 items
            check_interval = 0.05  # Faster polling to minimize dashboard delay metrics

            # --- FIXED: INITIALIZE AND ACCURATELY TRACK ELAPSED TIMER ---
            elapsed = 0
            start_poll = time.time()

            while len(os.listdir(SHARED_OUTPUT_DIR)) < expected_count and elapsed < timeout:
                time.sleep(check_interval)
                elapsed = time.time() - start_poll

            # Read the files back out of the assembly line final station disk space
            for item in files_to_process:
                saved_path = os.path.join(SHARED_OUTPUT_DIR, item['filename'])
                if os.path.exists(saved_path):
                    with open(saved_path, 'rb') as sf:
                        processed_outputs.append(sf.read())
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
