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

WORKERS = {
    'B&W': os.getenv('BW_ADDR', 'localhost:50051'),
    'Brighten': os.getenv('BRIGHT_ADDR', 'localhost:50052'),
    'Blur': os.getenv('BLUR_ADDR', 'localhost:50053')
}

def call_worker(addr, image_data):
    with grpc.insecure_channel(addr) as channel:
        stub = photo_pb2_grpc.PhotoProcessorStub(channel)
        resp = stub.Process(photo_pb2.PhotoRequest(image_data=image_data), timeout=5)
        return resp.processed_data

def run_distributed_chain(image_bytes):
    current_data = image_bytes
    for name, addr in WORKERS.items():
        processed = call_worker(addr, current_data)
        if processed:
            current_data = processed
        else:
            raise grpc.RpcError(f"Worker Node failure encountered at: {name}")
    return current_data

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
                    files_to_process.append({'filename': f, 'bytes': incoming_zip.read(f)})
    else:
        files_to_process.append({'filename': uploaded_file.filename, 'bytes': file_content})

    start_time = time.time()
    processed_outputs = []

    try:
        # EXECUTION MANAGEMENT ROUTER
        # --- Inside @app.route('/process', methods=['POST']) ---
        if mode == 'distributed':
            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = [executor.submit(run_distributed_chain, f['bytes']) for f in files_to_process]
                processed_outputs = [f.result() for f in futures]
        elif mode == 'parallel':
            raw_bytes_list = [f['bytes'] for f in files_to_process]
            with ProcessPoolExecutor() as executor:
                processed_outputs = list(executor.map(monolith.process_image_to_bytes, raw_bytes_list))
        else:
            # CORRECTED: Loop using the exact updated function name from monolith.py
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