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
# =======================================================================
os.environ["OMP_NUM_THREADS"] = "1"
os.environ["MKL_NUM_THREADS"] = "1"
os.environ["OPENBLAS_NUM_THREADS"] = "1"
os.environ["VECLIB_MAXIMUM_THREADS"] = "1"
os.environ["NUMEXPR_NUM_THREADS"] = "1"

import cv2
cv2.setNumThreads(0) 

import monolith 

app = Flask(__name__)

GLOBAL_PARALLEL_EXECUTOR = ProcessPoolExecutor(max_workers=3)

SESSION_CACHE = {
    'processed_zip_bytes': None,
    'total_time': 0,
    'metrics': {},
    'batch_results': []
}

DISTRIBUTED_MEMORY_CACHE = {}

BW_WORKER_ADDR = os.getenv('BW_ADDR', 'dns:///bw-dns-service:50051')
GRPC_ROUND_ROBIN_CONFIG = '{"loadBalancingConfig": [{"round_robin": {}}]}'


def push_to_assembly_line_shared(filename, image_bytes, stub):
    try:
        metadata = (('filename', filename),)
        resp = stub.Process(photo_pb2.PhotoRequest(image_data=image_bytes), metadata=metadata, timeout=10)
        
        if resp.processed_data == b"ACK_BW":
            return True
    except grpc.RpcError as e:
        print(f"Failed to split-route {filename} onto assembly line: {e}")
    return False


@app.route('/receiver', methods=['POST'])
def receive_completed_image():
    if 'image' in request.files and 'filename' in request.form:
        file = request.files['image']
        filename = request.form['filename']
        DISTRIBUTED_MEMORY_CACHE[filename] = file.read()
        return "ACK_RECEIVE", 200
    return "BAD_REQUEST", 400


@app.route('/')
def index():
    return render_template('menu.html')


@app.route('/benchmark-engine')
def benchmark_engine():
    return render_template('index.html')


# 🌟 FIXED: REAL LIVE TERMINAL PARSER FOR GKE PODS
@app.route('/cluster-management')
def cluster_management():
    live_containers = []
    try:
        # Run a standard raw get pods command—this is foolproof and matches your exact terminal printout
        result = subprocess.run(['kubectl', 'get', 'pods'], capture_output=True, text=True, check=True)
        lines = result.stdout.strip().split('\n')
        
        if len(lines) > 1:
            # Skip the header line ("NAME READY STATUS RESTARTS AGE")
            for line in lines[1:]:
                if not line.strip():
                    continue
