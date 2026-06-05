import cv2
import numpy as np

def process_image_to_bytes(image_bytes):
    """
    Pure Image Processing Pipeline.
    Takes raw binary bytes, applies filters, and returns raw binary bytes.
    Optimized to be safe for both Monolithic sequential loops and Parallel multi-core cloning.
    """
    # 1. Decode raw bytes into an OpenCV Matrix
    nparr = np.frombuffer(image_bytes, np.uint8)
    img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
    if img is None:
        return None

    # 2. Execute Image Standardization Chain (Your exact variables)
    img = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    img = cv2.convertScaleAbs(img, alpha=1.0, beta=50) # Brightness Beta 50
    
    # =========================================================================
    # DYNAMIC KERNEL CALCULATION (THE MATHEMATICAL SCALE FACTOR)
    # Automatically scales the workload to match the resolution of your image.
    # For a standard high-res image (~4000px), this dynamically targets a ~101x101 matrix.
    # =========================================================================
    height, width = img.shape[:2]
    kernel_size = int(max(height, width) * 0.025)
    if kernel_size % 2 == 0:  # OpenCV strictly requires Gaussian kernels to be odd numbers
        kernel_size += 1
        
    img = cv2.GaussianBlur(img, (kernel_size, kernel_size), 0) 

    # 3. Re-encode the matrix back into a raw JPG binary byte stream
    _, buffer = cv2.imencode('.jpg', img)
    return buffer.tobytes()

def process_batch_of_images(image_batch):
    """
    COARSE-GRAINED ACCELERATION LOGIC: Loops through an entire array chunk 
    locally within the assigned child process memory to avoid frequent OS IPC taxes.
    """
    batch_results = []
    for item in image_batch:
        try:
            out_bytes = process_image_to_bytes(item['bytes'])
            batch_results.append(out_bytes)
        except Exception:
            batch_results.append(None)
    return batch_results
