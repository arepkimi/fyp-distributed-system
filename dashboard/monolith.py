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
    img = cv2.GaussianBlur(img, (15, 15), 0)           # Blur Kernel 15x15

    # 3. Re-encode the matrix back into a raw JPG binary byte stream
    _, buffer = cv2.imencode('.jpg', img)
    return buffer.tobytes()