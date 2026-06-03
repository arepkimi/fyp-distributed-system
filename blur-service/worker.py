import os
import cv2
import numpy as np
import grpc
import requests
from concurrent import futures
import photo_pb2
import photo_pb2_grpc

# Resolve the central Flask Dashboard routing address via K8s CoreDNS
DASHBOARD_HTTP_ADDR = os.getenv('DASHBOARD_ADDR', 'http://fyp-dashboard-service:80/receiver')


class PhotoProcessor(photo_pb2_grpc.PhotoProcessorServicer):
    def Process(self, request, context):
        # Read the unique tracking ID from gRPC Metadata headers
        metadata = dict(context.invocation_metadata())
        filename = metadata.get('filename', 'processed_image.jpg')

        nparr = np.frombuffer(request.image_data, np.uint8)
        img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

        if img is None:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            return photo_pb2.PhotoResponse()

        # --- Blur Processing Stage ---
        result = cv2.GaussianBlur(img, (15, 15), 0)
        
        _, buffer = cv2.imencode('.jpg', result)
        image_bytes = buffer.tobytes()

        # -----------------------------------------------------------------------
        # FIXED: LOOP CLOSED OVER INTERNAL HTTP TO ELIMINATE PHYSICAL NODE CO-LOCATION
        # -----------------------------------------------------------------------
        try:
            payload_file = {'image': (filename, image_bytes, 'image/jpeg')}
            payload_data = {'filename': filename}
            
            # Post directly over the cloud data network channel back to the dashboard RAM
            response = requests.post(DASHBOARD_HTTP_ADDR, files=payload_file, data=payload_data, timeout=5)
            if response.status_code == 200:
                print(f"Network Loop Complete! Streamed back: {filename}")
            else:
                print(f"Dashboard rejected image drop payload: {response.status_code}")
        except Exception as network_err:
            print(f"Failed to network-stream {filename} back to gateway node: {network_err}")

        # Return a fast acknowledgment back to the previous worker
        return photo_pb2.PhotoResponse(processed_data=b"ACK_BLUR")


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=20))
    photo_pb2_grpc.add_PhotoProcessorServicer_to_server(PhotoProcessor(), server)
    server.add_insecure_port('[::]:50053')
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    print("Blur Worker (Network Loop Mode) is running on Port 50053, receiving distributed load...")
    serve()
