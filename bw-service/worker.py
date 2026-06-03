import os
import cv2
import numpy as np
import grpc
from concurrent import futures
import photo_pb2
import photo_pb2_grpc

# Read the next stage address from environment variables (Defaults to local testing port)
NEXT_STAGE_ADDR = os.getenv('BRIGHT_ADDR', 'localhost:50052')

class PhotoProcessor(photo_pb2_grpc.PhotoProcessorServicer):
    def Process(self, request, context):
        # 1. Capture the file tracking metadata to pass down the assembly line
        metadata = context.invocation_metadata()

        nparr = np.frombuffer(request.image_data, np.uint8)
        img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

        if img is None:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("Corrupt image bytes.")
            return photo_pb2.PhotoResponse()

        # --- True Black & White Transformation ---
        result = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
        
        _, buffer = cv2.imencode('.jpg', result)
        bw_bytes = buffer.tobytes()

        # 2. IMMEDIATE ASSEMBLY LINE HANDOFF: Push directly to Brighten Worker
        try:
            with grpc.insecure_channel(NEXT_STAGE_ADDR) as channel:
                stub = photo_pb2_grpc.PhotoProcessorStub(channel)
                # Pass the image data and forward the tracking metadata headers
                stub.Process(
                    photo_pb2.PhotoRequest(image_data=bw_bytes),
                    metadata=metadata,
                    timeout=5
                )
        except grpc.RpcError as e:
            print(f"Assembly line handoff failed from B&W to Brighten: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            return photo_pb2.PhotoResponse()

        # 3. Return immediate acknowledgment back to the dashboard gateway
        return photo_pb2.PhotoResponse(processed_data=b"ACK_BW")

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    photo_pb2_grpc.add_PhotoProcessorServicer_to_server(PhotoProcessor(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    print(f"B&W Worker (Assembly Stage 1) is running on Port 50051, forwarding to {NEXT_STAGE_ADDR}...")
    serve()
