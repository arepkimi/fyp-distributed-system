import os
import cv2
import numpy as np
import grpc
from concurrent import futures
import photo_pb2
import photo_pb2_grpc

# CRITICAL: Prepend 'dns:///' prefix so gRPC pulls individual pod IPs from K8s DNS
NEXT_STAGE_ADDR = os.getenv('BLUR_ADDR', 'dns:///blur-dns-service:50053')

# -----------------------------------------------------------------------
# GLOBAL ROUND-ROBIN CONNECTION POOLING FOR STAGE 3 DISTRIBUTED SCALE
# -----------------------------------------------------------------------
# This explicit config forces gRPC to rotate pods on EVERY single image payload sent to Stage 3
GRPC_ROUND_ROBIN_CONFIG = '{"loadBalancingConfig": [{"round_robin": {}}]}'

global_channel = grpc.insecure_channel(
    NEXT_STAGE_ADDR,
    options=[("grpc.service_config", GRPC_ROUND_ROBIN_CONFIG)]
)
global_stub = photo_pb2_grpc.PhotoProcessorStub(global_channel)


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

        # --- True Brightness Boost Transformation ---
        # Scale brightness by multiplying pixel values safely
        result = cv2.convertScaleAbs(img, alpha=1.2, beta=30)
        
        _, buffer = cv2.imencode('.jpg', result)
        bright_bytes = buffer.tobytes()

        # 2. FIXED HANDOFF: Uses persistent global round-robin stub to distribute work to Blur Worker
        try:
            # Pass the image data and forward the tracking metadata headers downstream
            global_stub.Process(
                photo_pb2.PhotoRequest(image_data=bright_bytes),
                metadata=metadata,
                timeout=10  # Relaxed slightly to account for high-concurrency pipeline queues safely
            )
        except grpc.RpcError as e:
            print(f"Assembly line load-split handoff failed from Brighten to Blur: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            return photo_pb2.PhotoResponse()

        # 3. Return immediate acknowledgment back to the B&W Worker upstream
        return photo_pb2.PhotoResponse(processed_data=b"ACK_BRIGHT")


def serve():
    # Bumped max_workers pool to 20 threads to handle concurrent round-robin incoming feeds
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=20))
    photo_pb2_grpc.add_PhotoProcessorServicer_to_server(PhotoProcessor(), server)
    server.add_insecure_port('[::]:50052')
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    print(f"Brightness Worker (Assembly Stage 2) is running on Port 50052, receiving distributed load...")
    serve()
