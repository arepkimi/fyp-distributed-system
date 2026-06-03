import os
import cv2
import numpy as np
import grpc
from concurrent import futures
import photo_pb2
import photo_pb2_grpc

# Create a shared local directory inside the container to dump finished images
OUTPUT_DIR = "/app/shared_output"
os.makedirs(OUTPUT_DIR, exist_ok=True)


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

        # Save the final image directly to the shared output volume
        filepath = os.path.join(OUTPUT_DIR, filename)
        with open(filepath, 'wb') as f:
            f.write(image_bytes)

        print(f"Assembly Line Complete! Saved: {filename}")

        # Return a fast acknowledgment back to the previous worker
        return photo_pb2.PhotoResponse(processed_data=b"ACK_BLUR")


def serve():
    # Bumped max_workers pool to 20 threads to cleanly execute multiple parallel file writes simultaneously
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=20))
    photo_pb2_grpc.add_PhotoProcessorServicer_to_server(PhotoProcessor(), server)
    server.add_insecure_port('[::]:50053')
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    print("Blur Worker (Assembly Final Station) is running on Port 50053, receiving distributed load...")
    serve()
