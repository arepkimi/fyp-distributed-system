import cv2, numpy as np, grpc
from concurrent import futures
import photo_pb2, photo_pb2_grpc

class PhotoProcessor(photo_pb2_grpc.PhotoProcessorServicer):
    def Process(self, request, context):
        nparr = np.frombuffer(request.image_data, np.uint8)
        img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

        if img is None:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            return photo_pb2.PhotoResponse()

        # --- CORRECTED ACTION: Brightness Boost Beta=50 ---
        result = cv2.convertScaleAbs(img, alpha=1.0, beta=50)
        
        _, buffer = cv2.imencode('.jpg', result)
        return photo_pb2.PhotoResponse(processed_data=buffer.tobytes())

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    photo_pb2_grpc.add_PhotoProcessorServicer_to_server(PhotoProcessor(), server)
    server.add_insecure_port('[::]:50052')
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    print("Brighten Worker is running on Port 50052...")
    serve()