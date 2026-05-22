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

        # --- CORRECTED ACTION: Match unified Monolith (15, 15) Blur ---
        result = cv2.GaussianBlur(img, (15, 15), 0)
        
        _, buffer = cv2.imencode('.jpg', result)
        return photo_pb2.PhotoResponse(processed_data=buffer.tobytes())

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    photo_pb2_grpc.add_PhotoProcessorServicer_to_server(PhotoProcessor(), server)
    server.add_insecure_port('[::]:50053')
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    print("Blur Worker is running on Port 50053...")
    serve()