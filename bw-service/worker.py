import cv2, numpy as np, grpc
from concurrent import futures
import photo_pb2, photo_pb2_grpc

class PhotoProcessor(photo_pb2_grpc.PhotoProcessorServicer):
    def Process(self, request, context):
        nparr = np.frombuffer(request.image_data, np.uint8)
        img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

        if img is None:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("Corrupt image bytes.")
            return photo_pb2.PhotoResponse()

        # --- CORRECTED ACTION: True Black & White Transformation ---
        result = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
        
        _, buffer = cv2.imencode('.jpg', result)
        return photo_pb2.PhotoResponse(processed_data=buffer.tobytes())

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    photo_pb2_grpc.add_PhotoProcessorServicer_to_server(PhotoProcessor(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    print("B&W Worker is running on Port 50051...")
    serve()