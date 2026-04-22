from datetime import datetime

def generate_batch_id():
    return "batch_" + datetime.now().strftime("%Y%m%d_%H%M%S")